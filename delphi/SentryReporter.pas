{
  SentryReporter — fachada de captura de erros com fila em disco.

  Regras de projeto:
    1. Capturar NUNCA bloqueia a thread que chamou. O evento é serializado
       e gravado em disco; quem envia é uma thread separada.
    2. Se o GlitchTip estiver fora do ar (ou a internet do cliente cair),
       os eventos ficam na fila em disco e sobem depois. Nada se perde.
    3. Um loop em erro não derruba nada: existe limite de eventos por
       minuto por tipo de erro.

  Uso mínimo:

    var LOpt: TSentryOptions;
    begin
      LOpt := TSentryOptions.Defaults;
      LOpt.Dsn            := 'https://CHAVE@erros.suaempresa.com.br/3';
      LOpt.Cnpj           := '12345678000199';
      LOpt.ClienteNome    := 'Fulano Comércio Ltda';
      LOpt.ReleaseVersion := 'meuservico@' + VERSAO_DO_BUILD;
      LOpt.Environment    := 'producao';
      TSentryReporter.Configure(LOpt);
    end;

    ...

    try
      ProcessarNFe(LDados);
    except
      on E: Exception do
      begin
        Sentry.CaptureException(E, 'POST /api/v1/nfe',
          procedure(const AEvent: TSentryEvent)
          begin
            AEvent.SetExtra('id_transacao', LDados.Id);
            AEvent.SetTag('modulo', 'nfe');
          end);
        raise;
      end;
    end;

  No encerramento do serviço:  TSentryReporter.Shutdown;
}
unit SentryReporter;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  SentryClient;

type
  TSentryInternalLog = reference to procedure(const AMessage: string);
  TSentryEventCustomizer = reference to procedure(const AEvent: TSentryEvent);

  TSentryOptions = record
    /// DSN do projeto no GlitchTip. Vazio = captura desligada.
    Dsn: string;
    /// CNPJ do cliente dono desta instalação. Vira a tag `cnpj`.
    Cnpj: string;
    /// Razão social / apelido do cliente. Vira a tag `cliente`.
    ClienteNome: string;
    /// 'producao' | 'homologacao' | 'desenvolvimento'
    Environment: string;
    /// Formato recomendado: 'meuservico@1.2.3'. Permite ver em que versão o bug apareceu.
    ReleaseVersion: string;
    /// Nome do servidor. Vazio = detecta automaticamente.
    ServerName: string;
    /// Pasta da fila. Vazio = <pasta do executável>/sentry-spool.
    SpoolDir: string;
    Enabled: Boolean;
    HttpTimeoutMs: Integer;
    /// Teto de eventos por minuto para o MESMO erro. Evita inundar o servidor.
    MaxEventsPerMinute: Integer;
    /// Teto de arquivos na fila. Acima disso os mais antigos são descartados.
    MaxSpoolFiles: Integer;
    /// Idade máxima de um evento na fila, em dias.
    MaxSpoolAgeDays: Integer;
    /// Intervalo de varredura da fila quando não há sinal, em ms.
    PollIntervalMs: Integer;

    // ---- aba Logs -------------------------------------------------------
    /// Envia também o log de rotina (aba Logs), além dos erros (aba Issues).
    LogsEnabled: Boolean;
    /// Piso de nível para o log de rotina. slTrace = manda tudo.
    LogMinLevel: TSentryLevel;
    /// Linhas por envelope. O protocolo não aceita mais que 100.
    LogMaxBatchSize: Integer;
    /// De quanto em quanto tempo o buffer em memória vira arquivo na fila.
    /// Este é o tamanho da janela de perda se o processo morrer de repente.
    LogFlushIntervalMs: Integer;
    /// Teto do buffer em memória. Estourando, as linhas mais antigas caem.
    LogMaxBufferItems: Integer;
    /// Prefixa o corpo de cada linha com [CNPJ].
    ///
    /// Existe por uma limitação da UI: a aba Logs do GlitchTip filtra por
    /// nível, service, environment e texto livre no corpo — NÃO por atributo
    /// arbitrário. O atributo `cnpj` continua sendo enviado (aparece ao abrir
    /// a linha), mas não é pesquisável. Com o prefixo, buscar o CNPJ no campo
    /// de texto livre passa a funcionar.
    LogPrefixCnpjInBody: Boolean;
    /// Sufixa o corpo de cada linha correlacionada com ` trace=<id>`.
    ///
    /// Mesma limitação da UI que motivou o LogPrefixCnpjInBody: a aba Logs só
    /// pesquisa texto livre no corpo, então o trace_id enviado no campo
    /// próprio não é pesquisável. Com o sufixo, o caminho "abro a issue, copio
    /// o trace da tag `trace`, colo na busca da aba Logs" funciona.
    ///
    /// Só entra nas linhas cujo trace veio da thread (correlação real). As
    /// que chegam com trace explícito — caso do provider do DataLogger — não
    /// ganham sufixo aqui; o provider decide por conta própria.
    LogTraceInBody: Boolean;
    /// Diagnóstico do próprio reporter (não é o log da aplicação).
    OnInternalLog: TSentryInternalLog;

    class function Defaults: TSentryOptions; static;
  end;

  TSentryReporter = class
  strict private
  type
    TRateBucket = record
      WindowStart: TDateTime;
      Count: Integer;
    end;

    TSpoolThread = class(TThread)
    strict private
      FOwner: TSentryReporter;
    protected
      procedure Execute; override;
    public
      constructor Create(const AOwner: TSentryReporter);
    end;

  private
    class var FInstance: TSentryReporter;
    class var FClassLock: TCriticalSection;
  strict private
    FStopping: Boolean;
    FOptions: TSentryOptions;
    FDsn: TSentryDsn;
    FTransport: TSentryTransport;
    FSpoolDir: string;
    FRejectDir: string;
    FServerName: string;
    FCS: TCriticalSection;
    FRates: TDictionary<string, TRateBucket>;
    FWake: TEvent;
    FWorker: TSpoolThread;
    FSequence: Integer;
    FDroppedByRate: Int64;

    FNextSendAt: TDateTime;
    FLastPrune: TDateTime;

    FLogBuffer: TList<TSentryLogItem>;
    FLastLogFlush: TDateTime;
    FDroppedLogs: Int64;

    procedure InternalLog(const AMessage: string);
    procedure EnsureDirs;
    function NextSpoolFile: string;
    function AllowByRate(const AKey: string): Boolean;
    procedure PruneSpool;
    procedure PruneSpoolIfDue;
    procedure EnqueueBytes(const ABytes: TBytes);
    function DefaultLogAttrs: TArray<TSentryAttr>;
    function ApplyCnpjPrefix(const ABody: string): string;
    procedure FlushLogBufferIfDue;
    /// Retorna a espera (ms) sugerida antes da próxima passada.
    function ProcessSpoolOnce: Integer;
    procedure ApplyDefaultsTo(const AEvent: TSentryEvent);
    procedure Enqueue(const AEvent: TSentryEvent);
  public
    constructor Create(const AOptions: TSentryOptions);
    destructor Destroy; override;
    /// Fecha a captura e encerra a thread de envio, sem liberar nada.
    procedure StopAccepting;

    class procedure Configure(const AOptions: TSentryOptions);
    class function Instance: TSentryReporter;
    class function IsConfigured: Boolean;
    /// Para a captura e drena a fila. Depois disso o reporter aceita chamadas
    /// mas as ignora — chamar Sentry.CaptureException no shutdown é seguro.
    class procedure Shutdown(const AFlushTimeoutMs: Integer = 10000);
    class procedure InitializeClass;
    class procedure FinalizeClass;

    function NewEvent(const ALevel: TSentryLevel = slError): TSentryEvent;

    /// Todas retornam o event_id (ou '' se o evento foi descartado).
    /// ATENÇÃO: Capture assume a posse do evento e SEMPRE o libera, inclusive
    /// quando o evento é descartado. Não chame Free depois.
    function Capture(const AEvent: TSentryEvent): string;
    function CaptureException(const E: Exception; const ATransaction: string = ''; const ACustomize: TSentryEventCustomizer = nil): string;
    function CaptureMessage(const AMessage: string; const ALevel: TSentryLevel = slError; const ATransaction: string = '';
      const ACustomize: TSentryEventCustomizer = nil): string;

    // ---- aba Logs -------------------------------------------------------
    /// Registra uma linha na aba Logs. Não agrupa, não alerta, não bloqueia:
    /// a linha entra num buffer em memória e vira arquivo na fila em lote.
    procedure Log(const ALevel: TSentryLevel; const ABody: string; const AAttrs: TArray<TSentryAttr> = nil; const ATraceId: string = '');
    procedure LogTrace(const ABody: string; const AAttrs: TArray<TSentryAttr> = nil);
    procedure LogDebug(const ABody: string; const AAttrs: TArray<TSentryAttr> = nil);
    procedure LogInfo(const ABody: string; const AAttrs: TArray<TSentryAttr> = nil);
    procedure LogWarn(const ABody: string; const AAttrs: TArray<TSentryAttr> = nil);
    procedure LogError(const ABody: string; const AAttrs: TArray<TSentryAttr> = nil);
    /// Empurra o buffer de log para a fila em disco agora.
    procedure FlushLogBuffer;
    function BufferedLogCount: Integer;

    /// Espera a fila esvaziar. Chame antes de encerrar o serviço.
    function Flush(const ATimeoutMs: Integer = 10000): Boolean;
    function PendingCount: Integer;

    property Options: TSentryOptions read FOptions;
    property DroppedByRate: Int64 read FDroppedByRate;
    property DroppedLogs: Int64 read FDroppedLogs;
  end;

/// Atalho. Levanta exceção se Configure não tiver sido chamado.
function Sentry: TSentryReporter;

implementation

uses
  System.IOUtils,
  System.DateUtils,
  System.Math;

function Sentry: TSentryReporter;
begin
  Result := TSentryReporter.Instance;
end;

{ TSentryOptions }

class function TSentryOptions.Defaults: TSentryOptions;
begin
  Result := Default (TSentryOptions);
  Result.Enabled := True;
  Result.Environment := 'producao';
  Result.HttpTimeoutMs := 15000;
  Result.MaxEventsPerMinute := 20;
  Result.MaxSpoolFiles := 5000;
  Result.MaxSpoolAgeDays := 7;
  Result.PollIntervalMs := 5000;

  Result.LogsEnabled := True;
  Result.LogMinLevel := slTrace;
  Result.LogMaxBatchSize := SENTRY_MAX_LOGS_PER_ENVELOPE;
  Result.LogFlushIntervalMs := 2000;
  Result.LogMaxBufferItems := 5000;
  Result.LogPrefixCnpjInBody := True;
  Result.LogTraceInBody := True;
end;

{ TSentryReporter.TSpoolThread }

constructor TSentryReporter.TSpoolThread.Create(const AOwner: TSentryReporter);
begin
  FOwner := AOwner;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TSentryReporter.TSpoolThread.Execute;
var
  LWaitMs: Integer;
begin
  NameThreadForDebugging('SentrySpool');
  LWaitMs := FOwner.FOptions.PollIntervalMs;

  while not Terminated do
  begin
    FOwner.FWake.WaitFor(LWaitMs);
    FOwner.FWake.ResetEvent;

    if Terminated then
      Break;

    try
      FOwner.FlushLogBufferIfDue;
      LWaitMs := FOwner.ProcessSpoolOnce;

      // Com logs ligados a thread precisa acordar pelo menos a cada janela
      // de flush, senão as linhas ficariam presas em memória até o próximo
      // erro ou o próximo poll.
      if FOwner.FOptions.LogsEnabled then
        LWaitMs := Min(LWaitMs, FOwner.FOptions.LogFlushIntervalMs);
    except
      on E: Exception do
      begin
        FOwner.InternalLog('falha na varredura da fila: ' + E.Message);
        LWaitMs := 30000;
      end;
    end;

    if LWaitMs <= 0 then
      LWaitMs := FOwner.FOptions.PollIntervalMs;
  end;
end;

{ TSentryReporter }

constructor TSentryReporter.Create(const AOptions: TSentryOptions);
begin
  inherited Create;

  FOptions := AOptions;

  if FOptions.PollIntervalMs <= 0 then
    FOptions.PollIntervalMs := 5000;
  if FOptions.HttpTimeoutMs <= 0 then
    FOptions.HttpTimeoutMs := 15000;
  if FOptions.MaxSpoolFiles <= 0 then
    FOptions.MaxSpoolFiles := 5000;

  if FOptions.LogMaxBatchSize <= 0 then
    FOptions.LogMaxBatchSize := SENTRY_MAX_LOGS_PER_ENVELOPE;
  FOptions.LogMaxBatchSize := Min(FOptions.LogMaxBatchSize, SENTRY_MAX_LOGS_PER_ENVELOPE);

  if FOptions.LogFlushIntervalMs <= 0 then
    FOptions.LogFlushIntervalMs := 2000;
  if FOptions.LogMaxBufferItems <= 0 then
    FOptions.LogMaxBufferItems := 5000;

  FCS := TCriticalSection.Create;
  FRates := TDictionary<string, TRateBucket>.Create;
  FLogBuffer := TList<TSentryLogItem>.Create;
  FLastLogFlush := Now;
  FWake := TEvent.Create(nil, True, False, '');

  FServerName := FOptions.ServerName;
  if FServerName.Trim.IsEmpty then
    FServerName := SentryHostName;

  FSpoolDir := FOptions.SpoolDir;
  if FSpoolDir.Trim.IsEmpty then
    FSpoolDir := TPath.Combine(TPath.GetDirectoryName(ParamStr(0)), 'sentry-spool');
  FRejectDir := TPath.Combine(FSpoolDir, 'rejeitados');

  FDsn := Default (TSentryDsn);
  if FOptions.Enabled and not FOptions.Dsn.Trim.IsEmpty then
  begin
    try
      FDsn := TSentryDsn.Parse(FOptions.Dsn);
    except
      on E: Exception do
      begin
        InternalLog('DSN inválido, captura desligada: ' + E.Message);
        FOptions.Enabled := False;
      end;
    end;
  end
  else
    FOptions.Enabled := False;

  if not FOptions.Enabled then
    Exit;

  EnsureDirs;
  FTransport := TSentryTransport.Create(FDsn, FOptions.HttpTimeoutMs);
  FWorker := TSpoolThread.Create(Self);

  InternalLog(Format('ativo — destino %s, fila em %s', [FDsn.EnvelopeUrl, FSpoolDir]));
end;

procedure TSentryReporter.StopAccepting;
begin
  if FStopping then
    Exit;

  // Última chance de transformar o que está em memória em arquivo na fila.
  try
    FlushLogBuffer;
  except
    // encerramento não pode falhar por causa de log
  end;

  // A partir daqui Capture vira no-op e a thread de envio encerra. Nada é
  // liberado ainda — quem libera é o destrutor, na finalização da unit.
  FStopping := True;

  if Assigned(FWorker) then
  begin
    FWorker.Terminate;
    FWake.SetEvent;
    FWorker.WaitFor;
    FreeAndNil(FWorker);
  end;
end;

destructor TSentryReporter.Destroy;
begin
  StopAccepting;

  FTransport.Free;
  FWake.Free;
  FLogBuffer.Free;
  FRates.Free;
  FCS.Free;

  inherited;
end;

class procedure TSentryReporter.Configure(const AOptions: TSentryOptions);
begin
  FClassLock.Enter;
  try
    FreeAndNil(FInstance);
    FInstance := TSentryReporter.Create(AOptions);
  finally
    FClassLock.Leave;
  end;
end;

class function TSentryReporter.Instance: TSentryReporter;
begin
  if not Assigned(FInstance) then
    raise ESentryError.Create('TSentryReporter.Configure não foi chamado.');
  Result := FInstance;
end;

class function TSentryReporter.IsConfigured: Boolean;
begin
  Result := Assigned(FInstance);
end;

class procedure TSentryReporter.InitializeClass;
begin
  // Criado na inicialização da unit, não sob demanda: duas threads chamando
  // Configure ao mesmo tempo criariam dois locks e nenhum protegeria nada.
  FClassLock := TCriticalSection.Create;
end;

class procedure TSentryReporter.Shutdown(const AFlushTimeoutMs: Integer);
begin
  FClassLock.Enter;
  try
    if not Assigned(FInstance) then
      Exit;

    // Drena o que dá, depois fecha a porta. A instância continua viva:
    // qualquer thread que ainda chame Sentry.CaptureException durante o
    // encerramento cai no teste de FStopping em vez de num ponteiro morto.
    FInstance.Flush(AFlushTimeoutMs);
    FInstance.StopAccepting;
  finally
    FClassLock.Leave;
  end;
end;

class procedure TSentryReporter.FinalizeClass;
begin
  // Sem espera aqui: o processo já está morrendo e o que sobrou na fila
  // sobe na próxima execução do serviço.
  Shutdown(0);

  FClassLock.Enter;
  try
    FreeAndNil(FInstance);
  finally
    FClassLock.Leave;
  end;

  FreeAndNil(FClassLock);
end;

procedure TSentryReporter.InternalLog(const AMessage: string);
begin
  if Assigned(FOptions.OnInternalLog) then
    try
      FOptions.OnInternalLog('[sentry] ' + AMessage);
    except
      // diagnóstico nunca pode derrubar o serviço
    end;
end;

procedure TSentryReporter.EnsureDirs;
begin
  if not TDirectory.Exists(FSpoolDir) then
    TDirectory.CreateDirectory(FSpoolDir);
  if not TDirectory.Exists(FRejectDir) then
    TDirectory.CreateDirectory(FRejectDir);
end;

function TSentryReporter.NextSpoolFile: string;
begin
  Result := TPath.Combine(FSpoolDir, Format('%s-%.6d', [FormatDateTime('yyyymmddhhnnsszzz', Now), TInterlocked.Increment(FSequence) and $FFFFF]));
end;

function TSentryReporter.AllowByRate(const AKey: string): Boolean;
var
  LBucket: TRateBucket;
  LNow: TDateTime;
begin
  if FOptions.MaxEventsPerMinute <= 0 then
    Exit(True);

  LNow := Now;

  FCS.Enter;
  try
    if not FRates.TryGetValue(AKey, LBucket) or (SecondsBetween(LNow, LBucket.WindowStart) >= 60) then
    begin
      LBucket.WindowStart := LNow;
      LBucket.Count := 0;
    end;

    Inc(LBucket.Count);
    FRates.AddOrSetValue(AKey, LBucket);

    Result := LBucket.Count <= FOptions.MaxEventsPerMinute;

    if not Result then
      Inc(FDroppedByRate);

    // higiene: o dicionário não pode crescer para sempre
    if FRates.Count > 500 then
      FRates.Clear;
  finally
    FCS.Leave;
  end;
end;

procedure TSentryReporter.ApplyDefaultsTo(const AEvent: TSentryEvent);
begin
  AEvent.ServerName := FServerName;
  AEvent.Environment := FOptions.Environment;
  AEvent.ReleaseVersion := FOptions.ReleaseVersion;

  if not FOptions.Cnpj.Trim.IsEmpty then
    AEvent.SetTag('cnpj', FOptions.Cnpj.Trim);
  if not FOptions.ClienteNome.Trim.IsEmpty then
    AEvent.SetTag('cliente', FOptions.ClienteNome.Trim);

{$IF DEFINED(MSWINDOWS)}
  AEvent.SetTag('so', 'windows');
{$ELSE}
  AEvent.SetTag('so', 'linux');
{$ENDIF}
end;

function TSentryReporter.NewEvent(const ALevel: TSentryLevel): TSentryEvent;
begin
  Result := TSentryEvent.Create;
  Result.Level := ALevel;
  ApplyDefaultsTo(Result);
end;

procedure TSentryReporter.Enqueue(const AEvent: TSentryEvent);
begin
  EnqueueBytes(FTransport.BuildEnvelope(AEvent));
end;

procedure TSentryReporter.EnqueueBytes(const ABytes: TBytes);
var
  LBytes: TBytes;
  LBase, LTmp, LFinal: string;
begin
  LBytes := ABytes;
  if Length(LBytes) = 0 then
    Exit;

  PruneSpoolIfDue;

  LBase := NextSpoolFile;
  LTmp := LBase + '.tmp';
  LFinal := LBase + '.envelope';

  // grava em .tmp e só então renomeia, para a thread de envio nunca
  // encontrar um arquivo pela metade
  TFile.WriteAllBytes(LTmp, LBytes);
  TFile.Move(LTmp, LFinal);

  FWake.SetEvent;
end;

function TSentryReporter.Capture(const AEvent: TSentryEvent): string;
begin
  Result := '';

  if not Assigned(AEvent) then
    Exit;

  try
    try
      if not FOptions.Enabled or FStopping then
        Exit;

      if not AllowByRate(AEvent.DedupeKey) then
      begin
        InternalLog('evento descartado por limite de taxa: ' + AEvent.DedupeKey);
        Exit;
      end;

      Enqueue(AEvent);
      Result := AEvent.EventId;
    except
      on E: Exception do
        // Falhar ao registrar um erro não pode virar um segundo erro.
        InternalLog('falha ao enfileirar evento: ' + E.Message);
    end;
  finally
    AEvent.Free;
  end;
end;

function TSentryReporter.CaptureException(const E: Exception; const ATransaction: string; const ACustomize: TSentryEventCustomizer): string;
var
  LEvent: TSentryEvent;
begin
  if not FOptions.Enabled or FStopping then
    Exit('');

  LEvent := NewEvent(slError);
  LEvent.Transaction := ATransaction;
  LEvent.FromException(E);

  if Assigned(ACustomize) then
    try
      ACustomize(LEvent);
    except
      on EX: Exception do
        InternalLog('customizador do evento falhou: ' + EX.Message);
    end;

  Result := Capture(LEvent);
end;

function TSentryReporter.CaptureMessage(const AMessage: string; const ALevel: TSentryLevel; const ATransaction: string;
  const ACustomize: TSentryEventCustomizer): string;
var
  LEvent: TSentryEvent;
begin
  if not FOptions.Enabled or FStopping then
    Exit('');

  LEvent := NewEvent(ALevel);
  LEvent.Transaction := ATransaction;
  LEvent.MessageText := AMessage;

  if Assigned(ACustomize) then
    try
      ACustomize(LEvent);
    except
      on EX: Exception do
        InternalLog('customizador do evento falhou: ' + EX.Message);
    end;

  Result := Capture(LEvent);
end;

/// PruneSpool varre o diretório inteiro; rodar isso a cada captura violaria a
/// promessa de que capturar não bloqueia. Uma vez por minuto basta.
function TSentryReporter.DefaultLogAttrs: TArray<TSentryAttr>;
begin
  Result := [];

  if not FOptions.Cnpj.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('cnpj', FOptions.Cnpj.Trim)];
  if not FOptions.ClienteNome.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('cliente', FOptions.ClienteNome.Trim)];
  if not FOptions.Environment.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('sentry.environment', FOptions.Environment.Trim)];
  if not FOptions.ReleaseVersion.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('sentry.release', FOptions.ReleaseVersion.Trim)];

  Result := Result + [TSentryAttr.Str('server_name', FServerName)];
{$IF DEFINED(MSWINDOWS)}
  Result := Result + [TSentryAttr.Str('so', 'windows')];
{$ELSE}
  Result := Result + [TSentryAttr.Str('so', 'linux')];
{$ENDIF}
end;

function TSentryReporter.ApplyCnpjPrefix(const ABody: string): string;
var
  LPrefix: string;
begin
  Result := ABody;

  if not FOptions.LogPrefixCnpjInBody or FOptions.Cnpj.Trim.IsEmpty then
    Exit;

  LPrefix := '[' + FOptions.Cnpj.Trim + ']';

  // Não prefixa duas vezes: o provider do DataLogger pode reencaminhar uma
  // mensagem que já passou por aqui.
  if Result.StartsWith(LPrefix) then
    Exit;

  Result := LPrefix + ' ' + Result;
end;

procedure TSentryReporter.Log(const ALevel: TSentryLevel; const ABody: string; const AAttrs: TArray<TSentryAttr>; const ATraceId: string);
var
  LItem: TSentryLogItem;
  LAttr: TSentryAttr;
  LShouldFlush: Boolean;
  LUtc: TDateTime;
begin
  if not FOptions.Enabled or not FOptions.LogsEnabled or FStopping then
    Exit;

  if ALevel < FOptions.LogMinLevel then
    Exit;

  LUtc := TTimeZone.Local.ToUniversalTime(Now);

  LItem := Default (TSentryLogItem);
  // Conversão direta para epoch com fração. Não use DateTimeToUnix + fração:
  // ele ARREDONDA para o segundo mais próximo, então tudo com ms >= 500
  // ganharia um segundo a mais e a ordenação na aba Logs sairia trocada.
  LItem.TimestampUnix := (LUtc - UnixDateDelta) * SecsPerDay;
  LItem.Level := ALevel;
  LItem.Body := ApplyCnpjPrefix(ABody);

  if ATraceId.Trim.IsEmpty then
  begin
    LItem.TraceId := TSentryTrace.Current;
    // A busca da aba Logs só vê o corpo; o sufixo torna o trace pesquisável.
    if FOptions.LogTraceInBody then
      LItem.Body := LItem.Body + ' trace=' + LItem.TraceId;
  end
  else
    LItem.TraceId := ATraceId;

  // Atributos do chamador primeiro; os padrões só entram se a chave ainda
  // não existir. Chave repetida dentro de `attributes` gera JSON inválido.
  for LAttr in AAttrs do
    LItem.AddAttr(LAttr);

  for LAttr in DefaultLogAttrs do
    if not LItem.HasAttr(LAttr.Key) then
      LItem.AddAttr(LAttr);

  FCS.Enter;
  try
    // Buffer cheio: descarta o mais antigo. Perder linha velha é melhor do
    // que estourar a memória do serviço do cliente.
    while FLogBuffer.Count >= FOptions.LogMaxBufferItems do
    begin
      FLogBuffer.Delete(0);
      Inc(FDroppedLogs);
    end;

    FLogBuffer.Add(LItem);
    LShouldFlush := FLogBuffer.Count >= FOptions.LogMaxBatchSize;
  finally
    FCS.Leave;
  end;

  // Lote cheio: acorda a thread de envio em vez de escrever em disco aqui.
  // Quem chama Log está no meio de atender uma requisição.
  if LShouldFlush then
    FWake.SetEvent;
end;

procedure TSentryReporter.LogTrace(const ABody: string; const AAttrs: TArray<TSentryAttr>);
begin
  Log(slTrace, ABody, AAttrs);
end;

procedure TSentryReporter.LogDebug(const ABody: string; const AAttrs: TArray<TSentryAttr>);
begin
  Log(slDebug, ABody, AAttrs);
end;

procedure TSentryReporter.LogInfo(const ABody: string; const AAttrs: TArray<TSentryAttr>);
begin
  Log(slInfo, ABody, AAttrs);
end;

procedure TSentryReporter.LogWarn(const ABody: string; const AAttrs: TArray<TSentryAttr>);
begin
  Log(slWarning, ABody, AAttrs);
end;

procedure TSentryReporter.LogError(const ABody: string; const AAttrs: TArray<TSentryAttr>);
begin
  Log(slError, ABody, AAttrs);
end;

function TSentryReporter.BufferedLogCount: Integer;
begin
  if not Assigned(FLogBuffer) then
    Exit(0);

  FCS.Enter;
  try
    Result := FLogBuffer.Count;
  finally
    FCS.Leave;
  end;
end;

procedure TSentryReporter.FlushLogBuffer;
var
  LBatch: TArray<TSentryLogItem>;
  LTake, I: Integer;
begin
  if not FOptions.Enabled or not Assigned(FLogBuffer) then
    Exit;

  // Um envelope não pode passar de LogMaxBatchSize itens, então esvazia em
  // rodadas em vez de tentar mandar tudo de uma vez.
  repeat
    FCS.Enter;
    try
      LTake := Min(FLogBuffer.Count, FOptions.LogMaxBatchSize);
      if LTake = 0 then
        Break;

      SetLength(LBatch, LTake);
      for I := 0 to LTake - 1 do
        LBatch[I] := FLogBuffer[I];
      FLogBuffer.DeleteRange(0, LTake);
      FLastLogFlush := Now;
    finally
      FCS.Leave;
    end;

    try
      EnqueueBytes(FTransport.BuildLogEnvelope(LBatch));
    except
      on E: Exception do
      begin
        InternalLog('falha ao enfileirar lote de log: ' + E.Message);

        // O lote já saiu do buffer mas não chegou ao disco. Devolve para o
        // início da fila — do contrário disco cheio viraria log perdido,
        // que é exatamente o que a fila existe para evitar.
        FCS.Enter;
        try
          FLogBuffer.InsertRange(0, LBatch);
        finally
          FCS.Leave;
        end;

        Break;
      end;
    end;
  until False;

  FWake.SetEvent;
end;

procedure TSentryReporter.FlushLogBufferIfDue;
var
  LDue: Boolean;
begin
  if not FOptions.LogsEnabled or not Assigned(FLogBuffer) then
    Exit;

  FCS.Enter;
  try
    LDue := (FLogBuffer.Count >= FOptions.LogMaxBatchSize) or
      ((FLogBuffer.Count > 0) and (MilliSecondsBetween(Now, FLastLogFlush) >= FOptions.LogFlushIntervalMs));
  finally
    FCS.Leave;
  end;

  if LDue then
    FlushLogBuffer;
end;

procedure TSentryReporter.PruneSpoolIfDue;
var
  LDue: Boolean;
begin
  FCS.Enter;
  try
    LDue := (FLastPrune = 0) or (SecondsBetween(Now, FLastPrune) >= 60);
    if LDue then
      FLastPrune := Now;
  finally
    FCS.Leave;
  end;

  if LDue then
    PruneSpool;
end;

procedure TSentryReporter.PruneSpool;
var
  LFiles: TArray<string>;
  I, LExcess: Integer;
  LLimit, LTmpLimit: TDateTime;
begin
  LLimit := IncDay(Now, -Max(1, FOptions.MaxSpoolAgeDays));
  LTmpLimit := IncHour(Now, -1);

  // 1) .tmp órfãos — sobra de um processo morto entre o WriteAllBytes e o
  //    Move. Ninguém mais vai reclamá-los, então eles ficariam para sempre.
  try
    LFiles := TDirectory.GetFiles(FSpoolDir, '*.tmp');
    for I := Low(LFiles) to High(LFiles) do
      try
        if TFile.GetLastWriteTime(LFiles[I]) < LTmpLimit then
          TFile.Delete(LFiles[I]);
      except
        // pode estar sendo escrito agora
      end;
  except
    // diretório indisponível: tenta na próxima
  end;

  // 2) eventos vencidos
  try
    LFiles := TDirectory.GetFiles(FSpoolDir, '*.envelope');
  except
    Exit;
  end;

  for I := Low(LFiles) to High(LFiles) do
    try
      if TFile.GetLastWriteTime(LFiles[I]) < LLimit then
        TFile.Delete(LFiles[I]);
    except
      // arquivo pode ter sido consumido pela thread de envio
    end;

  // 3) excesso — relista depois da poda acima, senão a conta usa arquivos
  //    que já não existem e a fila continua acima do teto.
  try
    LFiles := TDirectory.GetFiles(FSpoolDir, '*.envelope');
  except
    Exit;
  end;

  LExcess := Length(LFiles) - FOptions.MaxSpoolFiles;
  if LExcess <= 0 then
    Exit;

  TArray.Sort<string>(LFiles);
  InternalLog(Format('fila cheia (%d), descartando %d eventos mais antigos', [Length(LFiles), LExcess]));

  for I := 0 to LExcess - 1 do
    try
      TFile.Delete(LFiles[I]);
    except
      // idem
    end;
end;

function TSentryReporter.ProcessSpoolOnce: Integer;
var
  LFiles: TArray<string>;
  LFile: string;
  LBytes: TBytes;
  LResult: TSentrySendResult;
  LSent: Integer;
begin
  Result := FOptions.PollIntervalMs;

  // Respeita o recuo pedido pelo servidor mesmo que alguém acorde a thread
  // no meio do caminho (Flush, por exemplo). Sem isto, um 429 viraria
  // marretada de 100 em 100 ms.
  if (FNextSendAt > 0) and (Now < FNextSendAt) then
    Exit(Max(500, MilliSecondsBetween(FNextSendAt, Now)));

  PruneSpoolIfDue;

  try
    LFiles := TDirectory.GetFiles(FSpoolDir, '*.envelope');
  except
    Exit(30000);
  end;

  if Length(LFiles) = 0 then
    Exit;

  TArray.Sort<string>(LFiles);
  LSent := 0;

  for LFile in LFiles do
  begin
    if FStopping then
      Break;

    try
      LBytes := TFile.ReadAllBytes(LFile);
    except
      Continue;
    end;

    LResult := FTransport.Send(LBytes);

    if LResult.Success then
    begin
      Inc(LSent);
      FNextSendAt := 0;
      try
        TFile.Delete(LFile);
      except
        // se não deu para apagar, o evento pode duplicar — o servidor
        // deduplica pelo event_id, então tudo bem
      end;
      Continue;
    end;

    if LResult.Retryable then
    begin
      InternalLog(Format('envio adiado (%s). Restam %d na fila.', [LResult.ErrorText, Length(LFiles) - LSent]));
      // Respeita o Retry-After do servidor; senão, recua 60s.
      Result := Max(FOptions.PollIntervalMs, IfThen(LResult.RetryAfterSec > 0, LResult.RetryAfterSec * 1000, 60000));
      FNextSendAt := IncMilliSecond(Now, Result);
      Exit;
    end;

    // Erro permanente (400/401/413...): insistir só entope a fila.
    InternalLog('evento rejeitado pelo servidor, movido para /rejeitados: ' + LResult.ErrorText);
    try
      TFile.Move(LFile, TPath.Combine(FRejectDir, TPath.GetFileName(LFile)));
    except
      try
        TFile.Delete(LFile);
      except
      end;
    end;
  end;

  // Ainda tem coisa? volta logo.
  if LSent > 0 then
    Result := 500;
end;

function TSentryReporter.PendingCount: Integer;
begin
  Result := 0;
  if not FOptions.Enabled then
    Exit;
  try
    Result := Length(TDirectory.GetFiles(FSpoolDir, '*.envelope'));
  except
    Result := 0;
  end;
end;

function TSentryReporter.Flush(const ATimeoutMs: Integer): Boolean;
var
  LDeadline: TDateTime;
begin
  if not FOptions.Enabled then
    Exit(True);

  // O que está em memória ainda não é "pendente" para o PendingCount, então
  // vira arquivo antes de a contagem começar a valer.
  FlushLogBuffer;

  if PendingCount = 0 then
    Exit(True);

  LDeadline := IncMilliSecond(Now, Max(0, ATimeoutMs));

  // Um único empurrão. Depois é só espera: acordar a thread em looping
  // atropelaria o recuo negociado com o servidor (FNextSendAt).
  FWake.SetEvent;

  while Now < LDeadline do
  begin
    if PendingCount = 0 then
      Exit(True);
    Sleep(100);
  end;

  Result := PendingCount = 0;
  if not Result then
    InternalLog(Format('Flush expirou com %d eventos na fila (serão enviados na próxima execução).', [PendingCount]));
end;

initialization

TSentryReporter.InitializeClass;

finalization

TSentryReporter.FinalizeClass;

end.
