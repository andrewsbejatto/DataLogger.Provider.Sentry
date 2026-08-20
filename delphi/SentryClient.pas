{
  SentryClient — protocolo de ingestão Sentry/GlitchTip para Delphi.

  Camada "burra": monta o evento, monta o envelope e faz o POST.
  Não tem fila, não tem thread, não tem retry — isso é o SentryReporter.

  Compatível com Windows e Linux (usa System.Net.HttpClient).

  Cobre os dois tipos de item que o GlitchTip aceita no mesmo endpoint:
    - `event`, que vira issue na aba Issues
    - `log`,   que vira linha na aba Logs

  Referência: https://develop.sentry.dev/sdk/data-model/envelopes/
              https://develop.sentry.dev/sdk/telemetry/logs/
}
unit SentryClient;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.DateUtils,
  System.Generics.Collections,
  System.Net.HttpClient,
  System.Net.URLClient;

const
  SENTRY_CLIENT_NAME = 'delphi-glitchtip/1.0';
  /// Teto do protocolo: um envelope de log não pode passar de 100 itens.
  SENTRY_MAX_LOGS_PER_ENVELOPE = 100;

type
  TSentryLevel = (slTrace, slDebug, slInfo, slWarning, slError, slFatal);

  ESentryError = class(Exception);

  /// Atributo de uma linha de log. O Sentry exige o tipo declarado junto do valor.
  TSentryAttrKind = (akString, akInteger, akDouble, akBoolean);

  TSentryAttr = record
    Key: string;
    Value: string;
    Kind: TSentryAttrKind;
    class function Str(const AKey, AValue: string): TSentryAttr; static;
    class function Int(const AKey: string; const AValue: Int64): TSentryAttr; static;
    class function Dbl(const AKey: string; const AValue: Double): TSentryAttr; static;
    class function Bool(const AKey: string; const AValue: Boolean): TSentryAttr; static;
  end;

  /// Uma linha de log. Diferente do evento, não agrupa e não alerta —
  /// é o rastro cru que você lê depois para entender o que aconteceu.
  TSentryLogItem = record
    /// Epoch em segundos, com fração.
    TimestampUnix: Double;
    /// Obrigatório, 32 hex. Use TSentryTrace.Current.
    TraceId: string;
    Level: TSentryLevel;
    Body: string;
    Attributes: TArray<TSentryAttr>;
    procedure AddAttr(const AAttr: TSentryAttr);
    function HasAttr(const AKey: string): Boolean;
  end;

  /// Correlação por requisição. Chame BeginRequest no início de cada chamada
  /// da API e todas as linhas daquela requisição ficam ligadas entre si na
  /// aba Logs — inclusive as de outras threads que herdarem o id.
  TSentryTrace = class
  public
    /// Devolve o trace da thread atual, criando um se ainda não existir.
    class function Current: string;
    /// Inicia (ou adota) um trace nesta thread. Devolve o id em uso.
    class function BeginRequest(const ATraceId: string = ''): string;
    class procedure EndRequest;
  end;

  /// <summary>
  ///   DSN no formato  {scheme}://{public_key}@{host}[:{porta}][/{prefixo}]/{project_id}
  ///   Ex.: https://a1b2c3d4@erros.suaempresa.com.br/3
  /// </summary>
  TSentryDsn = record
  strict private
    FRaw: string;
    FScheme: string;
    FPublicKey: string;
    FHost: string;
    FPort: Integer;
    FPathPrefix: string;
    FProjectId: string;
  public
    class function Parse(const ADsn: string): TSentryDsn; static;
    function IsValid: Boolean;
    function EnvelopeUrl: string;
    function AuthHeaderValue(const AClientName: string = SENTRY_CLIENT_NAME): string;

    property Raw: string read FRaw;
    property Scheme: string read FScheme;
    property PublicKey: string read FPublicKey;
    property Host: string read FHost;
    property Port: Integer read FPort;
    property ProjectId: string read FProjectId;
  end;

  /// <summary>
  ///   Um evento de erro. É o payload JSON que vira uma "issue" no GlitchTip.
  /// </summary>
  TSentryEvent = class
  strict private
    FEventId: string;
    FTimestampUtc: TDateTime;
    FLevel: TSentryLevel;
    FLogger: string;
    FTransaction: string;
    FServerName: string;
    FReleaseVersion: string;
    FEnvironment: string;
    FMessageText: string;
    FExceptionType: string;
    FExceptionValue: string;
    FStackTraceText: string;
    FTraceId: string;
    FFingerprint: TArray<string>;
    FTags: TDictionary<string, string>;
    FExtra: TDictionary<string, string>;
    function BuildStacktraceJSON: TJSONObject;
    function BuildExceptionJSON: TJSONObject;
    class function LevelToStr(const ALevel: TSentryLevel): string; static;
  public
    constructor Create;
    destructor Destroy; override;

    procedure FromException(const E: Exception);
    procedure SetTag(const AKey, AValue: string);
    procedure SetExtra(const AKey, AValue: string);

    function ToJSONString: string;

    /// Chave usada para limitar taxa/deduplicar do lado do cliente.
    /// Números são removidos para que "timeout após 1234ms" e
    /// "timeout após 5678ms" contem como o mesmo erro.
    function DedupeKey: string;

    property EventId: string read FEventId;
    property TimestampUtc: TDateTime read FTimestampUtc write FTimestampUtc;
    property Level: TSentryLevel read FLevel write FLevel;
    property Logger: string read FLogger write FLogger;
    /// Vira o "culprit" da issue — use o nome do endpoint/rotina.
    property Transaction: string read FTransaction write FTransaction;
    property ServerName: string read FServerName write FServerName;
    property ReleaseVersion: string read FReleaseVersion write FReleaseVersion;
    property Environment: string read FEnvironment write FEnvironment;
    property MessageText: string read FMessageText write FMessageText;
    property ExceptionType: string read FExceptionType write FExceptionType;
    property ExceptionValue: string read FExceptionValue write FExceptionValue;
    property StackTraceText: string read FStackTraceText write FStackTraceText;
    /// Trace da requisição (32 hex). Preenchido na criação com o trace da
    /// thread atual (TSentryTrace.Current). Vira `contexts.trace` e a tag
    /// `trace` no evento — é o elo entre a issue e as linhas da aba Logs
    /// daquela mesma requisição. Vazio = evento sem trace.
    property TraceId: string read FTraceId write FTraceId;
    /// Deixe vazio para o servidor agrupar sozinho. NÃO coloque o CNPJ aqui,
    /// senão o mesmo bug vira 50 issues diferentes.
    property Fingerprint: TArray<string> read FFingerprint write FFingerprint;
  end;

  TSentrySendResult = record
    Success: Boolean;
    StatusCode: Integer;
    Retryable: Boolean;
    RetryAfterSec: Integer;
    ErrorText: string;
  end;

  TSentryTransport = class
  strict private
    FDsn: TSentryDsn;
    FTimeoutMs: Integer;
    FUrl: string;
    FAuthHeader: string;
    /// Monta header do envelope + header do item + payload. Assume a posse
    /// de AItemHeader.
    function Assemble(const AItemHeader: TJSONObject; const APayload: TBytes; const AEventId: string): TBytes;
  public
    constructor Create(const ADsn: TSentryDsn; const ATimeoutMs: Integer = 15000);

    /// Serializa o evento no formato envelope (pronto para gravar em disco).
    function BuildEnvelope(const AEvent: TSentryEvent): TBytes;
    /// Idem, para um lote de linhas de log. Máximo SENTRY_MAX_LOGS_PER_ENVELOPE.
    function BuildLogEnvelope(const AItems: TArray<TSentryLogItem>): TBytes;
    function Send(const AEnvelope: TBytes): TSentrySendResult;

    property Dsn: TSentryDsn read FDsn;
  end;

function SentryNewEventId: string;
function SentryNewTraceId: string;
function SentryHostName: string;
/// Nível como o endpoint de eventos espera ('warning', sem 'trace').
function SentryEventLevelStr(const ALevel: TSentryLevel): string;
/// Nível como o endpoint de logs espera ('warn', com 'trace').
function SentryLogLevelStr(const ALevel: TSentryLevel): string;
/// severity_number do OpenTelemetry.
function SentrySeverityNumber(const ALevel: TSentryLevel): Integer;

implementation

uses
{$IF DEFINED(MSWINDOWS)}
  Winapi.Windows,
{$ENDIF}
  System.IOUtils,
  System.StrUtils,
  System.Character;

function SentryNewEventId: string;
var
  LGuid: TGUID;
begin
  LGuid := TGUID.NewGuid;
  Result := LGuid.ToString;
  Result := Result.Replace('{', '').Replace('}', '').Replace('-', '').ToLower;
end;

function SentryHostName: string;
{$IF DEFINED(MSWINDOWS)}
var
  LBuffer: array [0 .. MAX_COMPUTERNAME_LENGTH] of Char;
  LSize: DWORD;
{$ENDIF}
begin
  Result := '';
{$IF DEFINED(MSWINDOWS)}
  LSize := Length(LBuffer);
  if GetComputerName(LBuffer, LSize) then
    Result := string(LBuffer);
{$ELSE}
  Result := GetEnvironmentVariable('HOSTNAME');
  if Result.Trim.IsEmpty then
    try
      if TFile.Exists('/etc/hostname') then
        Result := TFile.ReadAllText('/etc/hostname').Trim;
    except
      Result := '';
    end;
{$ENDIF}
  if Result.Trim.IsEmpty then
    Result := 'desconhecido';
end;

function SentryNewTraceId: string;
begin
  // trace_id tem o mesmo formato do event_id: 32 hex, sem hífens.
  Result := SentryNewEventId;
end;

function SentryEventLevelStr(const ALevel: TSentryLevel): string;
begin
  case ALevel of
    // O endpoint de eventos não conhece 'trace'.
    slTrace, slDebug:
      Result := 'debug';
    slInfo:
      Result := 'info';
    slWarning:
      Result := 'warning';
    slFatal:
      Result := 'fatal';
  else
    Result := 'error';
  end;
end;

function SentryLogLevelStr(const ALevel: TSentryLevel): string;
begin
  case ALevel of
    slTrace:
      Result := 'trace';
    slDebug:
      Result := 'debug';
    slInfo:
      Result := 'info';
    // Atenção: aqui é 'warn', não 'warning' como no evento.
    slWarning:
      Result := 'warn';
    slFatal:
      Result := 'fatal';
  else
    Result := 'error';
  end;
end;

function SentrySeverityNumber(const ALevel: TSentryLevel): Integer;
begin
  case ALevel of
    slTrace:
      Result := 1;
    slDebug:
      Result := 5;
    slInfo:
      Result := 9;
    slWarning:
      Result := 13;
    slFatal:
      Result := 21;
  else
    Result := 17;
  end;
end;

{ TSentryAttr }

class function TSentryAttr.Str(const AKey, AValue: string): TSentryAttr;
begin
  Result.Key := AKey;
  Result.Value := AValue;
  Result.Kind := akString;
end;

class function TSentryAttr.Int(const AKey: string; const AValue: Int64): TSentryAttr;
begin
  Result.Key := AKey;
  Result.Value := AValue.ToString;
  Result.Kind := akInteger;
end;

class function TSentryAttr.Dbl(const AKey: string; const AValue: Double): TSentryAttr;
begin
  Result.Key := AKey;
  // Invariant de propósito: numa máquina pt-BR o FloatToStr local produziria
  // "1,5", que o parse de volta leria como zero.
  Result.Value := FloatToStr(AValue, TFormatSettings.Invariant);
  Result.Kind := akDouble;
end;

class function TSentryAttr.Bool(const AKey: string; const AValue: Boolean): TSentryAttr;
begin
  Result.Key := AKey;
  Result.Value := IfThen(AValue, 'true', 'false');
  Result.Kind := akBoolean;
end;

{ TSentryLogItem }

procedure TSentryLogItem.AddAttr(const AAttr: TSentryAttr);
begin
  if AAttr.Key.Trim.IsEmpty then
    Exit;
  Attributes := Attributes + [AAttr];
end;

function TSentryLogItem.HasAttr(const AKey: string): Boolean;
var
  I: Integer;
begin
  for I := Low(Attributes) to High(Attributes) do
    if SameText(Attributes[I].Key, AKey) then
      Exit(True);
  Result := False;
end;

{ TSentryTrace }

threadvar
  // Deliberadamente um array de Char e não uma string: threadvar com tipo
  // gerenciado não é finalizado de forma confiável quando a thread morre.
  GTraceId: array [0 .. 31] of Char;

class function TSentryTrace.Current: string;
begin
  if GTraceId[0] = #0 then
    Exit(BeginRequest);

  SetString(Result, PChar(@GTraceId[0]), Length(GTraceId));
end;

class function TSentryTrace.BeginRequest(const ATraceId: string): string;
var
  LId: string;
begin
  LId := ATraceId.Trim.ToLower;

  if Length(LId) <> 32 then
    LId := SentryNewTraceId;

  Move(PChar(LId)^, GTraceId[0], Length(GTraceId) * SizeOf(Char));
  Result := LId;
end;

class procedure TSentryTrace.EndRequest;
begin
  FillChar(GTraceId, SizeOf(GTraceId), 0);
end;

{ TSentryDsn }

class function TSentryDsn.Parse(const ADsn: string): TSentryDsn;
var
  LRest, LUserInfo, LHostPort, LPath: string;
  LPos: Integer;
  LSegments: TArray<string>;
  I: Integer;
begin
  Result := Default (TSentryDsn);
  Result.FRaw := Trim(ADsn);

  if Result.FRaw.IsEmpty then
    Exit;

  LPos := Pos('://', Result.FRaw);
  if LPos <= 0 then
    raise ESentryError.Create('DSN inválido: falta o esquema (http:// ou https://).');

  Result.FScheme := Copy(Result.FRaw, 1, LPos - 1).ToLower;
  LRest := Copy(Result.FRaw, LPos + 3, MaxInt);

  LPos := Pos('@', LRest);
  if LPos <= 0 then
    raise ESentryError.Create('DSN inválido: falta a chave pública antes do @.');

  LUserInfo := Copy(LRest, 1, LPos - 1);
  LRest := Copy(LRest, LPos + 1, MaxInt);

  // DSNs antigos traziam public:secret — só a parte pública importa hoje.
  LPos := Pos(':', LUserInfo);
  if LPos > 0 then
    Result.FPublicKey := Copy(LUserInfo, 1, LPos - 1)
  else
    Result.FPublicKey := LUserInfo;

  LPos := Pos('/', LRest);
  if LPos <= 0 then
    raise ESentryError.Create('DSN inválido: falta o id do projeto no final.');

  LHostPort := Copy(LRest, 1, LPos - 1);
  LPath := Copy(LRest, LPos + 1, MaxInt).TrimRight(['/']);

  LPos := Pos(':', LHostPort);
  if LPos > 0 then
  begin
    Result.FHost := Copy(LHostPort, 1, LPos - 1);
    Result.FPort := StrToIntDef(Copy(LHostPort, LPos + 1, MaxInt), 0);
  end
  else
  begin
    Result.FHost := LHostPort;
    Result.FPort := 0;
  end;

  LSegments := LPath.Split(['/']);
  if Length(LSegments) = 0 then
    raise ESentryError.Create('DSN inválido: falta o id do projeto no final.');

  Result.FProjectId := LSegments[High(LSegments)];

  Result.FPathPrefix := '';
  for I := Low(LSegments) to High(LSegments) - 1 do
    if not LSegments[I].IsEmpty then
      Result.FPathPrefix := Result.FPathPrefix + '/' + LSegments[I];

  if Result.FHost.IsEmpty or Result.FPublicKey.IsEmpty or Result.FProjectId.IsEmpty then
    raise ESentryError.Create('DSN inválido: ' + ADsn);
end;

function TSentryDsn.IsValid: Boolean;
begin
  Result := not(FHost.IsEmpty or FPublicKey.IsEmpty or FProjectId.IsEmpty);
end;

function TSentryDsn.EnvelopeUrl: string;
var
  LPort: string;
begin
  LPort := '';
  if (FPort > 0) and not((FScheme = 'https') and (FPort = 443)) and not((FScheme = 'http') and (FPort = 80)) then
    LPort := ':' + FPort.ToString;

  Result := Format('%s://%s%s%s/api/%s/envelope/', [FScheme, FHost, LPort, FPathPrefix, FProjectId]);
end;

function TSentryDsn.AuthHeaderValue(const AClientName: string): string;
begin
  Result := Format('Sentry sentry_version=7, sentry_client=%s, sentry_key=%s', [AClientName, FPublicKey]);
end;

{ TSentryEvent }

constructor TSentryEvent.Create;
begin
  inherited Create;
  FEventId := SentryNewEventId;
  FTimestampUtc := TTimeZone.Local.ToUniversalTime(Now);
  // O trace da thread que está criando o evento. Para CaptureException isso
  // é a thread da requisição — exatamente o trace das linhas de log dela.
  FTraceId := TSentryTrace.Current;
  FLevel := slError;
  FTags := TDictionary<string, string>.Create;
  FExtra := TDictionary<string, string>.Create;
end;

destructor TSentryEvent.Destroy;
begin
  FTags.Free;
  FExtra.Free;
  inherited;
end;

class function TSentryEvent.LevelToStr(const ALevel: TSentryLevel): string;
begin
  Result := SentryEventLevelStr(ALevel);
end;

procedure TSentryEvent.FromException(const E: Exception);
begin
  if not Assigned(E) then
    Exit;

  FExceptionType := E.ClassName;
  FExceptionValue := E.Message;

  if FMessageText.IsEmpty then
    FMessageText := E.Message;

  try
    FStackTraceText := E.StackTrace;
  except
    FStackTraceText := '';
  end;
end;

procedure TSentryEvent.SetTag(const AKey, AValue: string);
begin
  if AKey.Trim.IsEmpty then
    Exit;
  // Tags do Sentry/GlitchTip têm limite de 200 chars no valor.
  FTags.AddOrSetValue(AKey, Copy(AValue, 1, 200));
end;

procedure TSentryEvent.SetExtra(const AKey, AValue: string);
begin
  if AKey.Trim.IsEmpty then
    Exit;
  FExtra.AddOrSetValue(AKey, AValue);
end;

function TSentryEvent.BuildStacktraceJSON: TJSONObject;
var
  LFrames: TJSONArray;
  LLines: TArray<string>;
  LFrame: TJSONObject;
  I: Integer;
  LLine: string;
begin
  Result := nil;
  if FStackTraceText.Trim.IsEmpty then
    Exit;

  LLines := FStackTraceText.Replace(#13#10, #10).Replace(#13, #10).Split([#10]);
  LFrames := TJSONArray.Create;

  // O Sentry espera os frames do mais antigo para o mais recente; madExcept
  // e JclDebug entregam ao contrário. Por isso o laço é de trás pra frente.
  for I := High(LLines) downto Low(LLines) do
  begin
    LLine := LLines[I].Trim;
    if LLine.IsEmpty then
      Continue;

    LFrame := TJSONObject.Create;
    LFrame.AddPair('function', LLine);
    LFrame.AddPair('in_app', TJSONBool.Create(True));
    LFrames.AddElement(LFrame);
  end;

  if LFrames.Count = 0 then
  begin
    LFrames.Free;
    Exit;
  end;

  Result := TJSONObject.Create;
  Result.AddPair('frames', LFrames);
end;

function TSentryEvent.BuildExceptionJSON: TJSONObject;
var
  LValues: TJSONArray;
  LValue: TJSONObject;
  LStack: TJSONObject;
begin
  Result := nil;
  if FExceptionType.Trim.IsEmpty then
    Exit;

  LValue := TJSONObject.Create;
  LValue.AddPair('type', FExceptionType);
  LValue.AddPair('value', FExceptionValue);

  LStack := BuildStacktraceJSON;
  if Assigned(LStack) then
    LValue.AddPair('stacktrace', LStack);

  LValues := TJSONArray.Create;
  LValues.AddElement(LValue);

  Result := TJSONObject.Create;
  Result.AddPair('values', LValues);
end;

function TSentryEvent.ToJSONString: string;
var
  LRoot: TJSONObject;
  LTags: TJSONObject;
  LExtra: TJSONObject;
  LMessage: TJSONObject;
  LException: TJSONObject;
  LContexts: TJSONObject;
  LTraceCtx: TJSONObject;
  LFingerprint: TJSONArray;
  LPair: TPair<string, string>;
  LItem: string;
  LTraceId: string;
begin
  LRoot := TJSONObject.Create;
  try
    LRoot.AddPair('event_id', FEventId);
    LRoot.AddPair('timestamp', DateToISO8601(FTimestampUtc, True));
    LRoot.AddPair('platform', 'other');
    LRoot.AddPair('level', LevelToStr(FLevel));

    if not FLogger.IsEmpty then
      LRoot.AddPair('logger', FLogger);
    if not FTransaction.IsEmpty then
      LRoot.AddPair('transaction', FTransaction);
    if not FServerName.IsEmpty then
      LRoot.AddPair('server_name', FServerName);
    if not FReleaseVersion.IsEmpty then
      LRoot.AddPair('release', FReleaseVersion);
    if not FEnvironment.IsEmpty then
      LRoot.AddPair('environment', FEnvironment);

    if not FMessageText.IsEmpty then
    begin
      LMessage := TJSONObject.Create;
      LMessage.AddPair('formatted', FMessageText);
      LRoot.AddPair('message', LMessage);
    end;

    LException := BuildExceptionJSON;
    if Assigned(LException) then
      LRoot.AddPair('exception', LException);

    if Length(FFingerprint) > 0 then
    begin
      LFingerprint := TJSONArray.Create;
      for LItem in FFingerprint do
        LFingerprint.Add(LItem);
      LRoot.AddPair('fingerprint', LFingerprint);
    end;

    LTraceId := FTraceId.Trim.ToLower;
    if Length(LTraceId) <> 32 then
      LTraceId := '';

    if not LTraceId.IsEmpty then
    begin
      // contexts.trace é onde o protocolo espera o vínculo. span_id é
      // obrigatório no contexto, mas não rastreamos spans — vai um id
      // sintético só para o payload ser válido.
      LTraceCtx := TJSONObject.Create;
      LTraceCtx.AddPair('trace_id', LTraceId);
      LTraceCtx.AddPair('span_id', Copy(SentryNewTraceId, 1, 16));
      LContexts := TJSONObject.Create;
      LContexts.AddPair('trace', LTraceCtx);
      LRoot.AddPair('contexts', LContexts);
    end;

    if (FTags.Count > 0) or not LTraceId.IsEmpty then
    begin
      LTags := TJSONObject.Create;
      // Também como tag porque é o que a UI mostra na issue e aceita na
      // busca (trace:abc...). O chamador pode sobrescrever com SetTag.
      if (not LTraceId.IsEmpty) and not FTags.ContainsKey('trace') then
        LTags.AddPair('trace', LTraceId);
      for LPair in FTags do
        LTags.AddPair(LPair.Key, LPair.Value);
      LRoot.AddPair('tags', LTags);
    end;

    if FExtra.Count > 0 then
    begin
      LExtra := TJSONObject.Create;
      for LPair in FExtra do
        LExtra.AddPair(LPair.Key, LPair.Value);
      LRoot.AddPair('extra', LExtra);
    end;

    Result := LRoot.ToJSON;
  finally
    LRoot.Free;
  end;
end;

function TSentryEvent.DedupeKey: string;
var
  LText: string;
  LBuilder: TStringBuilder;
  C: Char;
begin
  LText := FExceptionType + '|' + FTransaction + '|' + Copy(FMessageText, 1, 120);

  LBuilder := TStringBuilder.Create;
  try
    for C in LText do
      if not C.IsDigit then
        LBuilder.Append(C);
    Result := LBuilder.ToString;
  finally
    LBuilder.Free;
  end;
end;

{ TSentryTransport }

constructor TSentryTransport.Create(const ADsn: TSentryDsn; const ATimeoutMs: Integer);
begin
  inherited Create;
  FDsn := ADsn;
  FTimeoutMs := ATimeoutMs;

  if FDsn.IsValid then
  begin
    FUrl := FDsn.EnvelopeUrl;
    FAuthHeader := FDsn.AuthHeaderValue;
  end;
end;

function TSentryTransport.Assemble(const AItemHeader: TJSONObject; const APayload: TBytes; const AEventId: string): TBytes;
var
  LHeader: TJSONObject;
  LText: string;
  LPrefix: TBytes;
begin
  try
    AItemHeader.AddPair('length', TJSONNumber.Create(Length(APayload)));

    LHeader := TJSONObject.Create;
    try
      if not AEventId.IsEmpty then
        LHeader.AddPair('event_id', AEventId);
      LHeader.AddPair('dsn', FDsn.Raw);
      LHeader.AddPair('sent_at', DateToISO8601(TTimeZone.Local.ToUniversalTime(Now), True));

      LText := LHeader.ToJSON + #10 + AItemHeader.ToJSON + #10;
    finally
      LHeader.Free;
    end;
  finally
    AItemHeader.Free;
  end;

  LPrefix := TEncoding.UTF8.GetBytes(LText);

  SetLength(Result, Length(LPrefix) + Length(APayload) + 1);
  if Length(LPrefix) > 0 then
    Move(LPrefix[0], Result[0], Length(LPrefix));
  if Length(APayload) > 0 then
    Move(APayload[0], Result[Length(LPrefix)], Length(APayload));
  Result[High(Result)] := 10; // #10 final
end;

function TSentryTransport.BuildEnvelope(const AEvent: TSentryEvent): TBytes;
var
  LItemHeader: TJSONObject;
  LPayload: TBytes;
begin
  // Payload primeiro: se ToJSONString levantar, ainda não existe header
  // nenhum para vazar (Assemble só assume a posse depois de ser chamado).
  LPayload := TEncoding.UTF8.GetBytes(AEvent.ToJSONString);

  LItemHeader := TJSONObject.Create;
  LItemHeader.AddPair('type', 'event');
  LItemHeader.AddPair('content_type', 'application/json');

  Result := Assemble(LItemHeader, LPayload, AEvent.EventId);
end;

function TSentryTransport.BuildLogEnvelope(const AItems: TArray<TSentryLogItem>): TBytes;
var
  LRoot: TJSONObject;
  LItems: TJSONArray;
  LItem: TJSONObject;
  LAttrs: TJSONObject;
  LAttr: TJSONObject;
  LItemHeader: TJSONObject;
  LPayload: string;
  I, J: Integer;
  LTrace: string;
begin
  Result := nil;
  if Length(AItems) = 0 then
    Exit;

  LItems := TJSONArray.Create;
  try
    for I := Low(AItems) to High(AItems) do
    begin
      LItem := TJSONObject.Create;
      LItems.AddElement(LItem); // dono desde já: se o resto falhar, não vaza
      LItem.AddPair('timestamp', TJSONNumber.Create(AItems[I].TimestampUnix));

      // trace_id é obrigatório e tem que ter 32 hex; sem ele o servidor
      // rejeita o lote inteiro, então nunca deixamos passar vazio.
      LTrace := AItems[I].TraceId.Trim.ToLower;
      if Length(LTrace) <> 32 then
        LTrace := SentryNewTraceId;
      LItem.AddPair('trace_id', LTrace);

      LItem.AddPair('level', SentryLogLevelStr(AItems[I].Level));
      LItem.AddPair('severity_number', TJSONNumber.Create(SentrySeverityNumber(AItems[I].Level)));
      LItem.AddPair('body', AItems[I].Body);

      if Length(AItems[I].Attributes) = 0 then
        Continue;

      LAttrs := TJSONObject.Create;
      LItem.AddPair('attributes', LAttrs);

      for J := Low(AItems[I].Attributes) to High(AItems[I].Attributes) do
      begin
        // AddPair com nome vazio é no-op e vazaria o objeto recém-criado,
        // então a chave é conferida antes de qualquer alocação.
        if AItems[I].Attributes[J].Key.Trim.IsEmpty then
          Continue;

        LAttr := TJSONObject.Create;
        LAttrs.AddPair(AItems[I].Attributes[J].Key, LAttr);

        case AItems[I].Attributes[J].Kind of
          akInteger:
            begin
              LAttr.AddPair('value', TJSONNumber.Create(StrToInt64Def(AItems[I].Attributes[J].Value, 0)));
              LAttr.AddPair('type', 'integer');
            end;
          akDouble:
            begin
              // Invariant: o valor foi formatado com ponto por TSentryAttr.Dbl.
              LAttr.AddPair('value', TJSONNumber.Create(StrToFloatDef(AItems[I].Attributes[J].Value, 0, TFormatSettings.Invariant)));
              LAttr.AddPair('type', 'double');
            end;
          akBoolean:
            begin
              LAttr.AddPair('value', TJSONBool.Create(SameText(AItems[I].Attributes[J].Value, 'true')));
              LAttr.AddPair('type', 'boolean');
            end;
        else
          LAttr.AddPair('value', AItems[I].Attributes[J].Value);
          LAttr.AddPair('type', 'string');
        end;
      end;
    end;

    LRoot := TJSONObject.Create;
    try
      LRoot.AddPair('items', LItems);
      LItems := nil; // a partir daqui quem libera é o LRoot
      LPayload := LRoot.ToJSON;
    finally
      LRoot.Free;
    end;
  except
    LItems.Free;
    raise;
  end;

  LItemHeader := TJSONObject.Create;
  LItemHeader.AddPair('type', 'log');
  LItemHeader.AddPair('content_type', 'application/vnd.sentry.items.log+json');
  LItemHeader.AddPair('item_count', TJSONNumber.Create(Length(AItems)));

  Result := Assemble(LItemHeader, TEncoding.UTF8.GetBytes(LPayload), '');
end;

function TSentryTransport.Send(const AEnvelope: TBytes): TSentrySendResult;
var
  LHttp: THTTPClient;
  LStream: TBytesStream;
  LResponse: IHTTPResponse;
  LHeaders: TNetHeaders;
  LRetryAfter: string;
begin
  Result := Default (TSentrySendResult);

  if FUrl.IsEmpty then
  begin
    Result.ErrorText := 'DSN não configurado.';
    Result.Retryable := False;
    Exit;
  end;

  LHeaders := [TNetHeader.Create('Content-Type', 'application/x-sentry-envelope'), TNetHeader.Create('X-Sentry-Auth', FAuthHeader),
    TNetHeader.Create('User-Agent', SENTRY_CLIENT_NAME)];

  LHttp := THTTPClient.Create;
  try
    LHttp.ConnectionTimeout := FTimeoutMs;
    LHttp.ResponseTimeout := FTimeoutMs;
    LHttp.SendTimeout := FTimeoutMs;
    LHttp.HandleRedirects := True;
    LHttp.UserAgent := SENTRY_CLIENT_NAME;

    LStream := TBytesStream.Create(AEnvelope);
    try
      try
        LResponse := LHttp.Post(FUrl, LStream, nil, LHeaders);
      except
        on E: Exception do
        begin
          // Rede fora, DNS, TLS, servidor inacessível: vale a pena tentar de novo.
          Result.Success := False;
          Result.Retryable := True;
          Result.StatusCode := 0;
          Result.ErrorText := E.ClassName + ': ' + E.Message;
          Exit;
        end;
      end;

      Result.StatusCode := LResponse.StatusCode;
      Result.Success := (LResponse.StatusCode >= 200) and (LResponse.StatusCode < 300);

      if Result.Success then
        Exit;

      Result.ErrorText := Format('HTTP %d %s — %s', [LResponse.StatusCode, LResponse.StatusText, Copy(LResponse.ContentAsString, 1, 500)]);

      if LResponse.StatusCode = 429 then
      begin
        Result.Retryable := True;
        LRetryAfter := LResponse.HeaderValue['Retry-After'];
        Result.RetryAfterSec := StrToIntDef(LRetryAfter.Trim, 60);
      end
      else
        // 5xx: problema do servidor, tenta de novo.
        // 4xx: payload/chave errados, insistir só gera lixo.
        Result.Retryable := LResponse.StatusCode >= 500;
    finally
      LStream.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

end.
