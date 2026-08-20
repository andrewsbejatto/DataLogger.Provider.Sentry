{
  DataLogger.Provider.Sentry — envia os logs de erro do DataLogger para o
  GlitchTip (ou Sentry) sem mexer nas chamadas de log já existentes.

  Este provider NÃO faz HTTP dentro do Save: ele só entrega o evento para o
  TSentryReporter, que grava na fila em disco e envia em thread própria.
  Assim o pipeline do DataLogger nunca fica preso esperando a rede.

  Uso:

    TSentryReporter.Configure(LOpt);   // ver SentryReporter.pas

    Logger
      .AddProvider(TProviderSQLite.Create...)          // continua igual
      .AddProvider(TProviderSentry.Create);            // novo destino

  Dois destinos, a partir do mesmo TLoggerItem:

    - TODOS os níveis viram linha na aba **Logs** (o rastro de rotina).
    - Error e Fatal, além disso, viram issue na aba **Issues** (o que agrupa
      e dispara alerta).

  Ou seja: o GlitchTip passa a receber o mesmo que o seu SQLite recebe hoje.
  Isso é o que você pediu, mas tem preço — leia a seção de volume no README
  antes de ligar isso nos 50 clientes de uma vez.
}
unit DataLogger.Provider.Sentry;

interface

uses
  DataLogger.Provider,
  DataLogger.Types,
  DataLogger.Utils,
  System.SysUtils,
  System.JSON,
  System.Classes,
  SentryClient,
  SentryReporter;

type
  TProviderSentry = class(TDataLoggerProvider<TProviderSentry>)
  private
    FIssueLevels: TLoggerLevels;
    FLogLevels: TLoggerLevels;
    FSendLogs: Boolean;
    FSendTagAsSentryTag: Boolean;
    function MapLevel(const ALevel: TLoggerLevel): TSentryLevel;
    function LevelsToString(const ALevels: TLoggerLevels): string;
    function LevelsFromString(const AValue: string; const ADefault: TLoggerLevels): TLoggerLevels;
    function BuildAttrs(const AItem: TLoggerItem): TArray<TSentryAttr>;
  protected
    procedure Save(const ACache: TArray<TLoggerItem>); override;
  public
    /// Quais níveis viram issue (agrupam e alertam). Padrão: [Error, Fatal].
    function IssueLevels(const AValue: TLoggerLevels): TProviderSentry;
    /// Quais níveis viram linha na aba Logs. Padrão: todos.
    function LogLevels(const AValue: TLoggerLevels): TProviderSentry;
    /// Liga/desliga o envio para a aba Logs. Padrão: True.
    function SendLogs(const AValue: Boolean): TProviderSentry;
    /// Se o `Tag` do DataLogger deve virar a tag `modulo` no GlitchTip. Padrão: True.
    function SendTagAsSentryTag(const AValue: Boolean): TProviderSentry;

    procedure LoadFromJSON(const AJSON: string); override;
    function ToJSON(const AFormat: Boolean = False): string; override;

    constructor Create;
  end;

implementation

{ TProviderSentry }

constructor TProviderSentry.Create;
begin
  inherited Create;

  FIssueLevels := [TLoggerLevel.Error, TLoggerLevel.Fatal];
  FLogLevels := [TLoggerLevel.Trace, TLoggerLevel.Debug, TLoggerLevel.Info, TLoggerLevel.Success, TLoggerLevel.Warn, TLoggerLevel.Error,
    TLoggerLevel.Fatal, TLoggerLevel.Custom];
  FSendLogs := True;
  FSendTagAsSentryTag := True;
end;

function TProviderSentry.IssueLevels(const AValue: TLoggerLevels): TProviderSentry;
begin
  Result := Self;
  FIssueLevels := AValue;
end;

function TProviderSentry.LogLevels(const AValue: TLoggerLevels): TProviderSentry;
begin
  Result := Self;
  FLogLevels := AValue;
end;

function TProviderSentry.SendLogs(const AValue: Boolean): TProviderSentry;
begin
  Result := Self;
  FSendLogs := AValue;
end;

function TProviderSentry.SendTagAsSentryTag(const AValue: Boolean): TProviderSentry;
begin
  Result := Self;
  FSendTagAsSentryTag := AValue;
end;

function TProviderSentry.MapLevel(const ALevel: TLoggerLevel): TSentryLevel;
begin
  case ALevel of
    TLoggerLevel.Trace:
      Result := slTrace;
    TLoggerLevel.Debug:
      Result := slDebug;
    TLoggerLevel.Info, TLoggerLevel.Success:
      Result := slInfo;
    TLoggerLevel.Warn:
      Result := slWarning;
    TLoggerLevel.Fatal:
      Result := slFatal;
  else
    Result := slError;
  end;
end;

function TProviderSentry.BuildAttrs(const AItem: TLoggerItem): TArray<TSentryAttr>;
begin
  Result := [TSentryAttr.Str('logger', AItem.Name), TSentryAttr.Int('sequence', AItem.Sequence), TSentryAttr.Int('thread_id', AItem.ThreadID)];

  if not AItem.Tag.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('modulo', AItem.Tag)];
  if not AItem.ProcessId.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('process_id', AItem.ProcessId)];
  if not AItem.Id.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('log_id', AItem.Id)];
  if not AItem.MessageJSON.Trim.IsEmpty then
    Result := Result + [TSentryAttr.Str('message_json', AItem.MessageJSON)];
end;

procedure TProviderSentry.Save(const ACache: TArray<TLoggerItem>);
var
  LItem: TLoggerItem;
  LEvent: TSentryEvent;
  LTraceForItem: string;
  LIsIssue: Boolean;
  LBody: string;
begin
  if Length(ACache) = 0 then
    Exit;

  if not TSentryReporter.IsConfigured then
    Exit;

  for LItem in ACache do
  begin
    if LItem.InternalItem.IsSlinebreak or LItem.InternalItem.IsUndoLast then
      Continue;

    // O trace_id vai explícito e novo a cada item. Save roda na thread do
    // DataLogger, não na thread que atendeu a requisição, então o trace
    // por thread não diz nada aqui — herdá-lo daria a TODAS as linhas do
    // processo o mesmo id, o que é pior que não correlacionar.
    // Para correlacionar por requisição de verdade, chame Sentry.Log direto
    // no ponto da requisição, entre TSentryTrace.BeginRequest e EndRequest.
    LTraceForItem := SentryNewTraceId;
    LIsIssue := LItem.Level in FIssueLevels;

    // 1) rastro de rotina — aba Logs
    //
    // Quando o mesmo item também vira issue, a linha e a issue compartilham
    // o trace — a issue mostra o id na tag `trace`, e o sufixo no corpo
    // torna a linha encontrável pela busca de texto livre da aba Logs.
    if FSendLogs and (LItem.Level in FLogLevels) then
    begin
      LBody := LItem.Message;
      if LIsIssue then
        LBody := LBody + ' trace=' + LTraceForItem;
      Sentry.Log(MapLevel(LItem.Level), LBody, BuildAttrs(LItem), LTraceForItem);
    end;

    // 2) o que merece agrupamento e alerta — aba Issues
    if not LIsIssue then
      Continue;

    LEvent := Sentry.NewEvent(MapLevel(LItem.Level));
    try
      // Sobrescreve o trace herdado da thread do DataLogger (seria o mesmo
      // para todos os eventos do processo) pelo trace deste item.
      LEvent.TraceId := LTraceForItem;
      LEvent.MessageText := LItem.Message;
      LEvent.Logger := LItem.Name;
      LEvent.Transaction := LItem.Tag;

      // Sem objeto Exception aqui: o DataLogger só entrega texto.
      // O tipo vira o nível para o agrupamento continuar fazendo sentido.
      LEvent.ExceptionType := 'Log' + LItem.Level.ToString;
      LEvent.ExceptionValue := LItem.Message;

      if FSendTagAsSentryTag and not LItem.Tag.Trim.IsEmpty then
        LEvent.SetTag('modulo', LItem.Tag);

      LEvent.SetExtra('log_id', LItem.Id);
      LEvent.SetExtra('log_sequence', LItem.Sequence.ToString);
      LEvent.SetExtra('thread_id', LItem.ThreadID.ToString);
      LEvent.SetExtra('process_id', LItem.ProcessId);

      if not LItem.MessageJSON.Trim.IsEmpty then
        LEvent.SetExtra('message_json', LItem.MessageJSON);

      if not LItem.Username.Trim.IsEmpty then
        LEvent.SetExtra('usuario_so', LItem.Username);

      if not LItem.OSVersion.Trim.IsEmpty then
        LEvent.SetExtra('os_version', LItem.OSVersion);
    except
      LEvent.Free;
      raise;
    end;

    // Capture assume a posse do evento e o libera.
    Sentry.Capture(LEvent);
  end;
end;

function TProviderSentry.LevelsToString(const ALevels: TLoggerLevels): string;
var
  LLevel: TLoggerLevel;
begin
  Result := '';
  for LLevel := Low(TLoggerLevel) to High(TLoggerLevel) do
    if LLevel in ALevels then
    begin
      if not Result.IsEmpty then
        Result := Result + ',';
      Result := Result + LLevel.ToString;
    end;
end;

function TProviderSentry.LevelsFromString(const AValue: string; const ADefault: TLoggerLevels): TLoggerLevels;
var
  LNames: TArray<string>;
  LName: string;
  LLevel: TLoggerLevel;
begin
  if AValue.Trim.IsEmpty then
    Exit(ADefault);

  Result := [];
  LNames := AValue.Split([',']);

  for LName in LNames do
    for LLevel := Low(TLoggerLevel) to High(TLoggerLevel) do
      if SameText(LLevel.ToString, LName.Trim) then
      begin
        Result := Result + [LLevel];
        Break;
      end;

  if Result = [] then
    Result := ADefault;
end;

procedure TProviderSentry.LoadFromJSON(const AJSON: string);
var
  LValue: TJSONValue;
  LJO: TJSONObject;
begin
  if AJSON.Trim.IsEmpty then
    Exit;

  LValue := nil;
  try
    LValue := TJSONObject.ParseJSONValue(AJSON);
  except
    on E: Exception do
      Exit;
  end;

  if not Assigned(LValue) then
    Exit;

  // ParseJSONValue pode devolver um array ou escalar válido — nesse caso o
  // `as TJSONObject` levantaria EInvalidCast e vazaria o valor já alocado.
  if not(LValue is TJSONObject) then
  begin
    LValue.Free;
    Exit;
  end;

  LJO := TJSONObject(LValue);
  try
    SendTagAsSentryTag(LJO.GetValue<Boolean>('send_tag_as_sentry_tag', FSendTagAsSentryTag));
    SendLogs(LJO.GetValue<Boolean>('sentry_send_logs', FSendLogs));
    FIssueLevels := LevelsFromString(LJO.GetValue<string>('sentry_issue_levels', ''), FIssueLevels);
    FLogLevels := LevelsFromString(LJO.GetValue<string>('sentry_log_levels', ''), FLogLevels);
    SetJSONInternal(LJO);
  finally
    LJO.Free;
  end;
end;

function TProviderSentry.ToJSON(const AFormat: Boolean): string;
var
  LJO: TJSONObject;
begin
  LJO := TJSONObject.Create;
  try
    LJO.AddPair('send_tag_as_sentry_tag', TJSONBool.Create(FSendTagAsSentryTag));
    LJO.AddPair('sentry_send_logs', TJSONBool.Create(FSendLogs));
    LJO.AddPair('sentry_issue_levels', LevelsToString(FIssueLevels));
    LJO.AddPair('sentry_log_levels', LevelsToString(FLogLevels));
    ToJSONInternal(LJO);
    Result := TLoggerJSON.Format(LJO, AFormat);
  finally
    LJO.Free;
  end;
end;

end.
