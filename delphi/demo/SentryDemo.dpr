{
  Teste de fumaça da integração. Compila para Windows e Linux 64.

    SentryDemo.exe "https://CHAVE@erros.suaempresa.com.br/3" 12345678000199

  Se tudo estiver certo, em alguns segundos aparecem 3 issues no GlitchTip,
  todas com a tag cnpj preenchida.
}
program SentryDemo;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  SentryClient in '..\SentryClient.pas',
  SentryReporter in '..\SentryReporter.pas';

type
  EPedidoInvalido = class(Exception);

procedure ConfigurarSentry(const ADsn, ACnpj: string);
var
  LOpt: TSentryOptions;
begin
  LOpt := TSentryOptions.Defaults;
  LOpt.Dsn := ADsn;
  LOpt.Cnpj := ACnpj;
  LOpt.ClienteNome := 'Cliente de Teste Ltda';
  LOpt.Environment := 'desenvolvimento';
  LOpt.ReleaseVersion := '0200820260000';
  LOpt.MaxEventsPerMinute := 60;
  LOpt.OnInternalLog := procedure(const AMessage: string)
    begin
      Writeln(AMessage);
    end;

  TSentryReporter.Configure(LOpt);
end;

procedure SimularErroSimples;
begin
  try
    raise EPedidoInvalido.Create('Pedido 4711 sem itens');
  except
    on E: Exception do
      Writeln('  event_id: ', Sentry.CaptureException(E, 'POST /api/v1/pedidos'));
  end;
end;

procedure SimularErroComContexto;
begin
  try
    raise EDivByZero.Create('Divisão por zero ao calcular o rateio');
  except
    on E: Exception do
      Writeln('  event_id: ', Sentry.CaptureException(E, 'Financeiro.CalcularRateio',
        procedure(const AEvent: TSentryEvent)
        begin
          AEvent.SetTag('modulo', 'financeiro');
          AEvent.SetExtra('id_transacao', '9f2c-4410');
          AEvent.SetExtra('parametros', '{"competencia":"2026-08","filial":3}');
        end));
  end;
end;

procedure SimularMensagem;
begin
  Writeln('  event_id: ', Sentry.CaptureMessage('Fila de integração acima de 10.000 registros', slWarning, 'Monitor.FilaIntegracao'));
end;

/// Simula uma requisição inteira: um trace por chamada, várias linhas de log
/// correlacionadas e um erro no fim. Na aba Logs você consegue ler a sequência
/// toda filtrando pelo trace_id.
procedure SimularRequisicaoComLogs;
var
  LTrace: string;
begin
  LTrace := TSentryTrace.BeginRequest;
  try
    Writeln('  trace_id: ', LTrace);

    Sentry.LogInfo('Requisição recebida', [TSentryAttr.Str('endpoint', 'POST /api/v1/nfe'), TSentryAttr.Str('ip', '10.0.0.7')]);
    Sentry.LogDebug('Validando schema do XML', [TSentryAttr.Int('tamanho_bytes', 48213)]);
    Sentry.LogInfo('Consultando SEFAZ', [TSentryAttr.Str('uf', 'SP')]);
    Sentry.LogWarn('SEFAZ respondeu devagar', [TSentryAttr.Int('duracao_ms', 8400)]);

    try
      raise EPedidoInvalido.Create('Rejeição 225: falha no schema do XML');
    except
      on E: Exception do
        Sentry.CaptureException(E, 'POST /api/v1/nfe');
    end;

    Sentry.LogError('Requisição encerrada com erro', [TSentryAttr.Int('http_status', 422)]);
  finally
    TSentryTrace.EndRequest;
  end;
end;

var
  LDsn, LCnpj: string;

begin
  try
    LDsn := ParamStr(1);
    LCnpj := ParamStr(2);

    if LDsn.Trim.IsEmpty then
    begin
      Writeln('uso: SentryDemo <dsn> [cnpj]');
      Exit;
    end;

    if LCnpj.Trim.IsEmpty then
      LCnpj := '00000000000191';

    ConfigurarSentry(LDsn, LCnpj);

    Writeln('1) exceção simples');
    SimularErroSimples;

    Writeln('2) exceção com tags e extras');
    SimularErroComContexto;

    Writeln('3) mensagem de aviso');
    SimularMensagem;

    Writeln('4) requisição completa com logs correlacionados');
    SimularRequisicaoComLogs;

    Writeln;
    Writeln('em buffer de log: ', Sentry.BufferedLogCount);
    Writeln('na fila: ', Sentry.PendingCount);
    Write('enviando... ');

    if Sentry.Flush(30000) then
      Writeln('fila vazia, tudo entregue.')
    else
      Writeln('sobrou ', Sentry.PendingCount, ' na fila — confira o DSN, a rede e o log acima.');

    TSentryReporter.Shutdown;
  except
    on E: Exception do
      Writeln('ERRO: ', E.ClassName, ': ', E.Message);
  end;

{$IF DEFINED(MSWINDOWS)}
  Writeln;
  Write('tecle ENTER...');
  Readln;
{$ENDIF}
end.
