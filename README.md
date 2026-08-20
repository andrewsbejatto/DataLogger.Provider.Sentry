# Centralização de erros do serviço Delphi com GlitchTip

Kit para trocar o par *madExcept (Windows) + DataLogger/SQLite (Linux)* por um destino único:
um GlitchTip self-hosted, com os erros dos ~50 clientes num só lugar e filtráveis por CNPJ.

```
  Cliente A (Windows)  ─┐
  Cliente B (Linux)    ─┼──►  fila em disco  ──►  HTTPS  ──►  GlitchTip  ──►  você
  ...  (50 instalações)─┘     (nunca perde)                   (agrupa)
```

## O que tem aqui

```
deploy/
  docker-compose.yml          stack do GlitchTip (Postgres + Valkey + web)
  .env.example                variáveis a preencher
  nginx/glitchtip.conf        proxy reverso + TLS
delphi/
  SentryClient.pas            protocolo (DSN, envelope de evento e de log, POST, trace)
  SentryReporter.pas          fachada + fila em disco + thread de envio + rate limit
                              + buffer de log em lote
  DataLogger.Provider.Sentry.pas   plugue para o DataLogger que você já usa
  demo/SentryDemo.dpr         teste de fumaça
```

Só depende da RTL (`System.Net.HttpClient`, `System.JSON`, `System.IOUtils`). Sem FireDAC, sem Indy,
sem componente de terceiro. Compila para Windows e Linux 64.

---

## Passo 1 — subir o GlitchTip

Num VPS seu (2 vCPU / 4 GB dá folga para 50 instalações):

```bash
cp .env.example .env
# edite .env:  SECRET_KEY (openssl rand -hex 32), POSTGRES_PASSWORD,
#              GLITCHTIP_DOMAIN, DEFAULT_FROM_EMAIL, EMAIL_URL
docker compose up -d
docker compose logs -f web     # as migrações rodam sozinhas
```

Depois aponte o DNS para o servidor, copie `nginx/glitchtip.conf` para
`/etc/nginx/sites-available/`, ative e emita o certificado:

```bash
ln -s /etc/nginx/sites-available/glitchtip.conf /etc/nginx/sites-enabled/
certbot --nginx -d erros.suaempresa.com.br
```

`GLITCHTIP_DOMAIN` precisa bater exatamente com a URL pública (com `https://`, sem barra no fim),
senão os links dos e-mails de alerta saem quebrados.

**Primeiro usuário:** o compose sobe com cadastro fechado. Crie o seu pelo shell:

```bash
docker compose exec web ./manage.py createsuperuser
```

Depois convide a equipe pela UI (*Settings → Members*).

## Passo 2 — criar o projeto e pegar o DSN

Na UI: crie uma organização, e dentro dela **um único projeto** (ex.: `api-servico`),
plataforma "Other".

> **Um projeto para os 50 clientes, não 50 projetos.** É contraintuitivo, mas é o
> ponto central do desenho: com um projeto só, a mesma exceção vira **uma** issue
> listando os CNPJs afetados. Com 50 projetos você vira caçador de erro repetido.
> Só divida em projetos se precisar de isolamento de acesso (cliente vendo os próprios erros).

Copie o DSN gerado — algo como `https://a1b2c3d4e5@erros.suaempresa.com.br/1`.
Esse mesmo DSN vai para as 50 instalações; o que muda em cada uma é o CNPJ.

## Passo 3 — integrar no serviço Delphi

Adicione `delphi/` ao *search path* e configure uma vez, na subida do serviço:

```pascal
uses
  SentryClient, SentryReporter;

procedure TServicoAPI.ConfigurarTelemetria;
var
  LOpt: TSentryOptions;
begin
  LOpt := TSentryOptions.Defaults;
  LOpt.Dsn            := FConfig.SentryDsn;        // igual em todas as instalações
  LOpt.Cnpj           := FConfig.CnpjCliente;      // o que você já tem configurado
  LOpt.ClienteNome    := FConfig.RazaoSocial;
  LOpt.Environment    := 'producao';
  LOpt.ReleaseVersion := 'meuservico@' + VERSAO_BUILD;
  LOpt.SpoolDir       := TPath.Combine(FConfig.PastaDados, 'sentry-spool');

  // diagnóstico do próprio reporter — mande para o seu log local de sempre
  LOpt.OnInternalLog  := procedure(const AMensagem: string)
                         begin
                           Logger.Debug(AMensagem);
                         end;

  TSentryReporter.Configure(LOpt);
end;
```

E no encerramento:

```pascal
TSentryReporter.Shutdown;   // dá até 10s para a fila esvaziar
```

### Capturando

Nos pontos onde hoje você chama o madExcept ou o `Logger.Error`:

```pascal
try
  ProcessarNFe(LDados);
except
  on E: Exception do
  begin
    Sentry.CaptureException(E, 'POST /api/v1/nfe',
      procedure(const AEvent: TSentryEvent)
      begin
        AEvent.SetTag('modulo', 'nfe');
        AEvent.SetExtra('id_transacao', LDados.Id);
        AEvent.SetExtra('chave_acesso', LDados.ChaveAcesso);
      end);
    raise;
  end;
end;
```

O segundo parâmetro (`transaction`) vira o *culprit* da issue. Use o nome do endpoint
ou da rotina — é o que aparece no título da lista e o que separa "erro no NF-e" de
"erro no financeiro".

### Ou sem tocar nas chamadas existentes

Se preferir não mexer nos pontos de log, plugue o provider no DataLogger:

```pascal
uses DataLogger, DataLogger.Provider.Sentry;

Logger
  .AddProvider(TProviderSQLite.Create)          // o que você já tem
  .AddProvider(TProviderSentry.Create);         // novo destino
```

O provider alimenta as **duas** abas a partir do mesmo item de log: todos os níveis
viram linha em *Logs*, e `Error`/`Fatal` viram issue além disso. Dá para reconfigurar:

```pascal
TProviderSentry.Create
  .IssueLevels([TLoggerLevel.Error, TLoggerLevel.Fatal])   // o que agrupa e alerta
  .LogLevels([TLoggerLevel.Warn, TLoggerLevel.Error,       // o que vira rastro
              TLoggerLevel.Fatal])
  .SendLogs(True);
```

As duas formas convivem: o provider pega o que já é logado hoje, e o `CaptureException`
você usa nos pontos onde quer contexto rico.

## A aba Logs

*Issues* agrupa erros e alerta. *Logs* guarda o rastro cru — o que você hoje grava
no SQLite de cada cliente. Chegam pelo mesmo DSN e o mesmo endpoint; o que muda é o
tipo do item dentro do envelope.

```pascal
Sentry.LogInfo('Requisição recebida',
  [TSentryAttr.Str('endpoint', 'POST /api/v1/nfe'),
   TSentryAttr.Int('tamanho_bytes', 48213)]);
```

Todas as linhas saem com `cnpj`, `cliente`, `server_name`, `so`, `sentry.environment` e
`sentry.release` como atributos, e com o CNPJ também no corpo — porque a aba Logs
**não** pesquisa atributo, só texto livre (ver "Passo 4"). Os atributos ficam ali para
leitura e para o dia em que o GlitchTip ganhar busca por atributo.

### Correlação por requisição

O protocolo exige `trace_id` (32 hex) em toda linha. Isso deixa de ser burocracia e
vira recurso se você abrir um trace por requisição:

```pascal
procedure TMinhaAPI.HandleRequest(...);
begin
  TSentryTrace.BeginRequest;      // um id novo, preso a esta thread
  try
    ...
  finally
    TSentryTrace.EndRequest;
  end;
end;
```

A partir daí toda linha de log e todo erro daquela requisição compartilham o mesmo
`trace_id`, e você lê a sequência inteira do que aconteceu antes da exceção. Se você
não chamar `BeginRequest`, cada thread ganha um trace próprio na primeira linha e
mantém até o fim — funciona, mas correlaciona por thread, não por requisição.

**Como isso aparece na UI.** O evento de erro leva o trace em dois lugares: em
`contexts.trace.trace_id` (o campo do protocolo) e na tag **`trace`**, que aparece
na issue ao lado de `cnpj` e é pesquisável (`trace:bff9aea6...`). Do lado dos logs,
a aba Logs só pesquisa texto livre no corpo — o `trace_id` enviado no campo próprio
não é filtrável — então cada linha correlacionada sai com o sufixo ` trace=<id>` no
corpo (`LogTraceInBody`, ligado por padrão). O fluxo de investigação fica:

1. abre a issue e copia o id da tag `trace`;
2. cola na busca de texto livre da aba Logs;
3. lê a sequência inteira daquela requisição, na ordem.

**Ressalva importante sobre o provider do DataLogger:** as linhas que chegam por ele
*não* correlacionam por requisição. O `Save` do DataLogger roda na thread dele, não na
que atendeu a requisição, então cada item recebe um `trace_id` próprio. Isso é
deliberado: herdar o trace daquela thread daria o mesmo id a todas as linhas do
processo, o que é pior que não correlacionar. Há uma exceção útil: quando o mesmo item
vira issue **e** linha de log (um `Logger.Error`, por exemplo), os dois compartilham o
mesmo trace — a issue mostra o id na tag `trace` e a linha correspondente é
encontrável pela busca. Onde a correlação por requisição importa, chame
`Sentry.LogInfo`/`LogWarn` direto no ponto da requisição, dentro do
`BeginRequest`/`EndRequest`.

### O que você precisa saber antes de ligar isso nos 50

**Volume.** Erro é raro; log de rotina não é. Se cada instalação atende 10 mil
requisições/dia com 5 linhas cada, são 2,5 milhões de linhas/dia somando os 50 clientes.
Comece com `LOG_RETENTION_DAYS` curto (30) e `LOG_HOT_DAYS=7`, meça o crescimento do
volume `pg_data` por uma semana, e só então decida quanto guardar.

**Armazenamento frio.** Passados os dias quentes, o log migra do Postgres para DuckDB +
Parquet e continua consultável. O compose já liga (`GLITCHTIP_ENABLE_DUCKDB`), mas
**confirme na doc da sua versão qual variável aponta o destino** e monte um volume
persistente nele — se o padrão cair dentro do container, você perde o histórico frio
no primeiro `docker compose down`.

**Janela de perda.** Diferente dos erros, o log não vai para o disco linha a linha —
seria brutal em I/O. Ele acumula num buffer em memória e vira arquivo a cada
`LogFlushIntervalMs` (padrão 2s) ou a cada 100 linhas, o que vier primeiro. Se o
processo morrer de forma abrupta, você perde até ~2 segundos de log. Erros não têm
essa janela: vão direto para a fila em disco.

Mantenha o SQLite local ligado enquanto avalia — ele é a caixa-preta sem janela de perda.

### Teste de fumaça

```
SentryDemo "https://CHAVE@erros.suaempresa.com.br/1" 12345678000199
```

Três issues devem aparecer na UI em poucos segundos.

## Passo 4 — consultar por cliente

Na tela de issues do projeto, o filtro aceita `chave:valor`:

```
cnpj:12345678000199                  todos os erros daquele cliente
cnpj:12345678000199 modulo:nfe       só o módulo NF-e daquele cliente
so:linux                             só as instalações Linux
release:meuservico@3.4.12            o que quebrou na última versão
```

**A aba Logs não filtra igual.** Ela oferece nível, service, environment e busca em
texto livre no corpo da mensagem — e **não** filtra por atributo arbitrário. O `cnpj`
vai como atributo em toda linha e aparece quando você abre a linha, mas não é
pesquisável como `cnpj:123...`. Só as issues têm busca por tag.

Por isso o cliente Delphi prefixa o corpo de cada linha com o CNPJ
(`LogPrefixCnpjInBody`, ligado por padrão):

```
[12345678000199] Requisição recebida
```

Aí a busca por cliente na aba Logs vira uma busca de texto livre por `12345678000199`.
Não é elegante, mas é o que a UI permite hoje e não depende de mudança no servidor.

O caminho de investigação mais comum nem passa por isso: alerta chega → abre a issue →
copia o id da tag `trace` → cola na busca de texto livre da aba Logs (funciona porque
toda linha correlacionada leva ` trace=<id>` no corpo) → lê a sequência inteira daquela
requisição. A issue já te disse de qual cliente ela é, então o CNPJ não entra na conta.

E o caminho inverso — o que interessa de verdade: abra uma issue e veja, na aba de tags,
a distribuição de `cnpj`. É assim que você descobre que aquele bug "de um cliente"
na verdade atinge 12.

**Alertas:** em *Project Settings → Alerts*, crie uma regra por e-mail ou webhook.
Um bom começo é "issue nova" e "issue com mais de N ocorrências em 1h" — não alerte
em toda ocorrência, senão vira ruído e você para de ler.

---

## Como o cliente Delphi se comporta

**Capturar nunca bloqueia.** `CaptureException` serializa o evento, grava um arquivo
na pasta da fila e retorna. Quem faz HTTP é uma thread separada. A latência da sua
API não muda nem se o GlitchTip estiver fora do ar.

**Nada se perde offline.** Servidor do cliente sem internet, GlitchTip em manutenção,
certificado expirado — os eventos ficam em disco e sobem quando a conexão voltar.
A fila tem teto (`MaxSpoolFiles`, padrão 5.000) e validade (`MaxSpoolAgeDays`, padrão 7 dias);
estourando, os mais antigos são descartados com aviso no log interno.

**Loop em erro não derruba nada.** `MaxEventsPerMinute` (padrão 20) limita eventos por
minuto **por tipo de erro**. A chave de deduplicação ignora números, então
"timeout após 1234ms" e "timeout após 5678ms" contam como o mesmo erro.

**Recuo educado.** Em HTTP 429 o cliente respeita o `Retry-After`; em 5xx recua 60s;
em 4xx permanente (payload inválido, chave errada) move o arquivo para
`sentry-spool/rejeitados/` em vez de insistir para sempre — é lá que você olha quando
"não chega nada".

---

## Migração a partir do que você tem hoje

Faça em três ondas, não de uma vez.

**Onda 1 — rodar em paralelo.** Adicione o `TProviderSentry` ao DataLogger e mantenha
tudo que existe: SQLite local e madExcept. Suba em 2 ou 3 clientes. Por uma semana você
compara: o que aparece no GlitchTip bate com o que está no SQLite?

**Onda 2 — o madExcept vira só produtor de stack.** Este é o ponto mais delicado da
migração, então vale ser direto: **o stack trace simbolizado é o que você perde no Linux.**
O madExcept resolve isso no Windows e não tem equivalente no Linux. Duas saídas:

- *Recomendada:* mantenha o madExcept instalado no build Windows apenas para preencher
  `Exception.StackTrace`. O `SentryClient` lê essa propriedade automaticamente e monta
  os frames. No Linux ela vem vazia e o evento sobe sem stack — compense com
  `transaction` bem nomeado e `SetExtra` com os parâmetros da operação. Na prática,
  saber *qual endpoint, com quais dados, em qual cliente* resolve a maioria dos casos
  tão bem quanto o stack.
- *Alternativa:* desinstale o madExcept e passe a registrar manualmente o ponto de
  origem. Mais trabalho no código, comportamento idêntico nos dois sistemas.

Em nenhuma das duas o madExcept continua enviando/gravando relatório: quem reporta é o
GlitchTip. O `.mes`/caixa de diálogo do madExcept não faz sentido num serviço sem interface.

**Onda 3 — ligar a aba Logs, em 2 ou 3 clientes primeiro.** Não ligue nos 50 de uma vez:
o volume de log de rotina é uma ordem de grandeza acima do de erros e você precisa medir
antes de dimensionar. Rode uma semana, olhe o crescimento do volume `pg_data`,
extrapole para 50 e só então decida `LOG_RETENTION_DAYS`. Se o número assustar, o meio
termo é `LogLevels([Warn, Error, Fatal])` no provider — `Info`/`Debug` seguem só no SQLite.

**Onda 4 — desligar o SQLite como fonte de consulta.** Mantenha o provider SQLite
gravando (é seu histórico local, sua rede de segurança e o único sem janela de perda),
mas pare de ir até o servidor do cliente para consultar. Se depois de um mês você não
abriu nenhum SQLite remoto, o serviço central substituiu o processo antigo.

---

## Checklist antes de mandar para os 50

- [ ] Backup do Postgres agendado (`pg_dump` diário do volume `pg_data`) — é onde mora todo o histórico
- [ ] `RETENTION_DAYS` compatível com o disco: ~30 GB por milhão de eventos/mês
- [ ] `LOG_RETENTION_DAYS` definido depois de **medir** o volume real, não por chute
- [ ] Destino do armazenamento frio (DuckDB/Parquet) apontado para volume persistente
- [ ] `TSentryTrace.BeginRequest`/`EndRequest` no início e no fim de cada requisição
- [ ] Alerta configurado, e alguém de fato recebendo o e-mail
- [ ] `ReleaseVersion` alimentado pelo número de build, não hardcoded — sem isso você não sabe em que versão o bug entrou
- [ ] `SpoolDir` numa pasta que o serviço tem permissão de escrita (atenção ao usuário do systemd no Linux)
- [ ] Nenhum dado pessoal indo no `SetExtra`. CNPJ e razão social são dados de empresa, tudo bem; CPF, nome, e-mail e endereço de cliente final, não — LGPD vale para log também
- [ ] Testado o cenário offline: derrube o GlitchTip, gere erros, suba de volta, confirme que subiram

## Um ponto de segurança que vale saber

Com um projeto só, as 50 instalações carregam a mesma chave pública do DSN. Ela é
projetada para ser semi-pública (fica embutida em app web e mobile mundo afora), mas
alguém com acesso ao servidor de um cliente pode ler essa chave e forjar eventos.
No seu cenário — servidores de clientes seus, atrás de contrato — o risco é baixo.
Se um dia incomodar, a solução é projeto por cliente, ao custo de perder a visão
agregada por bug.

---

## Referências

- [GlitchTip](https://glitchtip.com/) · [instalação](https://glitchtip.com/documentation/install/) · [notas da 5.2](https://glitchtip.com/blog/2025-11-13-glitchtip-5-2-released/)
- [Formato de envelope do Sentry](https://develop.sentry.dev/sdk/data-model/envelopes/)
- [DataLogger](https://github.com/dliocode/datalogger) (MIT)
