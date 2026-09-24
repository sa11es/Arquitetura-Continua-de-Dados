# 📊 TCC - Prova de Conceito (PoC)

**Arquitetura de Integração Contínua: Modernização de Sistemas Legados para Vigilância Epidemiológica em Tempo Real**

Modernização de sistemas legados de vigilância epidemiológica (inspirados no SINAN) através de uma arquitetura orientada a eventos, usando **Debezium (CDC)**, **Apache Kafka** e **Apache Flink**, com um dashboard em tempo real via **WebSocket**.

Autores: Luís Eduardo Salles Rosa e Murilo Flores
Orientador: Prof. Márcio Augusto de Souza | Coorientadora: Priscilla Vanessa Alves Santos

---

## 🎯 Visão geral

O projeto simula três municípios (Ponta Grossa, Curitiba e Castro), cada um com seu próprio banco de dados legado (PostgreSQL). Sempre que uma notificação epidemiológica é inserida em qualquer um desses bancos, o pipeline abaixo é disparado automaticamente, sem a necessidade de rotinas de ETL em lote:

```
PostgreSQL (3 cidades) → Debezium (CDC) → Kafka (tópicos raw)
    → Apache Flink (agregação com estado, em tempo real)
        → Kafka (tópico de alertas) → Dashboard (FastAPI + WebSocket)
```

O objetivo é demonstrar que é possível substituir o modelo tradicional de consolidação em lote (como o do SINAN, que pode levar dias/semanas) por um fluxo contínuo que disponibiliza a contagem de casos por doença/cidade/bairro em **tempo real**, na casa dos segundos.

---

## 🏗️ Arquitetura em 4 camadas

| Camada                   | Tecnologia                  | Função                                                                                |
| ------------------------ | --------------------------- | ------------------------------------------------------------------------------------- |
| **1. Origem e Ingestão** | PostgreSQL + Debezium (CDC) | Captura `INSERT`s na tabela `notificacoes` via WAL, sem impactar o banco transacional |
| **2. Transporte**        | Apache Kafka                | Barramento de eventos, um tópico por cidade + um tópico de alertas                    |
| **3. Inteligência**      | Apache Flink                | Processa os eventos com estado (`KeyedProcessFunction`) e calcula contagens por chave |
| **4. Ação e Consumo**    | FastAPI + WebSocket         | Recebe as contagens do Kafka e atualiza o dashboard ao vivo no navegador              |

### Por que `KeyedProcessFunction` em vez de `SlidingEventTimeWindows`?

A primeira versão do motor analítico usava uma janela deslizante nativa do Flink (`SlidingEventTimeWindows`, 24h de tamanho / 10s de slide). Isso causava uma explosão de estado: cada evento era replicado em `24h ÷ 10s = 8.640` janelas simultâneas, gerando backpressure e atrasos que impediam o dashboard de atualizar em tempo real.

A solução adotada mantém a mesma semântica (contagem das últimas 24h por chave), mas implementada via `KeyedProcessFunction`: cada chave guarda apenas a lista de timestamps ainda "ativos", com expiração via `registerEventTimeTimer`. Isso reduz o custo de O(tamanho/slide) para O(1) por evento, e emite a contagem atualizada imediatamente a cada notificação nova, em vez de esperar o próximo "slide".

---

## 📁 Estrutura do repositório

```
TCC- Prova de Conceito/
├── infra/
│   └── docker-compose.yml       # Orquestra todos os containers
├── database/
│   └── init.sql                 # Schema da tabela notificacoes + triggers
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/br/edu/tcc/
│       └── MotorAnalitico.java  # Job Flink (extração, chaves, KeyedProcessFunction)
├── dashboard/
│   ├── server.py                # Servidor FastAPI (consumer Kafka + WebSocket)
│   └── index.html               # Front-end do dashboard
├── simulator/
│   ├── auditoria.py             # Gera uma carga controlada (ex: 30 casos) para validação
│   ├── gerador.py                # Gerador de carga contínuo
│   ├── simulador_continuo.py
│   ├── metrics_collector.py     # Coleta métricas de latência/throughput (próximo passo)
│   └── teste.py
├── ativar_cdc_debezium.py       # Registra os 3 conectores Debezium (um por cidade)
└── README.md
```

---

## ✅ Status atual do projeto

- **Infraestrutura Docker**: `docker-compose.yml` configurado com 3 bancos PostgreSQL (um por cidade), Zookeeper, Kafka, Debezium Connect e um cluster Flink (1 JobManager + 1 TaskManager, 2 task slots).
- **Banco de dados legado**: schema `sinan_db` com a tabela `notificacoes`, populada via CDC (WAL, `wal_level=logical`).
- **CDC e Mensageria**: os 3 conectores Debezium (`sinan-postgres-ponta-grossa`, `sinan-postgres-curitiba`, `sinan-postgres-castro`) capturam inserções em tempo real e publicam nos tópicos Kafka `epidemiologia_<cidade>.public.notificacoes`.
- **Motor analítico (Flink)**: `MotorAnalitico.java` consome os 3 tópicos via padrão de regex, extrai e normaliza as notificações, agrega por `doenca|tipo|cidade[|bairro]` usando `KeyedProcessFunction`, e publica as contagens no tópico `epidemiologia.alertas`.
- **Dashboard em tempo real**: `server.py` consome o tópico de alertas e transmite as atualizações via WebSocket para o front-end, validado ponta a ponta — uma carga de teste inserida no Postgres aparece no dashboard em poucos segundos.

### ⚠️ Limitação conhecida (importante para reprodução dos testes)

Nenhum serviço no `docker-compose.yml` usa **volumes persistentes**. Isso significa que **todo `docker compose down` apaga os dados do Postgres, do Kafka e os conectores registrados no Debezium**. Sempre que o ambiente for recriado do zero, é obrigatório rodar `ativar_cdc_debezium.py` novamente antes de gerar qualquer carga de teste — veja o passo a passo abaixo.

---

## 🛠️ Pré-requisitos

- **Docker Desktop** (mantenha o aplicativo aberto)
- **Python 3.x** com `psycopg2-binary` (`pip install psycopg2-binary`)
- **Java 11+** (para o Flink)
- **Maven** — ou, alternativamente, o comando via Docker mostrado abaixo, que não exige instalação local
- Git e um editor de código (ex: VS Code)

---

## 🚀 Passo a passo completo (do zero até o dashboard ao vivo)

Siga **exatamente nesta ordem** — a ordem importa por causa da limitação de volumes citada acima.

### 1. Subir a infraestrutura

```bash
cd infra
docker compose up -d
```

Aguarde cerca de 1-2 minutos para todos os containers ficarem saudáveis. Confira com:

```bash
docker ps
```

Você deve ver 8 containers: `postgres-ponta-grossa` (porta 5433), `postgres-curitiba` (5434), `postgres-castro` (5435), `zookeeper`, `kafka` (9092), `debezium-connect` (8083), `flink-jobmanager` (8081) e `flink-taskmanager`.

### 2. Ativar o CDC (Debezium)

Na raiz do projeto:

```bash
python ativar_cdc_debezium.py
```

Confirme que os 3 conectores foram criados e estão rodando:

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/sinan-postgres-ponta-grossa/status
curl http://localhost:8083/connectors/sinan-postgres-curitiba/status
curl http://localhost:8083/connectors/sinan-postgres-castro/status
```

Cada resposta deve trazer `"state":"RUNNING"` tanto no `connector` quanto em cada `task`.

### 3. Compilar o job Flink

Sem precisar instalar o Maven na máquina, use o container oficial:

```bash
cd flink-job
docker run --rm -v "${PWD}:/app" -w /app maven:3.8.6-openjdk-11 mvn clean package -DskipTests
```

Confirme o jar gerado (o nome correto **não** tem o prefixo `original-`):

```bash
dir target\*.jar        # Windows (PowerShell)
ls target/*.jar         # Linux/Mac
```

### 4. Submeter o job ao cluster Flink

```bash
docker cp target/motor-analitico-epidemiologico-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/motor-analitico.jar
docker exec -it flink-jobmanager flink run -c br.edu.tcc.MotorAnalitico /opt/flink/motor-analitico.jar
```

Confirme na Web UI (`http://localhost:8081`) que o job aparece em **Running Jobs**, sem exceções na aba **Exceptions**.

### 5. Subir o dashboard

Em um novo terminal:

```bash
cd dashboard
python server.py
```

Acesse `http://localhost:8501` no navegador.

### 6. Gerar carga de teste

Em outro terminal:

```bash
cd simulator
python auditoria.py       # carga controlada (30 casos, ótimo para validação rápida)
# ou
python gerador.py         # carga contínua, para testes mais longos
```

### 7. Verificar

O dashboard deve atualizar em poucos segundos, refletindo os novos casos por doença/cidade. Para inspecionar os eventos brutos no Kafka a qualquer momento:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic epidemiologia_ponta_grossa.public.notificacoes --from-beginning
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic epidemiologia.alertas --from-beginning
```

---

## 🔄 Reiniciando o ambiente do zero

Como não há volumes persistentes, sempre que os containers forem recriados (`docker compose down` seguido de `up`), repita a partir do **passo 2** (ativar o CDC) antes de submeter o job ou gerar carga. Resumo rápido:

```bash
cd infra && docker compose up -d
python ativar_cdc_debezium.py          # obrigatório a cada reset
# recompilar/resubmeter o job Flink (passos 3 e 4)
# subir o dashboard (passo 5)
# gerar carga (passo 6)
```

## 🛑 Como desligar o ambiente

```bash
cd infra
docker compose down
```

(Lembre-se: isso apaga todos os dados e conectores, pelos motivos explicados acima.)

---

## 🐛 Troubleshooting — lições aprendidas durante o desenvolvimento

| Sintoma                                                           | Causa                                                                                                                                                                                                                                                                                                                      | Solução                                                                                                                                     |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Job Flink "RUNNING" mas dashboard nunca atualiza                  | O `KafkaSource` descobre tópicos que casam com o padrão regex **uma única vez**, no instante em que o job sobe. Se o Debezium ainda não tiver criado os tópicos nesse momento, o job nunca mais os encontra                                                                                                                | Adicionado `.setProperty("partition.discovery.interval.ms", "10000")` no `MotorAnalitico.java` — reavalia tópicos/partições a cada 10s      |
| `kafka-consumer-groups --describe` retorna "group does not exist" | Sem nenhuma partição atribuída (pelo motivo acima), o consumer nunca chega a commitar offset, então o grupo nunca é criado no Kafka                                                                                                                                                                                        | Mesma correção acima; também ajuda usar um `GROUP_ID` novo ao depurar, para eliminar dúvidas sobre offsets antigos                          |
| Dashboard nunca aparece nada mesmo com o Kafka recebendo dados    | Debezium não estava ativado após um reset do ambiente (containers recriados sem volumes)                                                                                                                                                                                                                                   | Sempre rodar `ativar_cdc_debezium.py` logo após subir os containers, antes de qualquer outra etapa                                          |
| Contagens no dashboard "travadas" e nunca caem, mesmo após 24h    | O `server.py` tem uma trava anti-oscilação (`deve_emitir_alerta`) que só aceita atualizações com contagem **maior** que a anterior — pensada para a versão antiga (janelas sobrepostas fora de ordem), mas incompatível com a nova lógica, em que a contagem pode legitimamente cair quando casos expiram da janela de 24h | Ajustar `deve_emitir_alerta` para aceitar atualizações mais recentes por timestamp, não apenas por contagem crescente (ver próximos passos) |

---

## 🎯 Próximos passos

1. **Corrigir a trava anti-oscilação do `server.py`** para refletir corretamente quedas legítimas de contagem quando casos saem da janela de 24h.
2. **Validar o `metrics_collector.py`** e formalizar a coleta dos KPIs definidos na metodologia do TCC:
   - **Latência ponta a ponta (Le2e)**: tempo entre a inserção no Postgres e a chegada do alerta processado no dashboard.
   - **Throughput**: eventos processados por segundo sob carga.
3. **Executar os cenários de teste da seção 3.4**:
   - _Cenário Basal_: fluxo linear e homogêneo de notificações.
   - _Cenário de Surto_: injeção abrupta de registros para estressar a vazão da arquitetura (pode exigir escalar o `flink-taskmanager` via `docker compose up -d --scale flink-taskmanager=2` para aumentar os task slots disponíveis).
4. **Adicionar volumes persistentes** ao `docker-compose.yml` (opcional, mas recomendado) para não perder dados/conectores a cada reinicialização durante o desenvolvimento.
5. **Documentar os resultados** comparando a implementação original (`SlidingEventTimeWindows`) com a atual (`KeyedProcessFunction`) em termos de latência e uso de estado, para a seção de resultados do TCC.



README MURILO // VERSÃO 23/09 (após ajustes de inserções com escalas corretas)

GUIA DE EXECUÇÃO DA POC — SISTEMA DE VIGILÂNCIA EPIDEMIOLÓGICA EM TEMPO REAL

Manual Operacional de Validação de Ponta a Ponta no Ambiente Windows (PowerShell)

23 de setembro de 2026

1. Pré-requisitos do Ambiente
Esta seção estabelece os componentes de software necessários para a reprodução integral da Prova de Conceito (PoC). O fluxo operacional dispensa a instalação local de Java Development Kit (JDK) ou Apache Maven no sistema hospedeiro, uma vez que o processo de compilação do artefato do Apache Flink é inteiramente encapsulado em contêineres Docker.

1.1 Softwares Necessários
Sistema Operacional: Microsoft Windows 10 ou 11 (64-bit) com terminal PowerShell 5.1 ou superior.
Docker Desktop: Instalado e em execução com suporte a contêineres Linux (WSL 2 backend).
Git: Cliente de versionamento distribuído configurado no PATH.
Python: Versão 3.10 ou superior instalada no hospedeiro.
1.2 Verificação do Docker Desktop
Antes de iniciar os procedimentos, certifique-se de que a interface de integração do daemon Docker está acessível:

powershell


docker version
Saída esperada:

text


Client:<br/>
 Version:           24.0.7<br/>
 API version:       1.43
 ...
Server: Docker Desktop<br/>
 Engine:<br/>
  Version:          24.0.7<br/>
  API version:      1.43
Critério de aprovação: Exibição bem-sucedida das seções Client e Server sem exceções de conexão nomeada de canal (named pipe).

1.3 Instalação de Dependências Python
Execute a instalação das bibliotecas requeridas para a execução do simulador de carga, conectores de banco e interface analítica:

powershell


pip install psycopg2-binary fastapi uvicorn websockets kafka-python
Critério de aprovação: Mensagem Successfully installed ou Requirement already satisfied para todos os pacotes requisitados.

2. Passo 1 — Inicialização da Infraestrutura Distribuída
A topologia base compreende instâncias isoladas de bancos de dados relacionais transacionais (PostgreSQL) para três municípios paranaenses, mensageria distribuída (Apache Kafka e Apache Zookeeper), motor de captura de dados alterados (Debezium Connect) e infraestrutura de processamento de fluxo (Apache Flink JobManager e TaskManager).

Atenção: A configuração do Compose não utiliza volumes persistentes mapeados para o disco rígido do hospedeiro. O comando docker compose down expurga completamente os registros dos bancos e tópicos Kafka, exigindo a reinscrição dos conectores CDC no Debezium.

2.1 Atualização de Código e Subida dos Contêineres
powershell


cd C:\Users\muril\Documents\TCC
git pull origin multi-databases
cd C:\Users\muril\Documents\TCC\infra
docker compose up -d
2.2 Verificação de Saúde dos Contêineres
powershell


docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
Saída esperada:

text


NAMES                   STATUS                        PORTS
kafka                   Up About a minute             0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
zookeeper               Up About a minute             2181/tcp, 2888/tcp, 3888/tcp
postgres-ponta-grossa   Up About a minute (healthy)   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp<br/>
postgres-curitiba       Up About a minute (healthy)   0.0.0.0:5434->5432/tcp, [::]:5434->5432/tcp<br/>
postgres-castro         Up About a minute (healthy)   0.0.0.0:5435->5432/tcp, [::]:5435->5432/tcp<br/>
debezium-connect        Up About a minute             0.0.0.0:8083->8083/tcp, [::]:8083->8083/tcp<br/>
flink-jobmanager        Up About a minute             0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp
flink-taskmanager       Up About a minute             6123/tcp, 8081/tcp
Critério de aprovação: Exatamente 8 contêineres em estado Up, com as 3 bases PostgreSQL reportando (healthy). Eventual aviso sobre contêiner órfão postgres-legado deve ser ignorado.

Nota: Aguarde 120 segundos para que o framework Java interno do Debezium Connect conclua o provisionamento da porta REST 8083.

3. Passo 2 — Ativação e Validação do CDC (Debezium)
O conector CDC realiza a extração de dados contínua baseada em leitura direta do log de escrita prévia (Write-Ahead Log — WAL) do PostgreSQL, propagando as notificações municipais em tópicos de granularidade única.

3.1 Verificação da Interface REST do Conector
powershell


Invoke-RestMethod http://localhost:8083/connectors
Critério de aprovação: A API deve responder com código HTTP 200, retornando um array (vazio ou com inscrições preexistentes), sem lançar falha de conexão.

3.2 Registro dos Conectores Municipais
powershell


cd C:\Users\muril\Documents\TCC
python ativar_cdc_debezium.py
3.3 Inspeção de Estado das Tarefas dos Conectores
powershell


$conectores = Invoke-RestMethod http://localhost:8083/connectors
foreach ($c in $conectores) {
    $r = Invoke-RestMethod "http://localhost:8083/connectors/$c/status"<br/>
    "$c  ->  $($r.connector.state)  |  tasks: $($r.tasks.state -join ',')"
}
Saída esperada:

text


sinan-postgres-connector  ->  RUNNING  |  tasks: FAILED<br/>
sinan-postgres-castro     ->  RUNNING  |  tasks: RUNNING<br/>
sinan-postgres-curitiba   ->  RUNNING  |  tasks: RUNNING<br/>
sinan-postgres-ponta-grossa ->  RUNNING  |  tasks: RUNNING
Critério de aprovação: Os conectores sinan-postgres-castro, sinan-postgres-curitiba e sinan-postgres-ponta-grossa devem estar estritamente em RUNNING tanto no conector quanto nas tasks. O conector legado sinan-postgres-connector com tarefas em FAILED é inofensivo.

3.4 Confirmação dos Tópicos no Apache Kafka
powershell


docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
Critério de aprovação: Presença confirmada de:

epidemiologia_curitiba.public.notificacoes
epidemiologia_ponta_grossa.public.notificacoes
epidemiologia_castro.public.notificacoes
epidemiologia.alertas
4. Passo 3 — Inspeção Estática e Compilação do Artefato Java
Para mitigar as instabilidades de consumo e falhas de retenção de janelas anteriores, o componente analítico adota processamento com retenção explícita de estado via KeyedProcessFunction e registro controlado por identificador de grupo versionado.

4.1 Validação Estrutural do Código-Fonte
powershell


cd C:\Users\muril\Documents\TCC
Get-ChildItem .\flink-job\src -Recurse -Filter *.java | Select-String -Pattern "KeyedProcessFunction|enableCheckpointing|setCheckpointStorage|FileSystemCheckpointStorage|GROUP_ID"
Critério de aprovação: Confirmação da importação da classe KeyedProcessFunction, invocação do método enableCheckpointing(60000) e atribuição de GROUP_ID = "motor-analitico-epidemiologico-v2".

4.2 Compilação Isolada via Contêiner Docker
powershell


cd C:\Users\muril\Documents\TCC\flink-job<br/>
docker run --rm -v "${PWD}:/usr/src/app" -w /usr/src/app maven:3.9-eclipse-temurin-17 mvn clean package -DskipTests
Critério de aprovação: Finalização do processo com a mensagem conclusiva [INFO] BUILD SUCCESS.

Nota Técnica: Tratamento de Caractere Oculto (BOM): Se a compilação falhar com o erro illegal character: '\ufeff', execute no PowerShell a remoção da marca d'água UTF-8 gerada por editores Windows e recompile:

powershell


$path = ".\src\main\java\br\edu\tcc\MotorAnalitico.java"
$content = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
if ($content[0] -eq [char]0xFEFF) {
    $content = $content.Substring(1)
    [System.IO.File]::WriteAllText($path, $content, [System.Text.UTF8Encoding]::new($false))
}
4.3 Confirmação do Pacote Gerado
powershell


Get-ChildItem .\target\*.jar | Select-Object Name, Length, LastWriteTime
Critério de aprovação: Geração do arquivo motor-analitico-epidemiologico-1.0-SNAPSHOT.jar (fat JAR com dependências) com tamanho aproximado de 2,8 MB (2.799.576 bytes). O arquivo de prefixo original-* (14 KB) não deve ser utilizado.

5. Passo 4 — Submissão do Job ao Cluster Apache Flink
O envio do pacote compilado ao Flink JobManager é realizado por chamadas REST multipart, seguido pelo acionamento assíncrono do grafo de execução.

5.1 Envio do Artefato Binário
powershell


cd C:\Users\muril\Documents\TCC\flink-job<br/>
curl.exe -sS -X POST -H "Expect:" -F "jarfile=@target/motor-analitico-epidemiologico-1.0-SNAPSHOT.jar" http://localhost:8081/jars/upload
Saída esperada:

json


{"filename":"/tmp/flink-web-upload/42515378-136d-4a51-93be-28b3259ddc27_motor-analitico-epidemiologico-1.0-SNAPSHOT.jar","status":"success"}
5.2 Disparo do Grafo de Operadores
Isole exclusivamente o nome do arquivo gerado no campo filename (removendo o prefixo de diretório):

powershell


$jarId = "42515378-136d-4a51-93be-28b3259ddc27_motor-analitico-epidemiologico-1.0-SNAPSHOT.jar"
curl.exe -sS -X POST "http://localhost:8081/jars/$jarId/run?entry-class=br.edu.tcc.MotorAnalitico&parallelism=1"
Saída esperada:

json


{"jobid":"8219b18c6027c484b202f3495b90c3ca"}
5.3 Validação de Registro do Job
powershell


Invoke-RestMethod http://localhost:8081/jobs/overview
Critério de aprovação: Localização do job Motor Analitico Epidemiologico em estado operacional RUNNING. Armazene o identificador alfanumérico retornado na variável $jobId.

6. Passo 5 — Auditoria de Saúde: Checkpoints e Retenção Kafka
Esta etapa audita os mecanismos de tolerância a falhas do pipeline, garantindo que o acúmulo de estado assíncrono não degrade a integridade do nó executor.

6.1 Auditoria Quantitativa de Checkpoints
Aguarde 180 segundos a partir do provisionamento e consulte o coordenador:

powershell


$jobId = "8219b18c6027c484b202f3495b90c3ca"
curl.exe -sS "http://localhost:8081/jobs/$jobId/checkpoints" | ConvertFrom-Json | Select-Object -ExpandProperty counts
Saída esperada:

text


restored    : 0<br/>
total       : 2<br/>
in_progress : 0<br/>
completed   : 2<br/>
failed      : 0
Critério de aprovação: Valor de completed estritamente superior a zero e índice cumulativo de failed igual a zero.

6.2 Monitoramento do Grupo Consumidor
powershell


docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group motor-analitico-epidemiologico-v2
Saída esperada:

text


Consumer group 'motor-analitico-epidemiologico-v2' has no active members.

GROUP                             TOPIC                                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
motor-analitico-epidemiologico-v2 epidemiologia_castro.public.notificacoes       0          949             949             0
motor-analitico-epidemiologico-v2 epidemiologia_curitiba.public.notificacoes     0          1403            1403            0
motor-analitico-epidemiologico-v2 epidemiologia_ponta_grossa.public.notificacoes 0          1291            1291            0
Critério de aprovação: Diferencial de retardo (LAG) estritamente igual a 0 em todas as partições. O estado has no active members é padrão e esperado para a implementação nativa de KafkaSource orientada a checkpoints do Apache Flink.

7. Passo 6 — Injeção de Carga Sintética e Detecção de Surtos
O gerador sintético injeta uma distribuição realista de notificações com coordenadas e carimbos temporais, intercalando eventos de rotina com anomalias propositais para validação dos filtros analíticos.

7.1 Captura de Linha de Base dos Offsets
Execute a medição prévia de posição absoluta nos tópicos analíticos:

powershell


docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic epidemiologia.alertas --time -1<br/>
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic epidemiologia.contagens --time -1
Saída de referência:

text


epidemiologia.alertas:0:26148<br/>
epidemiologia.alertas:1:24474<br/>
epidemiologia.alertas:2:30866<br/>
epidemiologia.contagens:0:77265
7.2 Execução do Módulo Simulador
powershell


cd C:\Users\muril\Documents\TCC
python .\simulator\gerador.py
Saída esperada:

text


Ciclo de 300s = 24h simuladas | 3516 notificacoes (3491 de preenchimento)
Surto planejado: 3 casos de MENINGITE em Castro (bairro ancora: CENTRO)

[1/3] SURTO MENINGITE -> Castro (bairro: MOCAMINHO)<br/>
[2/3] SURTO MENINGITE -> Castro (bairro: VILA ISA)<br/>
[3/3] SURTO MENINGITE -> Castro (bairro: COHAB)

Fim do ciclo: 3516/3516 inseridos | Surto de MENINGITE: 3/3 casos injetados em Castro
7.3 Verificação de Gravação Transacional
powershell


docker exec postgres-curitiba psql -U postgres -d sinan_db -c "SELECT COUNT(*) FROM public.notificacoes;"
Critério de aprovação: Contagem de registros superior a 4.000 linhas no banco sinan_db de Curitiba, confirmando inserção efetiva.

7.4 Avaliação de Incremento no Kafka
Reexecute a inspeção de offsets:

powershell


docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic epidemiologia.alertas --time -1
Saída esperada após carga:

text


epidemiologia.alertas:0:27358<br/>
epidemiologia.alertas:1:25524<br/>
epidemiologia.alertas:2:32123
Critério de aprovação: Diferencial positivo estrito em relação à linha de base em todas as partições do tópico epidemiologia.alertas.

7.5 Leitura Dirigida de Alertas e Validação de Formato
Consuma os dados gerados utilizando a posição da linha de base como offset de partida:

powershell


docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic epidemiologia.alertas --partition 0 --offset 26148 --timeout-ms 30000 --max-messages 5
Estrutura JSON validada:

json


{"doenca":"POPULAR","atualizado_em":"2026-09-23T23:23:03.155288Z","municipio":"CURITIBA","bairro":"CENTRO","chave":"POPULAR|CIDADE|4106902","codigo_ibge":"4106902","tipo_agregacao":"CIDADE","quantidade_casos":1}
7.6 Confirmação da Regra Epidemiológica do Surto
Filtre os registros analíticos do surto induzido na partição correspondente:

powershell


docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic epidemiologia.alertas --partition 0 --offset 26148 --timeout-ms 30000 --max-messages 2000 | Select-String "MENINGITE" | Select-Object -First 5
Saída esperada:

json


{"doenca":"MENINGITE","atualizado_em":"2026-09-23T23:27:50.737859Z","municipio":"CASTRO","bairro":"COHAB","chave":"MENINGITE|CIDADE|4104153","codigo_ibge":"4104153","tipo_agregacao":"CIDADE","quantidade_casos":3}
Critério de aprovação: Identificação de mensagem com municipio: "CASTRO", doenca: "MENINGITE" e quantidade_casos: 3, atestando a detecção exata dos 3 eventos injetados pelo simulador.

8. Passo 7 — Visualização Analítica no Dashboard em Tempo Real
A camada de apresentação compreende um serviço desacoplado que ingere eventos analíticos do Kafka via consumidor interno e disponibiliza agregados via WebSocket e endpoints HTTP REST.

8.1 Inicialização do Servidor (Terminal 1)
Abra uma janela exclusiva do PowerShell e inicialize o serviço:

powershell


cd C:\Users\muril\Documents\TCC
python .\dashboard\server.py
Saída esperada no console:

text


INFO:     Started server process [13828]<br/>
INFO:     Application startup complete.<br/>
INFO:     Uvicorn running on http://0.0.0.0:8501 (Press CTRL+C to quit)
[Kafka] Consumer iniciado — aguardando contagens do Flink...
8.2 Consulta à API de Estatísticas (Terminal 2)
Em um terminal paralelo, audite o endpoint de extração:

powershell


Invoke-RestMethod http://localhost:8501/api/stats
8.3 Interface de Usuário
Acesse pelo navegador web o endereço http://localhost:8501.

Critérios de aprovação:

Disponibilidade operacional do serviço HTTP respondendo na porta 8501.
Agrupamento estrito de municípios sob caixa única normalizada (CURITIBA, CASTRO, PONTA GROSSA), eliminando duplicatas.
Exibição do registro de MENINGITE em Castro com contagem consolidada igual a 3.
Atualização reativa dos componentes gráficos sem necessidade de recarregamento manual de página via WebSocket.
9. Sumário Executivo de Validação Operacional (23/09/2026)
A execução consolidada em 23 de setembro de 2026 homologou a robustez da arquitetura de fluxo contínuo implementada, comprovando sua viabilidade técnica para observabilidade em saúde pública.

Parâmetro de Auditoria	Comportamento Anterior (Job Legado)	Resultado Homologado (Motor v2)
Estratégia de Janelamento	SlidingEventTimeWindows (24h/5min)	KeyedProcessFunction com Retenção de Estado
Estabilidade de Checkpoints	137 falhas consecutivas em 158 disparos	33 checkpoints concluídos com sucesso (0 falhas)
Tamanho de Estado Serializado	~24 MB (estouro do limite de memória de 5 MB)	< 500 KB (compatível com retenção em memória)
Vazão sob Carga Agrupada	Operadores reiniciados em loop (attempt 161+)	3.516 eventos absorvidos sem backpressure
Retardo de Consumo (Kafka LAG)	Acúmulo de 660 mensagens represadas	LAG 0 em todos os tópicos particionados
Normalização Territorial	Entradas divergentes ("Curitiba" vs "CURITIBA")	Padronização consistente via toUpperCase()
Sensibilidade a Surtos	Indeterminado por travamento do grafo analítico	Detecção exata de surto (3 casos de Meningite)
10. Guia Rápido de Solução de Problemas (Troubleshooting)
Catálogo de comportamentos anômalos comuns durante o ciclo de implantação e seus respectivos planos de contenção imediata.

10.1 Falha de Comunicação com Daemon Docker (npipe)
Sintoma: failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine.
Causa: Processo em segundo plano do Docker Desktop inativo ou motor WSL 2 finalizado.
Ação Corretiva: Inicie o Docker Desktop pelo atalho do sistema e execute repetidamente docker version até que o bloco Server seja reportado.
10.2 Conector Legado em Estado Falho (sinan-postgres-connector)
Sintoma: Verificação do Debezium reporta sinan-postgres-connector -> RUNNING | tasks: FAILED.
Causa: Resquício de metadados buscando a instância extinta postgres-legado.
Ação Corretiva: Inofensivo para a execução atual. Caso deseje limpar o conector do catálogo, execute: curl.exe -X DELETE http://localhost:8083/connectors/sinan-postgres-connector.
10.3 Falha de Checkpoint Assíncrono (Asynchronous task checkpoint failed)
Sintoma: Consulta ao Flink indica status: FAILED e mensagem Size of the state is larger than the maximum permitted memory-backed state.
Causa: Utilização de MemoryStateBackend padrão com estado cumulativo excedendo o teto de 5.242.880 bytes (5 MB).
Ação Corretiva: A migração para KeyedProcessFunction suprime este problema por manter o estado compacto. Para volumes de produção, configure persistência externa via FileSystemCheckpointStorage("file:///opt/flink/checkpoints") com montagem de volume mapeado entre JobManager e TaskManager no docker-compose.yml.
10.4 Erro de Conexão na Consulta REST do Dashboard
Sintoma: Invoke-RestMethod : Impossível conectar-se ao servidor remoto na porta 8000.
Causa: Mapeamento de porta incorreto na chamada ou ausência de inicialização do processo.
Ação Corretiva: O backend analítico server.py utiliza nativamente a porta 8501. Assegure que o Terminal 1 esteja com o Uvicorn ativo e aponte as requisições para http://localhost:8501/api/stats.
10.5 Consumidor Kafka Retorna Zero Mensagens com Timeout
Sintoma: Processed a total of 0 messages acompanhado de TimeoutException.
Causa: Utilização de auto.offset.reset=latest em tópicos cujas mensagens foram propagadas antes do início da escuta do consumidor.
Ação Corretiva: Realize o consumo direcionado via --partition e atribuição manual do --offset base anotado antes da injeção de dados.
10.6 Comando Maven Não Reconhecido no Shell
Sintoma: mvn : O termo 'mvn' não é reconhecido como nome de cmdlet.
Causa: Ausência de variáveis de ambiente do Apache Maven ou JDK configuradas no hospedeiro.
Ação Corretiva: Não instale dependências manuais no sistema operacional hospedeiro. Utilize estritamente o contêiner efêmero de compilação fornecido no Passo 3 (docker run ... maven:3.9-eclipse-temurin-17).