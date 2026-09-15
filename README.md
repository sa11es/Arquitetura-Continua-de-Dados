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
