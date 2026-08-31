# 📊 TCC - Prova de Conceito (PoC)

**Modernização de sistemas legados de vigilância epidemiológica via arquitetura orientada a eventos em tempo real com Debezium CDC, Apache Kafka e Apache Flink.**

## ✅ Status Atual do Projeto

- **Infraestrutura Base (Docker):** O `docker-compose.yml` está configurado e testado com PostgreSQL, Zookeeper, Kafka, Debezium e Flink.
- **Banco de Dados Legado:** A porta externa foi mapeada para **`5433`** (evitando conflitos com instalações locais do Windows). O banco `sinan_db` e a tabela `notificacoes_dengue` sobem criados automaticamente, incluindo as *triggers* necessárias para o CDC.
- **Estratégia de Dados da Dupla:** Os ambientes Docker rodam de forma isolada na máquina de cada um. O script simulador injeta a carga inicial de forma padronizada.
- **CDC e Mensageria (Concluído hoje! 🚀):** O Debezium foi ativado com sucesso e os eventos de inserção no banco legado já estão chegando no Kafka em tempo real.

---

## 🛠️ Pré-requisitos

Certifique-se de ter instalado em sua máquina:
- **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** (Mantenha o aplicativo aberto no Windows)
- **[Python 3.x](https://www.python.org/downloads/)**
- **Git** e um editor de código (ex: VS Code)

---

## 🚀 Como rodar a Arquitetura (Tutorial de Ponta a Ponta)

Siga os passos abaixo no terminal do VS Code para executar a pipeline de dados em tempo real:

### 1. Atualizar e Subir a Infraestrutura
```bash
# Baixe as atualizações mais recentes do repositório
git pull origin main

# Entre na pasta de infraestrutura
cd infra

# Suba os containers em segundo plano (aguarde ~2 minutos para inicialização completa)
docker compose up -d
```
> *Nota: O banco de dados estará acessível no `localhost`, porta `5433`. A senha padrão é `postgrespassword`.*

### 2. Preparar o Ambiente Python
Abra um **novo terminal** na raiz do projeto (pasta principal) e instale o driver de conexão com o banco:
```bash
pip install psycopg2-binary
```

### 3. Ativar o Change Data Capture (Debezium)
Na raiz do projeto, envie a configuração para o Debezium começar a monitorar o banco legado e enviar as mudanças para o Kafka:
```bash
python ativar_cdc_debezium.py
```
> *Você deve ver a mensagem: `✅ Conector do Debezium criado com sucesso!`*

### 4. Iniciar a Simulação de Dados
Com o CDC ativo, inicie a injeção contínua de fichas de pacientes no banco legado:
```bash
python simulator/gerador.py
```
> *O terminal começará a registrar as inserções. Deixe esta aba rodando para alimentar a pipeline.*

### 5. Verificar o Streaming no Kafka (Tempo Real)
Abra um **terceiro terminal** e rode o comando abaixo para interceptar e visualizar as mensagens JSON chegando no Kafka no exato momento em que são criadas no banco:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic epidemiologia.public.notificacoes_dengue --from-beginning
```

---

## 🛑 Como desligar o ambiente

Ao terminar de programar, sempre desligue os containers para não consumir a memória RAM do computador:

1. Pressione `Ctrl + C` nos terminais do Python e do Kafka para parar a execução.
2. Desligue os containers do Docker:
```bash
cd infra
docker compose down
```

---

## 🎯 Próximos Passos (Fase de Processamento)

Agora que a extração em tempo real (CDC) está funcionando, os próximos passos da PoC focarão no consumo e transformação:
1. **Configurar o Apache Flink:** Criar o job no Flink para consumir o tópico `epidemiologia.public.notificacoes_dengue` do Kafka.
2. **Transformação de Dados:** Limpar, anonimizar ou enriquecer os dados JSON usando o Flink em tempo real.
3. **Sink (Destino Final):** Enviar os dados processados para um Data Warehouse ou Data Lake para criação dos painéis epidemiológicos.
