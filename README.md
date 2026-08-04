# TCC

Prova de Conceito (PoC) para modernização de sistemas legados de vigilância epidemiológica via arquitetura orientada a eventos em tempo real com Debezium CDC, Apache Kafka e Apache Flink.

# 📊 TCC - Prova de Conceito (PoC)

Modernização de sistemas legados de vigilância epidemiológica via arquitetura orientada a eventos em tempo real com Debezium CDC, Apache Kafka e Apache Flink.

## ✅ Status Atual do Projeto

- **Infraestrutura Base (Docker):** O `docker-compose.yml` está configurado e testado com PostgreSQL, Zookeeper, Kafka, Debezium e Flink.
- **Banco de Dados Legado:** A porta foi mapeada para **`5433`** (evitando conflitos com instalações locais do Windows). O banco `sinan_db` e a tabela `notificacoes_dengue` sobem criados automaticamente, incluindo as _triggers_ necessárias para o CDC.
- **Estratégia de Dados da Dupla:** Os ambientes Docker rodam de forma isolada na máquina de cada um. Para garantir que ambos trabalhem com as mesmas informações, o simulador Python injetará uma carga de dados inicial idêntica (dados "semente") toda vez que o banco for recriado.

---

## 🐳 Como rodar a Infraestrutura (Tutorial)

Para rodar a base de dados e os serviços de streaming na sua máquina, certifique-se de que o **Docker Desktop** está aberto e siga os passos abaixo no terminal do VS Code:

**1. Atualize o repositório**
Garanta que você tem a versão mais recente do código:
`git pull origin main`

**2. Navegue até a infraestrutura**
Entre na pasta onde os arquivos do Docker estão configurados:
`cd infra`

**3. Suba os containers**
Inicie os serviços em segundo plano (na primeira vez, o download das imagens pode demorar alguns minutos):
`docker compose up -d`

**4. Confira os serviços**
Verifique se os 5 containers estão rodando perfeitamente:
`docker ps`
_(Nota: O banco de dados estará acessível no localhost, porta 5433. A senha padrão é postgrespassword)._

**5. Como desligar o ambiente**
Ao terminar de programar no dia, sempre desligue os containers para liberar a memória do computador:
`docker compose down`

---

## 🚀 Próximos Passos (Onde paramos)

1. **Criar o Simulador Python:** Desenvolver o script `gerador.py` na pasta `simulator` para injetar dados fictícios continuamente na tabela `notificacoes_dengue`.
2. **Ativar o Change Data Capture (CDC):** Rodar o script de configuração do Debezium para ele começar a rastrear a tabela e enviar os eventos.
3. **Consumo no Kafka:** Verificar se os tópicos estão sendo criados e se os dados estão chegando no formato JSON correto.
