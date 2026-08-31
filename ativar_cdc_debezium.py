import urllib.request
import json

# URL da API do Debezium rodando no Docker
url = 'http://localhost:8083/connectors'

# Configuração que diz ao Debezium o que ele deve vigiar
payload = {
    "name": "sinan-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres-legado",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgrespassword",
        "database.dbname": "sinan_db",
        "topic.prefix": "epidemiologia",
        "table.include.list": "public.notificacoes_dengue",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false"
    }
}

data = json.dumps(payload).encode('utf-8')
req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})

try:
    with urllib.request.urlopen(req) as response:
        if response.status in [200, 201]:
            print("✅ Conector do Debezium criado com sucesso!")
            print("O Debezium agora está vigiando a tabela e enviando os dados para o Kafka.")
except urllib.error.HTTPError as e:
    if e.code == 409:
        print("ℹ️ O conector já existe no Debezium.")
    else:
        print(f"❌ Erro HTTP: {e.code} - {e.read().decode('utf-8')}")
except Exception as e:
    print(f"❌ Erro ao conectar: {e}")