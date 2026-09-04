import urllib.request
import urllib.error
import json

URL_CONNECTORS = "http://localhost:8083/connectors"

def criar_conector(nome, configuracao):
    payload = {
        "name": nome,
        "config": configuracao
    }

    data = json.dumps(payload).encode("utf-8")

    requisicao = urllib.request.Request(
        URL_CONNECTORS,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(requisicao) as resposta:
            if resposta.status in [200, 201]:
                print(f"Conector criado com sucesso: {nome}")

    except urllib.error.HTTPError as erro:
        corpo = erro.read().decode("utf-8")

        if erro.code == 409:
            print(f"Conector já existe: {nome}")
        else:
            print(f"Erro HTTP ao criar {nome}: {erro.code}")
            print(corpo)

    except Exception as erro:
        print(f"Erro ao conectar ao Debezium para criar {nome}: {erro}")

def configuracao_base(host, topic_prefix, slot_name, publication_name):
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": host,
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgrespassword",
        "database.dbname": "sinan_db",
        "topic.prefix": topic_prefix,
        "table.include.list": "public.notificacoes",
        "slot.name": slot_name,
        "publication.name": publication_name,
        "publication.autocreate.mode": "filtered",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false"
    }

def iniciar():
    conectores = [
        {
            # Banco legado que representa Curitiba
            "nome": "sinan-postgres-curitiba",
            "cidade": "Curitiba",
            "config": configuracao_base(
                host="postgres-curitiba",
                topic_prefix="epidemiologia_curitiba",
                slot_name="sinan_slot_curitiba",
                publication_name="sinan_publication_curitiba"
            )
        },
        {
            # Banco legado que representa Castro
            "nome": "sinan-postgres-castro",
            "cidade": "Castro",
            "config": configuracao_base(
                host="postgres-castro",
                topic_prefix="epidemiologia_castro",
                slot_name="sinan_slot_castro",
                publication_name="sinan_publication_castro"
            )
        }
    ]

    for conector in conectores:
        print(
            f"Criando conector da cidade {conector['cidade']} "
            f"no banco {conector['config']['database.hostname']}"
        )

        criar_conector(
            nome=conector["nome"],
            configuracao=conector["config"]
        )

if __name__ == "__main__":
    iniciar()