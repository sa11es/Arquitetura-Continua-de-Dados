import psycopg2
import time
import random
from datetime import date, timedelta

DB_USER = "postgres"
DB_PASS = "postgrespassword"
DB_NAME = "sinan_db"

BANCOS = [
    {
        "cidade": "Ponta Grossa",
        "codigo_ibge": "4119905",
        "host": "localhost",
        "port": "5433",
        "bairro": [
            "Centro",
            "Estrela",
            "Oficinas",
            "Uvaranas",
            "Nova Rússia",
            "Contorno",
            "Jardim Carvalho",
            "Órfãs",
            "Neves",
            "Boa Vista",
            "Chapada",
            "Ronda"
        ],
        "topic_prefix": "epidemiologia_ponta_grossa"
    },
    {
        "cidade": "Curitiba",
        "codigo_ibge": "4106902",
        "host": "localhost",
        "port": "5434",
        "bairro": [
            "Centro",
            "Água Verde",
            "Batel",
            "Champagnat",
            "Santa Felicidade",
            "Cabral",
            "Portão",
            "Cajuru",
            "Boqueirão",
            "Tatuquara",
            "Pinheirinho",
            "Sí­tio Cercado"
        ],
        "topic_prefix": "epidemiologia_curitiba"
    },
    {
        "cidade": "Castro",
        "codigo_ibge": "4104907",
        "host": "localhost",
        "port": "5435",
        "bairro": [
            "Centro",
            "Invernada",
            "Cantagalo",
            "Jardim das Araucárias",
            "Vila Rio Branco",
            "Morada do Sol",
            "Jardim Social",
            "Lacustre",
            "Termas de Riviera",
            "Castrolanda",
            "Abapan",
            "Socavão"
        ],
        "topic_prefix": "epidemiologia_castro"
    }
]

DOENCAS = {
    "DENGUE": "A90",
    "MENINGITE": "G03.9",
    "INFLUENZA": "J11"
}

def gerar_dados_aleatorios(banco):
    dt_sintomas = date.today() - timedelta(days=random.randint(1, 15))
    bairro = random.choice(banco["bairro"])
    doenca = random.choice(list(DOENCAS.keys()))
    cid_10 = DOENCAS[doenca]
    idade = random.randint(5, 80)
    sexo = random.choice(["M", "F", "I"])

    return (
        dt_sintomas,
        banco["cidade"],
        banco["codigo_ibge"],
        bairro,
        doenca,
        cid_10,
        idade,
        sexo
    )

def inserir_notificacao(banco, dados):
    conexao = None
    cursor = None

    try:
        conexao = psycopg2.connect(
            host=banco["host"],
            port=banco["port"],
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        cursor = conexao.cursor()

        query = """
            INSERT INTO notificacoes
            (
                dt_sintomas,
                municipio_residencia,
                codigo_ibge,
                bairro,
                doenca,
                cid_10,
                idade,
                sexo
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query, dados)
        conexao.commit()

        print(
            f"Notificação inserida | "
            f"Origem: {banco['cidade']} | "
            f"Bairro: {dados[3]} | "
            f"Doença: {dados[4]} | "
            f"CID: {dados[5]} | "
            f"Idade: {dados[6]} | "
            f"Sexo: {dados[7]} | "
            f"Tópico esperado: {banco['topic_prefix']}"
        )

    except Exception as erro:
        print(
            f"Erro ao inserir no banco de {banco['cidade']}: {erro}"
        )

    finally:
        if cursor is not None:
            cursor.close()

        if conexao is not None:
            conexao.close()

def iniciar_simulacao():
    print("Iniciando simulador dos bancos de Ponta Grossa, Curitiba e Castro.")
    print("Pressione CTRL+C para interromper.\n")

    try:
        while True:
            banco = random.choice(BANCOS)
            dados = gerar_dados_aleatorios(banco)
            inserir_notificacao(banco, dados)
            time.sleep(3)

    except KeyboardInterrupt:
        print("\nSimulação encerrada pelo usuário.")

if __name__ == "__main__":
    iniciar_simulacao()