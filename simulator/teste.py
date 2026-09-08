from datetime import datetime, timedelta
import psycopg2

BANCOS = [
    {
        "cidade": "Ponta Grossa",
        "ibge": "4119905",
        "port": "5433",
        "bairro": "Centro",
    },
    {"cidade": "Curitiba", "ibge": "4106902", "port": "5434", "bairro": "Centro"},
    {"cidade": "Castro", "ibge": "4104907", "port": "5435", "bairro": "Centro"},
]


def inserir_caso(banco_config, doenca="MENINGITE"):
    conn = psycopg2.connect(
        host="localhost",
        port=banco_config["port"],
        database="sinan_db",
        user="postgres",
        password="postgrespassword",
    )
    cursor = conn.cursor()

    agora = datetime.now()
    futuro_curto = agora + timedelta(minutes=6)

    query = """
        INSERT INTO notificacoes 
        (dt_sintomas, municipio_residencia, codigo_ibge, bairro, doenca, cid_10, idade, sexo)
        VALUES (%s, %s, %s, %s, %s, 'G03.9', 25, 'M')
    """

    # Inserção do evento e avanço de Watermark no mesmo bloco
    cursor.execute(
        query,
        (
            futuro_curto,
            banco_config["cidade"],
            banco_config["ibge"],
            banco_config["bairro"],
            doenca,
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"➕ +1 caso de {doenca} inserido em {banco_config['cidade']}")


if __name__ == "__main__":
    # Teste: Insere 1 caso em Ponta Grossa
    inserir_caso(BANCOS[0], "MENINGITE")