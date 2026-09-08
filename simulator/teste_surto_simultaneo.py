from datetime import datetime, timedelta
import psycopg2

DB_USER = "postgres"
DB_PASS = "postgrespassword"
DB_NAME = "sinan_db"

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


def disparar_surto(banco_config, quantidade_casos=3):
    conn = psycopg2.connect(
        host="localhost",
        port=banco_config["port"],
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO notificacoes 
        (dt_sintomas, municipio_residencia, codigo_ibge, bairro, doenca, cid_10, idade, sexo)
        VALUES (%s, %s, %s, %s, 'MENINGITE', 'G03.9', 20, 'F')
    """

    # Usamos datetime.now() para incluir hora/minuto/segundo atuais
    agora = datetime.now()

    # 1. Inserção dos casos do surto
    for _ in range(quantidade_casos):
        cursor.execute(
            query,
            (
                agora,
                banco_config["cidade"],
                banco_config["ibge"],
                banco_config["bairro"],
            ),
        )

    # 2. Inserção de 1 caso futuro (+1 minuto) para empurrar a Watermark do Flink
    futuro = agora + timedelta(minutes=1)
    cursor.execute(
        query,
        (
            futuro,
            banco_config["cidade"],
            banco_config["ibge"],
            banco_config["bairro"],
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(
        f"🚨 Surto gerado em {banco_config['cidade']} ({quantidade_casos} casos + 1 avanço de Watermark)"
    )


def main():
    print("\n--- SIMULANDO SURTO MULTIMUNICIPAL SIMULTÂNEO ---")
    for banco in BANCOS:
        disparar_surto(banco, quantidade_casos=3)

    print(
        "\n✅ Inserções concluídas nos 3 bancos. Os alertas de Meningite devem aparecer no Dashboard!"
    )


if __name__ == "__main__":
    main()