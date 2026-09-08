from datetime import date
import time
import psycopg2

DB_USER = "postgres"
DB_PASS = "postgrespassword"
DB_NAME = "sinan_db"

# Conexão direta com Ponta Grossa (Porta 5433)
HOST = "localhost"
PORT = "5433"
CIDADE = "Ponta Grossa"
CODIGO_IBGE = "4119905"
BAIRRO = "Oficinas"


def inserir_lote_influenza(conn, quantidade, data_sintomas):
    cursor = conn.cursor()
    query = """
        INSERT INTO notificacoes 
        (dt_sintomas, municipio_residencia, codigo_ibge, bairro, doenca, cid_10, idade, sexo)
        VALUES (%s, %s, %s, %s, 'INFLUENZA', 'J11', 45, 'F')
    """
    for _ in range(quantidade):
        cursor.execute(
            query,
            (
                data_sintomas,
                CIDADE,
                CODIGO_IBGE,
                BAIRRO,
            ),
        )
    conn.commit()
    cursor.close()
    print(
        f"✅ Inseridos {quantidade} casos de INFLUENZA com data {data_sintomas}"
    )


def main():
    conn = psycopg2.connect(
        host=HOST, port=PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

    print("\n--- TESTE DE REGRAS SAZONAIS (INFLUENZA) ---")

    # Teste 1: Data em Maio (Alta temporada -> Limiar 200)
    data_inverno = date(date.today().year, 5, 15)
    print(
        f"\n[1/2] Inserindo 130 casos em {data_inverno} (Maio/Alta Temporada)..."
    )
    print(
        "Como o limiar de inverno é 200, 130 casos NÃO devem disparar alerta."
    )
    inserir_lote_influenza(conn, 130, data_inverno)
    time.sleep(5)

    # Teste 2: Data em Outubro (Baixa temporada -> Limiar 120)
    data_primavera = date(date.today().year, 10, 15)
    print(
        f"\n[2/2] Inserindo 130 casos em {data_primavera} (Outubro/Baixa Temporada)..."
    )
    print(
        "Como o limiar regular é 120, 130 casos DEVEM disparar um alerta MODERADA."
    )
    inserir_lote_influenza(conn, 130, data_primavera)

    conn.close()
    print("\n--- TESTE CONCLUÍDO ---")


if __name__ == "__main__":
    main()