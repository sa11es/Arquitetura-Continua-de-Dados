from datetime import date
import time
import psycopg2

DB_USER = "postgres"
DB_PASS = "postgrespassword"
DB_NAME = "sinan_db"

# Conexão direta com Curitiba (Porta 5434)
HOST = "localhost"
PORT = "5434"
CIDADE = "Curitiba"
CODIGO_IBGE = "4106902"
BAIRRO = "Água Verde"


def inserir_casos(conn, quantidade, doenca="DENGUE", cid_10="A90"):
    cursor = conn.cursor()
    query = """
        INSERT INTO notificacoes 
        (dt_sintomas, municipio_residencia, codigo_ibge, bairro, doenca, cid_10, idade, sexo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    hoje = date.today()
    for _ in range(quantidade):
        cursor.execute(
            query, (hoje, CIDADE, CODIGO_IBGE, BAIRRO, doenca, cid_10, 30, "M")
        )
    conn.commit()
    cursor.close()
    print(
        f"✅ Inseridos {quantidade} casos de {doenca} em {BAIRRO} ({CIDADE})"
    )


def main():
    conn = psycopg2.connect(
        host=HOST, port=PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

    print("\n--- TESTE DETERMINÍSTICO DE GRAVIDADE (DENGUE) ---")

    # Passagem 1: 4 casos (Abaixo do limiar de 5)
    print("\n[1/4] Inserindo 4 casos (Esperado: NENHUM ALERTA)...")
    inserir_casos(conn, 4)
    time.sleep(5)

    # Passagem 2: +1 caso (Total 5 - MODERADA)
    print("\n[2/4] Inserindo +1 caso (Total 5 - Esperado: Alerta MODERADA)...")
    inserir_casos(conn, 1)
    time.sleep(5)

    # Passagem 3: +3 casos (Total 8 - ALTA)
    print("\n[3/4] Inserindo +3 casos (Total 8 - Esperado: Alerta ALTA)...")
    inserir_casos(conn, 3)
    time.sleep(5)

    # Passagem 4: +5 casos (Total 13 - CRÍTICA)
    print(
        "\n[4/4] Inserindo +5 casos (Total 13 - Esperado: Alerta CRÍTICA)..."
    )
    inserir_casos(conn, 5)

    conn.close()
    print("\n--- TESTE CONCLUÍDO ---")


if __name__ == "__main__":
    main()