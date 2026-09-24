# -*- coding: utf-8 -*-
"""
Inserção manual de teste: 10 casos de MENINGITE em Ponta Grossa.
Com a regra vigente (limiar cidade: 3), 10 casos = CRITICO.
Uso: python teste_meningite_pg.py
"""
import random
from datetime import datetime
import psycopg2

DB_HOST, DB_PORT = "localhost", 5433          # postgres-ponta-grossa
DB_USER, DB_PASS, DB_NAME = "postgres", "postgrespassword", "sinan_db"
IBGE_PG = "4119905"

BAIRROS = ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"]

SQL = """INSERT INTO public.notificacoes
 (municipio_residencia, codigo_ibge, bairro, doenca, cid_10,
  dt_notificacao, dt_sintomas, idade, sexo, status_caso)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SUSPEITO');"""


def main():
    agora = datetime.now()
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT,
                            user=DB_USER, password=DB_PASS, dbname=DB_NAME)
    try:
        with conn.cursor() as cur:
            for i in range(1, 11):
                bairro = random.choice(BAIRROS)
                cur.execute(SQL, ("Ponta Grossa", IBGE_PG, bairro,
                                  "MENINGITE", "A39",
                                  agora, agora.date(),
                                  random.randint(1, 90),
                                  random.choice("MFI")))
                print(f"[{i}/10] MENINGITE -> Ponta Grossa (bairro: {bairro})")
        conn.commit()
        print("\n✅ 10 casos de MENINGITE inseridos em Ponta Grossa.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()