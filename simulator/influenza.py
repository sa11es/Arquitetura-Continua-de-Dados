# -*- coding: utf-8 -*-
"""
Inserção manual de teste:
  - 2 casos de MENINGITE em Castro       (abaixo do limiar: 3 -> SEM alerta)
  - 30 casos de INFLUENZA em Curitiba    (exatamente no limiar: 30 -> MODERADA)
Uso: python teste_misturado.py
"""
import random
from datetime import datetime
import psycopg2

DB_HOST = "localhost"
DB_USER, DB_PASS, DB_NAME = "postgres", "postgrespassword", "sinan_db"

COSIAS = {
    "Castro":       {"port": 5435, "ibge": "4104153"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
}

BAIRROS = {
    "Castro":   ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
    "Curitiba": ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
}

SQL = """INSERT INTO public.notificacoes
 (municipio_residencia, codigo_ibge, bairro, doenca, cid_10,
  dt_notificacao, dt_sintomas, idade, sexo, status_caso)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SUSPEITO');"""

PLANO = [
    ("Castro",   "MENINGITE", "A39", 2),
    ("Curitiba", "INFLUENZA", "J11", 30),
]


def main():
    agora = datetime.now()
    conns = {c: psycopg2.connect(host=DB_HOST, port=cfg["port"],
                                 user=DB_USER, password=DB_PASS,
                                 dbname=DB_NAME)
             for c, cfg in COSIAS.items()}
    try:
        for cidade, doenca, cid, n in PLANO:
            cfg = COSIAS[cidade]
            with conns[cidade].cursor() as cur:
                for i in range(1, n + 1):
                    bairro = random.choice(BAIRROS[cidade])
                    cur.execute(SQL, (cidade, cfg["ibge"], bairro, doenca, cid,
                                      agora, agora.date(),
                                      random.randint(1, 90),
                                      random.choice("MFI")))
                    print(f"[{i}/{n}] {doenca} -> {cidade} (bairro: {bairro})")
            conns[cidade].commit()
        print("\n✅ Inserido: 2 MENINGITE em Castro + 30 INFLUENZA em Curitiba.")
        print("Esperado no dashboard: SEM alerta de meningite (2 < 3) | "
              "MODERADA de influenza em Curitiba (30 >= 30).")
    except Exception as e:
        for c in conns.values():
            c.rollback()
        print(f"❌ Erro: {e}")
    finally:
        for c in conns.values():
            c.close()


if __name__ == "__main__":
    main()