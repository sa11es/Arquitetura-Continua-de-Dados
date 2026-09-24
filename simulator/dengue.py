# -*- coding: utf-8 -*-
"""
Inserção manual de teste (regras de dengue — cidade 6, bairro 3):
  - 5 casos de DENGUE em Ponta Grossa, cada um em um bairro DIFERENTE
    (cidade 5 < 6 e bairro 1 < 3  ->  SEM alerta)
  - 2 casos de DENGUE em Curitiba, no MESMO bairro
    (cidade 2 < 6 e bairro 2 < 3  ->  SEM alerta)
  - 6 casos de DENGUE em Castro, TODOS no mesmo bairro
    (cidade 6 >= 6  ->  MODERADA por CIDADE
     bairro 6 >= 3  ->  MODERADA por BAIRRO)

Uso:
  python teste_dengue_fronteiras.py            # todos os tres blocos
  python teste_dengue_fronteiras.py so5        # só os 5 de Ponta Grossa
  python teste_dengue_fronteiras.py so2        # só os 2 de Curitiba
  python teste_dengue_fronteiras.py castro     # só os 6 de Castro
"""
import sys
import random
from datetime import datetime
import psycopg2

DB_HOST = "localhost"
DB_USER, DB_PASS, DB_NAME = "postgres", "postgrespassword", "sinan_db"

CIDADES = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},
}

BAIRROS = {
    "Ponta Grossa": ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"],
    "Curitiba":     ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
    "Castro":       ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
}

SQL = """INSERT INTO public.notificacoes
 (municipio_residencia, codigo_ibge, bairro, doenca, cid_10,
  dt_notificacao, dt_sintomas, idade, sexo, status_caso)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SUSPEITO');"""

# Ponta Grossa: 5 casos, um em cada bairro (a lista tem exatamente 5)
PLANO_PG = [("Ponta Grossa", b, 1) for b in BAIRROS["Ponta Grossa"]]

# Curitiba: 2 casos no mesmo bairro (o primeiro da lista)
BAIRRO_ANCORA_CURITIBA = BAIRROS["Curitiba"][0]
PLANO_CURITIBA = [("Curitiba", BAIRRO_ANCORA_CURITIBA, 2)]

# Castro: 6 casos, todos no mesmo bairro (o primeiro da lista)
BAIRRO_ANCORA_CASTRO = BAIRROS["Castro"][0]
PLANO_CASTRO = [("Castro", BAIRRO_ANCORA_CASTRO, 6)]


def inserir_lote(conn, cidade, bairro, n, quando):
    cfg = CIDADES[cidade]
    with conn.cursor() as cur:
        for i in range(1, n + 1):
            cur.execute(SQL, (cidade, cfg["ibge"], bairro, "DENGUE", "A90",
                              quando, quando.date(),
                              random.randint(1, 90),
                              random.choice("MFI")))
            print(f"DENGUE -> {cidade} (bairro: {bairro}) [{i}/{n}]")
    conn.commit()


def main():
    filtro = sys.argv[1].lower() if len(sys.argv) > 1 else "tudo"

    lotes = []
    if filtro in ("tudo", "so5"):
        lotes += PLANO_PG
    if filtro in ("tudo", "so2"):
        lotes += PLANO_CURITIBA
    if filtro in ("tudo", "castro"):
        lotes += PLANO_CASTRO

    if not lotes:
        print(f"Filtro '{filtro}' nao reconhecido. Opções: tudo | so5 | so2 | castro")
        sys.exit(1)

    agora = datetime.now()
    conns = {c: psycopg2.connect(host=DB_HOST, port=cfg["port"],
                                 user=DB_USER, password=DB_PASS,
                                 dbname=DB_NAME)
             for c, cfg in CIDADES.items()}
    try:
        for cidade, bairro, n in lotes:
            inserir_lote(conns[cidade], cidade, bairro, n, agora)
    except Exception as e:
        for c in conns.values():
            c.rollback()
        print(f"❌ Erro: {e}")
    finally:
        for c in conns.values():
            c.close()

    print("\n✅ Inserção concluída.")
    print("Esperado no dashboard (dengue: cidade 6, bairro 3):")
    if filtro == "so5":
        print("  NADA deve alertar: 5 na cidade (< 6), 1 por bairro (< 3).")
    elif filtro == "so2":
        print("  NADA deve alertar: 2 na cidade (< 6), 2 no bairro (< 3).")
    elif filtro == "castro":
        print(f"  Castro MODERADA por cidade (6 >= 6) e MODERADA por bairro "
              f"({BAIRRO_ANCORA_CASTRO}: 6 >= 3).")
    else:
        print("  Ponta Grossa: SEM alerta (5 < 6 na cidade; 1 por bairro < 3).")
        print(f"  Curitiba: SEM alerta (2 < 6 na cidade; {BAIRRO_ANCORA_CURITIBA}: 2 < 3).")
        print(f"  Castro: MODERADA por cidade (6 >= 6) e por bairro "
              f"({BAIRRO_ANCORA_CASTRO}: 6 >= 3).")


if __name__ == "__main__":
    main()