# -*- coding: utf-8 -*-
"""
Inserção manual de teste — volume por doença:
  - Curitiba:      50 casos de CADA doença monitorada (150 no total)
  - Ponta Grossa:  30 casos de CADA doença monitorada (90 no total)
  - Castro:        10 casos de CADA doença monitorada (30 no total)
  - POPULAR não entra (só doenças monitoradas).

Os casos de cada doença se espalham entre os bairros da cidade
(distribuição round-robin), exercitando a agregação por bairro.

Uso:
  python teste_volume.py              # todas as cidades
  python teste_volume.py curitiba     # só Curitiba
  python teste_volume.py pg           # só Ponta Grossa
  python teste_volume.py castro       # só Castro
"""
import sys
import random
from datetime import datetime
import psycopg2

DB_HOST = "localhost"
DB_USER, DB_PASS, DB_NAME = "postgres", "postgrespassword", "sinan_db"

CIDADES = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905", "n": 30},
    "Curitiba":     {"port": 5434, "ibge": "4106902", "n": 50},
    "Castro":       {"port": 5435, "ibge": "4104153", "n": 10},
}

DOENCAS = [("DENGUE", "A90"), ("MENINGITE", "A39"), ("INFLUENZA", "J11")]

BAIRROS = {
    "Ponta Grossa": ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"],
    "Curitiba":     ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
    "Castro":       ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
}

SQL = """INSERT INTO public.notificacoes
 (municipio_residencia, codigo_ibge, bairro, doenca, cid_10,
  dt_notificacao, dt_sintomas, idade, sexo, status_caso)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SUSPEITO');"""

# Aliases aceitos na linha de comando
FILTROS = {"curitiba": ["Curitiba"], "pg": ["Ponta Grossa"],
           "ponta": ["Ponta Grossa"], "castro": ["Castro"]}


def main():
    filtro = sys.argv[1].lower() if len(sys.argv) > 1 else "tudo"
    cidades_alvo = FILTROS.get(filtro, list(CIDADES.keys())) if filtro != "tudo" else list(CIDADES.keys())

    if filtro != "tudo" and filtro not in FILTROS:
        print(f"Filtro '{filtro}' nao reconhecido. Opções: tudo | curitiba | pg | castro")
        sys.exit(1)

    agora = datetime.now()
    conns = {}
    total_geral = 0

    try:
        for cidade in cidades_alvo:
            cfg = CIDADES[cidade]
            n = cfg["n"]
            bairros = BAIRROS[cidade]
            conns[cidade] = psycopg2.connect(host=DB_HOST, port=cfg["port"],
                                             user=DB_USER, password=DB_PASS,
                                             dbname=DB_NAME)

            print(f"\n📍 {cidade} — {n} casos de cada doença "
                  f"({n * len(DOENCAS)} no total):")
            for doenca, cid in DOENCAS:
                with conns[cidade].cursor() as cur:
                    for i in range(n):
                        bairro = bairros[i % len(bairros)]  # round-robin
                        cur.execute(SQL, (cidade, cfg["ibge"], bairro, doenca, cid,
                                          agora, agora.date(),
                                          random.randint(1, 90),
                                          random.choice("MFI")))
                conns[cidade].commit()
                total_geral += n
                print(f"   ✅ {doenca:<10} {n} casos ({bairros[0]}...{bairros[-1]}: round-robin)")

    except Exception as e:
        for c in conns.values():
            c.rollback()
        print(f"❌ Erro: {e}")
    finally:
        for c in conns.values():
            c.close()

    print(f"\n✅ Inserção concluída: {total_geral} notificações no total.")
    print("Esperado no dashboard: os totais por doença devem refletir")
    print("exatamente os valores acima por cidade (após o pipeline processar).")


if __name__ == "__main__":
    main()