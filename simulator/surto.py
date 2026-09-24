# -*- coding: utf-8 -*-
"""
CENÁRIO 2 — SURTO ALEATÓRIO
Volume 10x o basal (~33.000 notificações no ciclo), SEM surto garantido.
Os alertas ocorrem organicamente: o volume cruza os limiares das regras.
Usado para testes de latência e throughput com carga alta.

Uso: python surto.py
     python surto.py --seed 42   (rodada reproduzível)

Observação: se o Debezium não acompanhar a rajada, aumente CICLO_SEGUNDOS
para 600 (mesma taxa de pico, mais tempo total).
"""
import sys
import time
import random
from datetime import datetime
import psycopg2

# ===================== CONFIG =====================
CICLO_SEGUNDOS   = 300      # 5 min = 24h simuladas
FIM_BASE         = 240      # casos "do dia" distribuídos até aqui
FATOR            = 10.0     # 10x o volume basal
SEED             = None     # None = aleatório a cada execução

DB_CONFIGS = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},
}
DB_USER, DB_PASS, DB_NAME, DB_HOST = "postgres", "postgrespassword", "sinan_db", "localhost"

# Escala basal (÷100 da real). Com FATOR 10x: Castro 162 | PG 3.720 | Curitiba 29.790
TAXAS = {
    "Ponta Grossa": {"total": 372,   "DENGUE": 0.37, "MENINGITE": 0.34, "INFLUENZA": 3.12},
    "Curitiba":     {"total": 2979,  "DENGUE": 0.80, "MENINGITE": 0.57, "INFLUENZA": 16.0},
    "Castro":       {"total": 16.25, "DENGUE": 0.03, "MENINGITE": 0.005, "INFLUENZA": 0.12},
}

CID10       = {"DENGUE": "A90", "MENINGITE": "A39", "INFLUENZA": "J11"}
CID_POPULAR = ["A09", "J18", "K04"]

BAIRROS = {
    "Ponta Grossa": ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"],
    "Curitiba":     ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
    "Castro":       ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
}

# ===================== HELPERS =====================
def sortear(qtd_media):
    """Converte média fracionária em nº de casos do ciclo."""
    if qtd_media < 1:
        return 1 if random.random() < qtd_media else 0
    n = int(qtd_media)
    return n + (1 if random.random() < (qtd_media - n) else 0)

def montar_agenda():
    agenda = []
    for cidade, tx in TAXAS.items():
        n_deng = sortear(tx["DENGUE"] * FATOR)
        n_men  = sortear(tx["MENINGITE"] * FATOR)
        n_inf  = sortear(tx["INFLUENZA"] * FATOR)
        n_pop  = max(0, int(tx["total"] * FATOR) - n_deng - n_men - n_inf)

        plano = ([("DENGUE", CID10["DENGUE"])] * n_deng +
                 [("MENINGITE", CID10["MENINGITE"])] * n_men +
                 [("INFLUENZA", CID10["INFLUENZA"])] * n_inf +
                 [("POPULAR", random.choice(CID_POPULAR))] * n_pop)

        for doenca, cid in plano:
            agenda.append((random.uniform(0, FIM_BASE), cidade, doenca, cid,
                           random.choice(BAIRROS[cidade])))

    agenda.sort(key=lambda e: e[0])
    return agenda

# ===================== BANCO =====================
SQL = """INSERT INTO public.notificacoes
 (municipio_residencia, codigo_ibge, bairro, doenca, cid_10,
  dt_notificacao, dt_sintomas, idade, sexo, status_caso)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'SUSPEITO');"""

def abrir_conexoes():
    return {c: psycopg2.connect(host=DB_HOST, port=cfg["port"], user=DB_USER,
                                password=DB_PASS, dbname=DB_NAME)
            for c, cfg in DB_CONFIGS.items()}

def inserir(conn, cidade, doenca, cid, bairro, quando):
    cfg = DB_CONFIGS[cidade]
    try:
        with conn.cursor() as cur:
            cur.execute(SQL, (cidade, cfg["ibge"], bairro, doenca, cid,
                              quando, quando.date(), random.randint(1, 90),
                              random.choice("MFI")))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ {cidade}: {e}")
        return False

# ===================== MAIN =====================
def main():
    if "--seed" in sys.argv:
        try:
            random.seed(int(sys.argv[sys.argv.index("--seed") + 1]))
        except (IndexError, ValueError):
            print("Uso: python surto.py --seed 42")
            sys.exit(1)

    agenda = montar_agenda()
    total_pop = sum(1 for e in agenda if e[2] == "POPULAR")
    print(f"🚀 CENÁRIO SURTO ALEATÓRIO | Volume: {FATOR}x basal | "
          f"Ciclo de {CICLO_SEGUNDOS}s = 24h simuladas | "
          f"{len(agenda)} notificações ({total_pop} de preenchimento)")
    print("Sem surto garantido — alertas surgem organicamente pelo volume.\n")

    conns = abrir_conexoes()
    t0 = time.time()
    ok = 0
    try:
        for t, cidade, doenca, cid, bairro in agenda:
            atraso = t0 + t - time.time()
            if atraso > 0:
                time.sleep(atraso)
            if inserir(conns[cidade], cidade, doenca, cid, bairro, datetime.now()):
                ok += 1
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo usuário.")
    finally:
        for c in conns.values():
            c.close()
    print(f"\n✅ Fim do ciclo: {ok}/{len(agenda)} inseridos")

if __name__ == "__main__":
    main()