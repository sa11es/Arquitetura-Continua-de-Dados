# -*- coding: utf-8 -*-
"""
CENÁRIO 1 — BASAL ALEATÓRIO
Simula um dia normal: volume basal, sem surto garantido.
Valida a contagem e a transmissão dos dados: os totais inseridos
devem bater com o processado pelo Flink e exibido no dashboard.

Uso: python basal.py
     python basal.py --seed 42            (rodada reproduzível)
     python basal.py --duracao 60         (ciclo comprimido de 60s)
"""
import sys
import time
import random
from datetime import datetime
import psycopg2

# ===================== CONFIG =====================
CICLO_SEGUNDOS   = 300      # 5 min = 24h simuladas (alterável via --duracao)
SEED             = None     # None = aleatório a cada execução

DB_CONFIGS = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},
}
DB_USER, DB_PASS, DB_NAME, DB_HOST = "postgres", "postgrespassword", "sinan_db", "localhost"

# Escala real (média de notificações/dia) dividida por 100:
#   Castro 1.625 -> 16,25 | Ponta Grossa 37.250 -> 372,5 | Curitiba 297.965 -> 2.979,65
TAXAS = {
    "Ponta Grossa": {"total": 372,  "DENGUE": 0.37, "MENINGITE": 0.34, "INFLUENZA": 3.12},
    "Curitiba":     {"total": 2979, "DENGUE": 0.80, "MENINGITE": 0.57, "INFLUENZA": 16.0},
    "Castro":       {"total": 16,   "DENGUE": 0.03, "MENINGITE": 0.005, "INFLUENZA": 0.12},
}

CID10       = {"DENGUE": "A90", "MENINGITE": "A39", "INFLUENZA": "J11"}
CID_POPULAR = ["A09", "J18", "K04"]   # gastroenterite, pneumonia, odontológico

BAIRROS = {
    "Ponta Grossa": ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"],
    "Curitiba":     ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
    "Castro":       ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
}

# ===================== HELPERS =====================
def sortear(qtd_media):
    """Converte média diária fracionária em nº de casos do ciclo."""
    if qtd_media < 1:
        return 1 if random.random() < qtd_media else 0
    n = int(qtd_media)
    return n + (1 if random.random() < (qtd_media - n) else 0)

def montar_agenda():
    agenda = []
    resumo = {}
    for cidade, tx in TAXAS.items():
        n_deng = sortear(tx["DENGUE"])
        n_men  = sortear(tx["MENINGITE"])
        n_inf  = sortear(tx["INFLUENZA"])
        n_pop  = max(0, int(tx["total"]) - n_deng - n_men - n_inf)

        plano = ([("DENGUE", CID10["DENGUE"])] * n_deng +
                 [("MENINGITE", CID10["MENINGITE"])] * n_men +
                 [("INFLUENZA", CID10["INFLUENZA"])] * n_inf +
                 [("POPULAR", random.choice(CID_POPULAR))] * n_pop)

        resumo[cidade] = {"DENGUE": n_deng, "MENINGITE": n_men,
                          "INFLUENZA": n_inf, "POPULAR": n_pop,
                          "TOTAL": len(plano)}

        for doenca, cid in plano:
            agenda.append((random.uniform(0, CICLO_SEGUNDOS), cidade, doenca, cid,
                           random.choice(BAIRROS[cidade])))

    agenda.sort(key=lambda e: e[0])
    return agenda, resumo

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
    global CICLO_SEGUNDOS
    # --duracao N: comprime o ciclo em N segundos (padrão: 300)
    if "--duracao" in sys.argv:
        try:
            CICLO_SEGUNDOS = int(sys.argv[sys.argv.index("--duracao") + 1])
        except (IndexError, ValueError):
            print("Uso: python basal.py --duracao 60")
            sys.exit(1)

    # --seed N: rodada reproduzível
    if "--seed" in sys.argv:
        try:
            random.seed(int(sys.argv[sys.argv.index("--seed") + 1]))
        except (IndexError, ValueError):
            print("Uso: python basal.py --seed 42")
            sys.exit(1)

    agenda, resumo = montar_agenda()
    total_pop = sum(1 for e in agenda if e[2] == "POPULAR")
    print(f"🚀 CENÁRIO BASAL ALEATÓRIO | Ciclo de {CICLO_SEGUNDOS}s = 24h simuladas | "
          f"{len(agenda)} notificações ({total_pop} de preenchimento)\n")
    for cidade, r in resumo.items():
        print(f"   📍 {cidade:<13} total={r['TOTAL']:<5} dengue={r['DENGUE']} "
              f"meningite={r['MENINGITE']} influenza={r['INFLUENZA']} "
              f"preenchimento={r['POPULAR']}")
    print("\nSem surto — validação de contagem e transmissão ponta a ponta.\n")

    conns = abrir_conexoes()
    t0 = time.time()
    ok, por_cidade = 0, {}
    try:
        for t, cidade, doenca, cid, bairro in agenda:
            atraso = t0 + t - time.time()
            if atraso > 0:
                time.sleep(atraso)
            if inserir(conns[cidade], cidade, doenca, cid, bairro, datetime.now()):
                ok += 1
                por_cidade[cidade] = por_cidade.get(cidade, 0) + 1
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo usuário.")
    finally:
        for c in conns.values():
            c.close()

    print(f"\n✅ Fim do ciclo: {ok}/{len(agenda)} inseridos")
    for cidade in TAXAS:
        planejado = resumo[cidade]["TOTAL"]
        inseridos = por_cidade.get(cidade, 0)
        status = "✅" if inseridos == planejado else "⚠️"
        print(f"   {status} {cidade}: {inseridos}/{planejado}")

if __name__ == "__main__":
    main()