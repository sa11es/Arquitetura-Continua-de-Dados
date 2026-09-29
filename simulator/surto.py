# -*- coding: utf-8 -*-
"""
CENÁRIO 2 — SURTO (ESCALA AMPLIADA)
Simula um dia com volume ampliado (10x o basal): Castro 162,
Ponta Grossa 3725, Curitiba 29796.
As doenças monitoradas são inseridas misturadas às demais, distribuídas
uniformemente pelo ciclo — sem janela de pico específica.
Objetivo: medir latência e throughput do pipeline sob carga alta,
sem preparo prévio do sistema.

Uso: python surto.py
     python surto.py --seed 42       (rodada reproduzível)
     python surto.py --duracao 600   (ciclo de 10 min, se o Debezium atrasar)
"""
import sys
import time
import random
from datetime import datetime
import psycopg2

# ===================== CONFIG =====================
CICLO_SEGUNDOS = 300      # 5 min = 24h simuladas (alterável via --duracao)
SEED           = None     # None = aleatório a cada execução

DB_CONFIGS = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},
}
DB_USER, DB_PASS, DB_NAME, DB_HOST = "postgres", "postgrespassword", "sinan_db", "localhost"

# Escala 10x o basal:
#   Castro 16.250 -> 162 | Ponta Grossa 372.500 -> 3.725 | Curitiba 297.9650 -> 29.796
TAXAS = {
    "Ponta Grossa": {"total": 3725,  "DENGUE": 3.70, "MENINGITE": 3.40, "INFLUENZA": 31.2},
    "Curitiba":     {"total": 29796, "DENGUE": 8.00, "MENINGITE": 5.70, "INFLUENZA": 160.0},
    "Castro":       {"total": 162,   "DENGUE": 0.30, "MENINGITE": 0.05, "INFLUENZA": 1.20},
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

        # Distribuição uniforme por todo o ciclo (até 95% do tempo)
        for doenca, cid in plano:
            agenda.append((random.uniform(0, CICLO_SEGUNDOS * 0.95),
                           cidade, doenca, cid,
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
    # --duracao N
    if "--duracao" in sys.argv:
        try:
            global CICLO_SEGUNDOS
            CICLO_SEGUNDOS = int(sys.argv[sys.argv.index("--duracao") + 1])
        except (IndexError, ValueError):
            print("Uso: python surto.py --duracao 600")
            sys.exit(1)

    # --seed N
    if "--seed" in sys.argv:
        try:
            random.seed(int(sys.argv[sys.argv.index("--seed") + 1]))
        except (IndexError, ValueError):
            print("Uso: python surto.py --seed 42")
            sys.exit(1)

    agenda, resumo = montar_agenda()
    total_pop = sum(1 for e in agenda if e[2] == "POPULAR")

    total_monitoradas = sum(
        r["DENGUE"] + r["MENINGITE"] + r["INFLUENZA"] for r in resumo.values()
    )
    print(f"🚀 CENÁRIO SURTO (ESCALA AMPLIADA) | Ciclo de {CICLO_SEGUNDOS}s = 24h simuladas\n")
    print(f"   Notificações: {len(agenda)} totais | {total_monitoradas} monitoradas | "
          f"{total_pop} de preenchimento\n")
    for cidade, r in resumo.items():
        print(f"   📍 {cidade:<13} total={r['TOTAL']:<6} dengue={r['DENGUE']} "
              f"meningite={r['MENINGITE']} influenza={r['INFLUENZA']} "
              f"preenchimento={r['POPULAR']}")
    print("\nSem pico concentrado — carga uniforme para medição de latência/throughput.\n")

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

        # Métricas próprias do produtor (referência para o TCC)
        duracao = time.time() - t0
        print(f"\n⏱  Duração real da inserção: {duracao:.1f}s | "
              f"Throughput produtor: {ok / duracao:.1f} casos/s")
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