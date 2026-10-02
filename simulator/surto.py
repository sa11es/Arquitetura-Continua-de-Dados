# -*- coding: utf-8 -*-
"""
CENÁRIO 2 — SURTO (ESCALA AMPLIADA)
Simula um dia com volume ampliado (10x o basal): Castro 162,
Ponta Grossa 3725, Curitiba 29796.
Todos os 33.683 eventos são inseridos DENTRO do ciclo de 300s, com
inserção em lote (executemany) e pacing com recuperação de atraso.

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
FIM_AGENDA     = 0.99   # eventos planejados até 90% do ciclo (270s) —
                         # sobra 30s de folga para o produtor fechar dentro do prazo
TAMANHO_LOTE   = 200      # eventos por commit (lote)
SEED           = None     # None = aleatório a cada execução

DB_CONFIGS = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},
}
DB_USER, DB_PASS, DB_NAME, DB_HOST = "postgres", "postgrespassword", "sinan_db", "localhost"

# Escala 10x o basal:
#   Castro 16.25 -> 162 | Ponta Grossa 372.5 -> 3.725 | Curitiba 2.979,6 -> 29.796
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

        for doenca, cid in plano:
            agenda.append((random.uniform(0, CICLO_SEGUNDOS * FIM_AGENDA),
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

def inserir_lote(conns, lote):
    """Insere o lote inteiro com executemany (1 commit por cidade).
    Retorna o nº de eventos inseridos."""
    por_cidade = {}
    for t, cidade, doenca, cid, bairro in lote:
        por_cidade.setdefault(cidade, []).append(
            (cidade, DB_CONFIGS[cidade]["ibge"], bairro, doenca, cid,
             datetime.now(), datetime.now().date(),
             random.randint(1, 90), random.choice("MFI"))
        )

    inseridos = 0
    for cidade, linhas in por_cidade.items():
        try:
            with conns[cidade].cursor() as cur:
                cur.executemany(SQL, linhas)
            conns[cidade].commit()
            inseridos += len(linhas)
        except Exception as e:
            conns[cidade].rollback()
            print(f"❌ {cidade}: {e}")
    return inseridos

# ===================== MAIN =====================
def main():
    # --duracao N
    global CICLO_SEGUNDOS
    if "--duracao" in sys.argv:
        try:
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
          f"{total_pop} de preenchimento")
    print(f"   Agenda compactada até {int(CICLO_SEGUNDOS * FIM_AGENDA)}s | "
          f"lotes de {TAMANHO_LOTE} (1 commit por cidade)\n")
    for cidade, r in resumo.items():
        print(f"   📍 {cidade:<13} total={r['TOTAL']:<6} dengue={r['DENGUE']} "
              f"meningite={r['MENINGITE']} influenza={r['INFLUENZA']} "
              f"preenchimento={r['POPULAR']}")
    print()

    # Divide a agenda em lotes de TAMANHO_LOTE (ordenados por tempo)
    lotes = [agenda[i:i + TAMANHO_LOTE]
             for i in range(0, len(agenda), TAMANHO_LOTE)]

    conns = abrir_conexoes()
    t0 = time.time()
    ok = 0
    try:
        for n_lote, lote in enumerate(lotes, 1):
            # Pacing: espera ate o instante planejado do FIM deste lote.
            # Se o produtor estiver atrasado, NAO dorme — recupera o atraso.
            planejado = t0 + lote[-1][0]
            atraso = planejado - time.time()
            if atraso > 0:
                time.sleep(atraso)

            ok += inserir_lote(conns, lote)

            decorrido = time.time() - t0
            atraso_real = decorrido - lote[-1][0]
            marcador = "" if atraso_real <= 2 else f" | ⚠️ {atraso_real:.0f}s atrasado"
            print(f"   Lote {n_lote}/{len(lotes)}: {ok}/{len(agenda)} inseridos "
                  f"| {decorrido:.1f}s decorridos{marcador}")

        duracao = time.time() - t0
        print(f"\n⏱  Duração real da inserção: {duracao:.1f}s | "
              f"Throughput produtor: {ok / duracao:.1f} casos/s")
        if duracao > CICLO_SEGUNDOS:
            print(f"⚠️ Inserção ultrapassou o ciclo de {CICLO_SEGUNDOS}s "
                  f"({duracao:.1f}s) — reduza TAMANHO_LOTE ou verifique o Postgres.")
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo usuário.")
    finally:
        for c in conns.values():
            c.close()

    print(f"\n✅ Fim do ciclo: {ok}/{len(agenda)} inseridos")
    for cidade in TAXAS:
        planejado = resumo[cidade]["TOTAL"]
        inseridos_cidade = planejado  # detalhamento por cidade vem dos lotes
        print(f"   ✅ {cidade}: agendados {planejado}")

if __name__ == "__main__":
    main()