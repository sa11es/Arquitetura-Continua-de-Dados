# -*- coding: utf-8 -*-
"""
Simulador Epidemiológico — ciclo comprimido
5 minutos de execução = 24 horas simuladas.
Gera a média diária de notificações por cidade/doença
e injeta 1 surto garantido no último minuto.

Uso:
  python gerador.py                  # surto aleatório (doença, cidade e bairro)
  python gerador.py dengue           # surto garantido de dengue, resto sorteado
  python gerador.py --seed 42        # rodada reproduzível
"""
import sys
import time
import random
from datetime import datetime
import psycopg2

# ===================== CONFIG =====================
CICLO_SEGUNDOS   = 300      # 5 min = 24h simuladas
FIM_BASE         = 240      # casos "do dia" distribuídos até aqui
FATOR_CARGA      = 1.0      # reduza p/ 0.5 se Debezium não acompanhar o volume
SEED             = None     # None = surto sorteado a cada execução

DB_CONFIGS = {
    "Ponta Grossa": {"port": 5433, "ibge": "4119905"},
    "Curitiba":     {"port": 5434, "ibge": "4106902"},
    "Castro":       {"port": 5435, "ibge": "4104153"},  # conferir IBGE (4104907 no script antigo)
}
DB_USER, DB_PASS, DB_NAME, DB_HOST = "postgres", "postgrespassword", "sinan_db", "localhost"

# Média diária: total da cidade + alvos monitorados (resto = POPULAR)
TAXAS = {
    "Ponta Grossa": {"total": 372,  "DENGUE": 0.37, "MENINGITE": 0.34, "INFLUENZA": 3.12},
    "Curitiba":     {"total": 2979, "DENGUE": 0.80, "MENINGITE": 0.57, "INFLUENZA": 16.0},
    "Castro":       {"total": 162,  "DENGUE": 0.03, "MENINGITE": 0.005, "INFLUENZA": 0.12},
}

CID10      = {"DENGUE": "A90", "MENINGITE": "A39", "INFLUENZA": "J11"}
CID_POPULAR = ["A09", "J18", "K04"]   # gastroenterite, pneumonia, odontológico

BAIRROS = {
    "Ponta Grossa": ["CENTRO", "OFICINAS", "UVARANAS", "NOVA RUSSIA", "CARLOS CARVALHO"],
    "Curitiba":     ["CENTRO", "BATEL", "BOQUEIRAO", "SITIO CERCADO", "PINHEIRINHO"],
    "Castro":       ["CENTRO", "COHAB", "VILA ISA", "MOCAMINHO"],
}

# Mínimos que GARANTEM o disparo do alerta
SURTO = {
    "MENINGITE": {"n": 3,  "bairro_fixo": False},
    "DENGUE":    {"n": 6,  "bairro_fixo": False},   # 6 na cidade cobre a regra
    "INFLUENZA": {"n": 20, "bairro_fixo": True},    # 20 no mesmo bairro cobre a regra
}

# ===================== HELPERS =====================
def sortear(qtd_media):
    """Converte média diária fracionária em nº de casos do ciclo."""
    if qtd_media < 1:
        return 1 if random.random() < qtd_media else 0
    n = int(qtd_media)
    return n + (1 if random.random() < (qtd_media - n) else 0)

def montar_agenda(doenca_forcada=None):
    agenda = []
    for cidade, tx in TAXAS.items():
        n_deng = sortear(tx["DENGUE"])
        n_men  = sortear(tx["MENINGITE"])
        n_inf  = sortear(tx["INFLUENZA"])
        n_pop  = max(0, tx["total"] - n_deng - n_men - n_inf)

        plano = ([("DENGUE", CID10["DENGUE"])] * n_deng +
                 [("MENINGITE", CID10["MENINGITE"])] * n_men +
                 [("INFLUENZA", CID10["INFLUENZA"])] * n_inf +
                 [("POPULAR", random.choice(CID_POPULAR))] * n_pop)

        for doenca, cid in plano:
            agenda.append((random.uniform(0, FIM_BASE), cidade, doenca, cid,
                           random.choice(BAIRROS[cidade])))

    # ---------- SURTO GARANTIDO (último minuto) ----------
    doenca_surto = (doenca_forcada if doenca_forcada in SURTO
                    else random.choice(list(SURTO.keys())))
    cidade_surto = random.choice(list(TAXAS.keys()))
    bairro_base  = random.choice(BAIRROS[cidade_surto])
    regra = SURTO[doenca_surto]

    for _ in range(regra["n"]):
        t = random.uniform(FIM_BASE + 2, CICLO_SEGUNDOS - 1)
        bairro = bairro_base if regra["bairro_fixo"] else random.choice(BAIRROS[cidade_surto])
        agenda.append((t, cidade_surto, doenca_surto, CID10[doenca_surto], bairro))

    agenda.sort(key=lambda e: e[0])
    return agenda, (doenca_surto, cidade_surto, bairro_base, regra["n"])

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
    args = sys.argv[1:]

    # --seed N  -> rodada reproduzível
    seed = SEED
    if "--seed" in args:
        try:
            seed = int(args[args.index("--seed") + 1])
        except (IndexError, ValueError):
            print("Uso incorreto de --seed. Ex: python gerador.py --seed 42")
            sys.exit(1)
        idx = args.index("--seed")
        args = args[:idx] + args[idx + 2:]

    # argumento posicional opcional: nome da doença do surto
    doenca_forcada = None
    if args:
        candidato = args[0].upper()
        if candidato in SURTO:
            doenca_forcada = candidato
        else:
            print(f"Doença '{candidato}' não reconhecida. Opções: {', '.join(SURTO.keys())}")
            sys.exit(1)

    if seed is not None:
        random.seed(seed)

    agenda, (doenca_s, cidade_s, bairro_s, n_s) = montar_agenda(doenca_forcada)
    total_pop = sum(1 for e in agenda if e[2] == "POPULAR")
    print(f"🚀 Ciclo de {CICLO_SEGUNDOS}s = 24h simuladas | {len(agenda)} notificações "
          f"({total_pop} de preenchimento)")
    print(f"🚨 Surto planejado: {n_s} casos de {doenca_s} em {cidade_s} "
          f"(bairro âncora: {bairro_s})\n")

    conns = abrir_conexoes()
    t0 = time.time()
    ok = surto_ok = 0
    try:
        for t, cidade, doenca, cid, bairro in agenda:
            atraso = t0 + t - time.time()
            if atraso > 0:
                time.sleep(atraso)
            if inserir(conns[cidade], cidade, doenca, cid, bairro, datetime.now()):
                ok += 1
                if doenca == doenca_s and cidade == cidade_s:
                    surto_ok += 1
                    print(f"🚨 [{surto_ok}/{n_s}] SURTO {doenca_s} → {cidade_s} "
                          f"(bairro: {bairro})")
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo usuário.")
    finally:
        for c in conns.values():
            c.close()
    print(f"\n✅ Fim do ciclo: {ok}/{len(agenda)} inseridos | "
          f"Surto de {doenca_s}: {surto_ok}/{n_s} casos injetados em {cidade_s}")

if __name__ == "__main__":
    main()