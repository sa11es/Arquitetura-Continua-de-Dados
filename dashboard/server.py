import os
import re
import json
import threading
import queue
import asyncio
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from kafka import KafkaConsumer
import uvicorn

app = FastAPI(title="Dashboard Epidemiologico")

fila_alertas = queue.Queue()
clientes_ws = []
alertas_recentes = []
MAX_ALERTAS = 100

# Estado atual: chave -> última mensagem recebida (contagem CUMULATIVA do Flink)
ultimos_registros = {}

# Baseline da rodada: chave -> valor cumulativo no momento do reset.
# Casos da rodada = valor atual - baseline (deltas). Usado nas ESTATISTICAS.
baseline_registros = {}

stats = {
    "total_casos": 0,
    "casos_por_doenca": {},
    "casos_por_cidade": {},
    "uptime_inicio": datetime.now().isoformat(),
}

# ===================== DIA DE 5 MINUTOS =====================
# Cada "dia" dura 5 minutos e e definido pelo eventTime da mensagem
# (janelas alinhadas ao relogio: 10:00-10:05, 10:05-10:10, ...).
# A contagem de SURTO considera apenas os casos do dia corrente.
# As ESTATISTICAS continuam somando a rodada inteira.
JANELA_DIA_SEG = 5 * 60

# Nomes possiveis do campo de tempo do evento na mensagem do Flink.
# Ajuste aqui se o seu job usar outro nome.
CAMPOS_EVENT_TIME = ("eventTime", "event_time", "eventtime", "timestamp_evento")

# Controle de dia por chave: chave -> {"dia": bucket, "inicio": cumulativo no
# inicio do dia, "ultimo": maior cumulativo ja visto}.
# NAO e limpo no reset (o cumulativo do Flink continua valendo).
dias_registros = {}

# Maior "dia" (bucket de 5 min) visto ate agora; usado para expirar surtos antigos.
dia_corrente = 0

# ===================== SURTOS =====================
# POPULAR serve apenas para povoar o banco — NUNCA gera surto.
DOENCAS_MONITORADAS = {"MENINGITE", "DENGUE", "INFLUENZA"}

# Regras MODERADA (por DIA de 5 minutos):
#   MENINGITE: 3 casos na mesma cidade
#   DENGUE:    6 casos na mesma cidade OU 3 no mesmo bairro
#   INFLUENZA: 30 casos na cidade OU 20 no mesmo bairro
# ALTA    = MODERADA x2    |    CRITICO = MODERADA x3
LIMIARES = {
    "MENINGITE": {"CIDADE": 3,  "BAIRRO": None},
    "DENGUE":    {"CIDADE": 6,  "BAIRRO": 3},
    "INFLUENZA": {"CIDADE": 30, "BAIRRO": 20},
}

# Surtos ativos do dia corrente: chave = "DOENCA|IBGE|BAIRRO"
surtos_ativos = {}


def obter_event_time(alerta: dict) -> float:
    """Retorna o eventTime da mensagem em segundos (epoch).
    Aceita epoch em ms/s ou string ISO-8601. Sem campo valido, usa o
    horario de recebimento como fallback."""
    for campo in CAMPOS_EVENT_TIME:
        valor = alerta.get(campo)
        if valor is None:
            continue
        try:
            if isinstance(valor, (int, float)):
                v = float(valor)
                return v / 1000.0 if v > 1e11 else v

            texto = str(valor).strip()
            if texto.replace(".", "", 1).isdigit():
                v = float(texto)
                return v / 1000.0 if v > 1e11 else v

            # trunca fracao de segundos > 6 digitos (ex.: nanos do Java)
            texto = re.sub(r"(\.\d{6})\d+", r"\1", texto).replace("Z", "+00:00")
            return datetime.fromisoformat(texto).timestamp()
        except (ValueError, OSError, OverflowError):
            continue

    print(
        f"[Kafka] ⚠️ eventTime ausente/invalido em {alerta.get('chave')} — "
        f"usando horario de recebimento."
    )
    return datetime.now().timestamp()


def atualizar_dia(alerta: dict):
    """
    Atualiza o controle de dia da chave. DEVE ser chamada ANTES de gravar
    o alerta em ultimos_registros.

    Quando o eventTime cai em um novo dia de 5 min, o cumulativo anterior
    da chave vira o 'inicio' do novo dia — assim os casos do dia
    recomecam do zero.

    Retorna o dia (bucket) da mensagem, ou None se ela for atrasada
    (pertence a um dia anterior ao ja registrado para a chave).
    """
    chave = alerta.get("chave")
    if not chave:
        return None

    dia = int(obter_event_time(alerta) // JANELA_DIA_SEG)
    qtd = alerta.get("quantidade_casos", 0)
    reg = dias_registros.get(chave)

    if reg is None:
        # Chave nunca vista por ESTE servidor. O acumulado do Flink pode incluir
        # casos de antes deste dia (ou de antes deste boot). Sem historico, so a
        # ULTIMA atualizacao (1 caso) e atribuida ao dia — evita que o card de
        # surto mostre o acumulado do Flink como se fosse do dia.
        # Chave realmente nova (qtd = 1) -> inicio = 0, sem perda.
        inicio = max(0, qtd - 1)
        dias_registros[chave] = {"dia": dia, "inicio": inicio, "ultimo": qtd}
        if qtd > 1:
            print(
                f"[Dia] ℹ️ {chave} vista pela 1ª vez com {qtd} acumulados — "
                f"contando 1 caso no dia (inicio do dia = {inicio})."
            )
        return dia

    if dia > reg["dia"]:
        reg["inicio"] = reg["ultimo"]
        reg["dia"] = dia
        reg["ultimo"] = max(reg["ultimo"], qtd)
        return dia

    reg["ultimo"] = max(reg["ultimo"], qtd)
    if dia < reg["dia"]:
        return None  # mensagem atrasada de um dia que ja passou
    return dia


def delta_de(alerta: dict) -> int:
    """Casos DA RODADA para a chave: valor atual - baseline.
    Chave sem baseline (nunca vista antes) -> tudo e desta rodada.
    Usado nas estatisticas (nao muda com o dia de 5 min)."""
    chave = alerta.get("chave")
    casos = alerta.get("quantidade_casos", 0)
    if chave not in baseline_registros:
        return casos
    return max(0, casos - baseline_registros[chave])


def casos_do_dia(alerta: dict) -> int:
    """Casos do DIA (janela de 5 min) para a chave.
    = cumulativo atual - max(baseline da rodada, cumulativo no inicio do dia)."""
    chave = alerta.get("chave")
    casos = alerta.get("quantidade_casos", 0)
    reg = dias_registros.get(chave)
    inicio_dia = reg["inicio"] if reg else 0
    base = max(baseline_registros.get(chave, 0), inicio_dia)
    return max(0, casos - base)


def deve_emitir_alerta(alerta: dict) -> bool:
    """
    Filtra mensagens e garante que a contagem de uma mesma chave
    NUNCA regrida (trata mensagens fora de ordem e sub-janelas).
    """
    chave = alerta.get("chave")
    qtd_atual = alerta.get("quantidade_casos", 0)

    if not chave:
        return True

    registro_anterior = ultimos_registros.get(chave)

    if registro_anterior is None:
        return True

    return qtd_atual > registro_anterior.get("quantidade_casos", 0)


def recalcular_estatisticas():
    """
    Métricas globais da RODADA ATUAL: soma apenas os DELTAS (casos desta
    rodada) das chaves de nivel CIDADE, para todas as cidades e todas as
    doenças (incluindo POPULAR).
    """
    total = 0
    por_doenca = {}
    por_cidade = {}

    for reg in list(ultimos_registros.values()):
        if reg.get("tipo_agregacao") != "CIDADE":
            continue

        d = delta_de(reg)
        doenca = reg.get("doenca", "DESCONHECIDA")
        cidade = reg.get("municipio", "DESCONHECIDA")

        total += d
        por_doenca[doenca] = por_doenca.get(doenca, 0) + d
        por_cidade[cidade] = por_cidade.get(cidade, 0) + d

    stats["total_casos"] = total
    stats["casos_por_doenca"] = por_doenca
    stats["casos_por_cidade"] = por_cidade


def expirar_surtos(dia: int):
    """Quando um novo dia de 5 min comeca, remove os surtos dos dias anteriores."""
    global dia_corrente
    if dia <= dia_corrente:
        return
    dia_corrente = dia

    vencidos = [k for k, s in list(surtos_ativos.items()) if s.get("dia", 0) < dia]
    for k in vencidos:
        surtos_ativos.pop(k, None)

    if vencidos:
        fila_alertas.put({"_tipo_": "surtos"})
        print(f"[SURTO] 🕐 Novo dia — {len(vencidos)} surto(s) do dia anterior encerrado(s).")


def detectar_surto(alerta: dict, dia: int):
    """Detecta surto pelos CASOS DO DIA (janela de 5 min), conforme as regras.
    MODERADA = limiar | ALTA = limiar x2 | CRITICO = limiar x3."""
    doenca = alerta.get("doenca", "")
    agregacao = alerta.get("tipo_agregacao", "")

    if doenca not in DOENCAS_MONITORADAS:
        return None
    if agregacao not in ("CIDADE", "BAIRRO"):
        return None

    limiar = LIMIARES[doenca].get(agregacao)
    if not limiar:  # doença sem regra nesse nível (ex.: MENINGITE por bairro)
        return None

    casos_dia = casos_do_dia(alerta)
    if casos_dia < limiar:
        return None

    ibge = alerta.get("codigo_ibge", "")
    bairro = alerta.get("bairro", "") if agregacao == "BAIRRO" else ""
    chave_surto = f"{doenca}|{ibge}|{bairro}".rstrip("|")

    if casos_dia >= limiar * 3:
        gravidade = "CRITICO"
    elif casos_dia >= limiar * 2:
        gravidade = "ALTA"
    else:
        gravidade = "MODERADA"

    anterior = surtos_ativos.get(chave_surto)
    if anterior and anterior.get("dia") == dia:
        ranks = {"MODERADA": 1, "ALTA": 2, "CRITICO": 3}
        melhorou = (
            casos_dia > anterior["casos"]
            or ranks[gravidade] > ranks[anterior["gravidade"]]
        )
        if not melhorou:
            return None

    surto = {
        "doenca": doenca,
        "municipio": alerta.get("municipio", ""),
        "bairro": bairro,
        "codigo_ibge": ibge,
        "casos": casos_dia,
        "limiar": limiar,
        "agregacao": agregacao,
        "gravidade": gravidade,
        "dia": dia,
        "janela_inicio": datetime.fromtimestamp(dia * JANELA_DIA_SEG).isoformat(),
        "janela_fim": datetime.fromtimestamp((dia + 1) * JANELA_DIA_SEG).isoformat(),
        "detectado_em": datetime.now().isoformat(),
    }
    surtos_ativos[chave_surto] = surto
    return surto


def consumir_kafka():
    """Thread dedicada a consumir as contagens do tópico Kafka."""
    consumer = KafkaConsumer(
        "epidemiologia.alertas",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        group_id="dashboard-server-v12",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    print("[Kafka] Consumer iniciado — aguardando contagens do Flink...")

    for msg in consumer:
        try:
            alerta = json.loads(msg.value)

            if deve_emitir_alerta(alerta):
                alerta["recebido_em"] = datetime.now().isoformat()

                # Atualiza o controle de dia ANTES de gravar o registro
                dia = atualizar_dia(alerta)

                chave = alerta.get("chave")
                if chave:
                    ultimos_registros[chave] = alerta

                alerta["casos_dia"] = casos_do_dia(alerta)

                recalcular_estatisticas()
                fila_alertas.put(alerta)

                # Surto so e avaliado para mensagens do dia corrente da chave
                if dia is not None:
                    expirar_surtos(dia)
                    surto = detectar_surto(alerta, dia)
                    if surto:
                        fila_alertas.put({"_tipo_": "surto", **surto})
                        escopo = (
                            f"bairro {surto['bairro']}" if surto["bairro"]
                            else f"cidade {surto['municipio']}"
                        )
                        print(
                            f"[SURTO] {surto['gravidade']}: {surto['doenca']} — "
                            f"{escopo} — {surto['casos']} casos no dia de 5 min "
                            f"(limiar {surto['limiar']})"
                        )

                print(
                    f"[Kafka] 📊 Contagem atualizada: {alerta.get('doenca')} em {alerta.get('municipio')} "
                    f"({alerta.get('quantidade_casos')} acumulados | "
                    f"{delta_de(alerta)} na rodada | "
                    f"{alerta['casos_dia']} no dia)"
                )
            else:
                print(
                    f"[Kafka] 🔇 Duplicado ou valor menor descartado: {alerta.get('chave')} ({alerta.get('quantidade_casos')} casos)"
                )

        except Exception as e:
            print(f"[Kafka] Erro ao processar mensagem: {e}")


async def processar_fila():
    """Loop assíncrono para despachar atualizações aos clientes via WebSocket."""
    while True:
        while not fila_alertas.empty():
            try:
                alerta = fila_alertas.get_nowait()

                # Apenas reenvia a lista de surtos ativos (ex.: apos expirar um dia)
                if alerta.get("_tipo_") == "surtos":
                    for ws in list(clientes_ws):
                        try:
                            await ws.send_json(
                                {"tipo": "surtos", "dados": list(surtos_ativos.values())}
                            )
                        except Exception:
                            if ws in clientes_ws:
                                clientes_ws.remove(ws)
                    continue

                if alerta.get("_tipo_") == "surto":
                    alerta.pop("_tipo_", None)
                    for ws in list(clientes_ws):
                        try:
                            await ws.send_json({"tipo": "surto", "dados": alerta})
                            await ws.send_json(
                                {
                                    "tipo": "surtos",
                                    "dados": list(surtos_ativos.values()),
                                }
                            )
                        except Exception:
                            if ws in clientes_ws:
                                clientes_ws.remove(ws)

                    print(
                        f"[Dashboard] 🚨 Surto transmitido via WS: {alerta.get('doenca')} em "
                        f"{alerta.get('municipio')} ({alerta.get('casos')} casos)"
                    )
                    continue

                alertas_recentes.append(alerta)
                if len(alertas_recentes) > MAX_ALERTAS:
                    alertas_recentes.pop(0)

                for ws in list(clientes_ws):
                    try:
                        await ws.send_json({"tipo": "alerta", "dados": alerta})
                        await ws.send_json({"tipo": "stats", "dados": stats})
                    except Exception:
                        if ws in clientes_ws:
                            clientes_ws.remove(ws)

                print(
                    f"[Dashboard] Transmitido via WS: {alerta.get('doenca')} em {alerta.get('municipio')} "
                    f"({alerta.get('quantidade_casos')} casos)"
                )

            except queue.Empty:
                break

        await asyncio.sleep(0.01)


@app.on_event("startup")
async def startup():
    thread = threading.Thread(target=consumir_kafka, daemon=True)
    thread.start()
    asyncio.create_task(processar_fila())


@app.get("/")
async def get_dashboard():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "index.html")

    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>index.html nao encontrado</h1>", status_code=404
        )


@app.get("/api/alertas")
async def get_alertas():
    return JSONResponse(
        content={
            "alertas": list(reversed(alertas_recentes)),
            "total": len(alertas_recentes),
        }
    )


@app.get("/api/stats")
async def get_stats():
    return JSONResponse(content=stats)


@app.get("/api/surtos")
async def get_surtos():
    return JSONResponse(
        content={
            "surtos": list(surtos_ativos.values()),
            "total": len(surtos_ativos),
        }
    )


@app.post("/api/reset")
async def reset_rodada():
    """
    Prepara uma nova rodada: fotografa os valores acumulados atuais como
    baseline (para calcular os deltas da rodada) e zera feed, surtos e stats.
    O controle de dias (dias_registros) e mantido, pois o cumulativo do
    Flink continua valendo.
    """
    for chave, reg in list(ultimos_registros.items()):
        baseline_registros[chave] = reg.get("quantidade_casos", 0)

    alertas_recentes.clear()
    ultimos_registros.clear()
    surtos_ativos.clear()

    stats["total_casos"] = 0
    stats["casos_por_doenca"] = {}
    stats["casos_por_cidade"] = {}

    for ws in list(clientes_ws):
        try:
            await ws.send_json({"tipo": "reset"})
            await ws.send_json({"tipo": "stats", "dados": stats})
        except Exception:
            if ws in clientes_ws:
                clientes_ws.remove(ws)

    print(
        f"[Dashboard] 🧹 Rodada reiniciada — baseline de {len(baseline_registros)} chaves, "
        f"feed/surtos/stats zerados."
    )
    return JSONResponse(content={"status": "ok"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clientes_ws.append(websocket)

    if alertas_recentes:
        await websocket.send_json(
            {"tipo": "historico", "dados": list(reversed(alertas_recentes))}
        )
    if surtos_ativos:
        await websocket.send_json(
            {"tipo": "surtos", "dados": list(surtos_ativos.values())}
        )
    await websocket.send_json({"tipo": "stats", "dados": stats})

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in clientes_ws:
            clientes_ws.remove(websocket)
    except Exception:
        if websocket in clientes_ws:
            clientes_ws.remove(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8501)