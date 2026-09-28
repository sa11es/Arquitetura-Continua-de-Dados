import os
import json
import time
import threading
import queue
import asyncio
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from kafka import KafkaConsumer, TopicPartition
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

# ===================== CARDS POR MUNICIPIO =====================
# Municipios simulados e doencas exibidas nos cards do painel. Sao pre-semeados
# com 0 para que os cards existam (e nao "pulem" na tela) antes do primeiro
# caso chegar. Os nomes seguem o que o Flink emite (MAIUSCULAS).
# Qualquer outra cidade/doenca que aparecer nos dados e adicionada dinamicamente.
CIDADES_CONHECIDAS = ["PONTA GROSSA", "CURITIBA", "CASTRO"]
DOENCAS_NOS_CARDS = ["DENGUE", "MENINGITE", "INFLUENZA"]


def estrutura_cidade_doenca_vazia():
    """{cidade: {doenca: 0}} com todas as cidades/doencas conhecidas zeradas."""
    return {c: {d: 0 for d in DOENCAS_NOS_CARDS} for c in CIDADES_CONHECIDAS}


stats = {
    "total_casos": 0,
    "casos_por_doenca": {},
    "casos_por_cidade": {},
    "casos_por_cidade_doenca": estrutura_cidade_doenca_vazia(),
    "uptime_inicio": datetime.now().isoformat(),
}


def delta_de(alerta: dict) -> int:
    """Casos DA RODADA para a chave: valor atual - baseline.
    Chave sem baseline (nunca vista antes) -> tudo e desta rodada."""
    chave = alerta.get("chave")
    casos = alerta.get("quantidade_casos", 0)
    if chave not in baseline_registros:
        return casos
    return max(0, casos - baseline_registros[chave])


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

    Alem dos totais por doenca e por cidade, monta o detalhamento
    cidade -> doenca usado pelos cards de municipio do painel.
    """
    total = 0
    por_doenca = {}
    por_cidade = {}
    por_cidade_doenca = estrutura_cidade_doenca_vazia()

    for reg in list(ultimos_registros.values()):
        if reg.get("tipo_agregacao") != "CIDADE":
            continue

        d = delta_de(reg)
        doenca = reg.get("doenca", "DESCONHECIDA")
        cidade = reg.get("municipio", "DESCONHECIDA")

        total += d
        por_doenca[doenca] = por_doenca.get(doenca, 0) + d
        por_cidade[cidade] = por_cidade.get(cidade, 0) + d

        doencas_da_cidade = por_cidade_doenca.setdefault(cidade, {})
        doencas_da_cidade[doenca] = doencas_da_cidade.get(doenca, 0) + d

    stats["total_casos"] = total
    stats["casos_por_doenca"] = por_doenca
    stats["casos_por_cidade"] = por_cidade
    stats["casos_por_cidade_doenca"] = por_cidade_doenca


def processar_mensagem(valor_bruto: str, silencioso: bool = False):
    """
    Processa uma mensagem do tópico de alertas, atualizando o estado
    (ultimos_registros, stats, alertas_recentes).

    silencioso=True é usado durante a RECONSTRUÇÃO DE ESTADO ao iniciar o
    servidor: o estado é atualizado normalmente, mas nada é enfileirado para
    o WebSocket (ninguém está conectado ainda) nem impresso no console
    (evita inundar o log ao reler um tópico com milhares de mensagens).
    """
    try:
        alerta = json.loads(valor_bruto)
    except Exception as e:
        print(f"[Kafka] Erro ao decodificar mensagem: {e}")
        return

    if not deve_emitir_alerta(alerta):
        if not silencioso:
            print(
                f"[Kafka] 🔇 Duplicado ou valor menor descartado: "
                f"{alerta.get('chave')} ({alerta.get('quantidade_casos')} casos)"
            )
        return

    alerta["recebido_em"] = datetime.now().isoformat()

    chave = alerta.get("chave")
    if chave:
        ultimos_registros[chave] = alerta

    recalcular_estatisticas()

    if silencioso:
        # Sem clientes conectados ainda: so alimenta o feed local (respeitando
        # o limite MAX_ALERTAS), sem passar pela fila/WebSocket.
        alertas_recentes.append(alerta)
        if len(alertas_recentes) > MAX_ALERTAS:
            alertas_recentes.pop(0)
    else:
        fila_alertas.put(alerta)
        print(
            f"[Kafka] 📊 Contagem atualizada: {alerta.get('doenca')} em {alerta.get('municipio')} "
            f"({alerta.get('quantidade_casos')} acumulados | {delta_de(alerta)} na rodada)"
        )


def _abrir_consumer_desde_o_inicio(tentativas=30, espera_seg=2):
    """
    Cria um KafkaConsumer SEM group_id (nao commita/depende de offset salvo),
    atribui manualmente todas as particoes do topico de alertas e posiciona
    a leitura no comeco. Assim, toda vez que o servidor sobe, ele consegue
    reler o historico inteiro e reconstruir o estado.

    Retorna (consumer, fim_bootstrap), onde fim_bootstrap e um dict
    TopicPartition -> offset (exclusivo) valido no instante da conexao:
    tudo que tiver offset menor que isso ja existia antes deste boot.
    """
    topico = "epidemiologia.alertas"
    for tentativa in range(1, tentativas + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=["localhost:9092"],
                enable_auto_commit=False,
                value_deserializer=lambda x: x.decode("utf-8"),
            )
            particoes = consumer.partitions_for_topic(topico)
            if not particoes:
                raise RuntimeError(f"tópico '{topico}' ainda não existe")

            tps = [TopicPartition(topico, p) for p in particoes]
            consumer.assign(tps)
            consumer.seek_to_beginning(*tps)
            fim_bootstrap = consumer.end_offsets(tps)
            return consumer, fim_bootstrap
        except Exception as e:
            print(
                f"[Kafka] Aguardando o tópico '{topico}' ficar disponível "
                f"(tentativa {tentativa}/{tentativas}): {e}"
            )
            time.sleep(espera_seg)

    print(f"[Kafka] Não foi possível localizar o tópico '{topico}'. Consumer não iniciado.")
    return None, {}


def consumir_kafka():
    """
    Thread dedicada a consumir as contagens do tópico Kafka.

    Primeiro relê TODO o histórico já publicado no tópico (reconstruindo
    ultimos_registros e stats em silêncio, sem transmitir nada pelo
    WebSocket) e, assim que alcança o fim do que já existia no momento da
    conexão, passa a tratar cada mensagem nova normalmente (com broadcast
    em tempo real).

    Como não usa group_id, isso acontece a cada reinício do servidor —
    útil para não perder os casos já inseridos ao dar Ctrl+C e rodar de novo.
    """
    consumer, fim_bootstrap = _abrir_consumer_desde_o_inicio()
    if consumer is None:
        return

    total_bootstrap = sum(fim_bootstrap.values())
    if total_bootstrap > 0:
        print(f"[Kafka] Reconstruindo estado a partir de {total_bootstrap} mensagem(ns) já existentes...")
    else:
        print("[Kafka] Nenhum histórico encontrado no tópico — iniciando do zero.")

    bootstrap_concluido = total_bootstrap == 0
    processadas_bootstrap = 0

    if bootstrap_concluido:
        print("[Kafka] Consumer ao vivo — aguardando contagens do Flink...")

    for msg in consumer:
        tp = TopicPartition(msg.topic, msg.partition)
        em_bootstrap = (not bootstrap_concluido) and msg.offset < fim_bootstrap.get(tp, 0)

        processar_mensagem(msg.value, silencioso=em_bootstrap)

        if em_bootstrap:
            processadas_bootstrap += 1
            if processadas_bootstrap >= total_bootstrap:
                bootstrap_concluido = True
                print(
                    f"[Kafka] Estado reconstruído: {len(ultimos_registros)} chave(s) ativa(s), "
                    f"{stats['total_casos']} caso(s) na rodada atual."
                )
                print("[Kafka] Consumer ao vivo — aguardando novas contagens do Flink...")


async def processar_fila():
    """Loop assíncrono para despachar atualizações aos clientes via WebSocket."""
    while True:
        while not fila_alertas.empty():
            try:
                alerta = fila_alertas.get_nowait()

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


@app.post("/api/reset")
async def reset_rodada():
    """
    Prepara uma nova rodada: fotografa os valores acumulados atuais como
    baseline (para calcular os deltas da rodada) e zera feed e stats.
    """
    for chave, reg in list(ultimos_registros.items()):
        baseline_registros[chave] = reg.get("quantidade_casos", 0)

    alertas_recentes.clear()
    ultimos_registros.clear()

    stats["total_casos"] = 0
    stats["casos_por_doenca"] = {}
    stats["casos_por_cidade"] = {}
    stats["casos_por_cidade_doenca"] = estrutura_cidade_doenca_vazia()

    for ws in list(clientes_ws):
        try:
            await ws.send_json({"tipo": "reset"})
            await ws.send_json({"tipo": "stats", "dados": stats})
        except Exception:
            if ws in clientes_ws:
                clientes_ws.remove(ws)

    print(
        f"[Dashboard] 🧹 Rodada reiniciada — baseline de {len(baseline_registros)} chaves, "
        f"feed/stats zerados."
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