import os
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

# Armazena o estado mais recente de cada chave (ex: "MENINGITE|CIDADE|4119905")
ultimos_registros = {}

stats = {
    "total_casos": 0,
    "casos_por_doenca": {},
    "casos_por_cidade": {},
    "uptime_inicio": datetime.now().isoformat(),
}


def deve_emitir_alerta(alerta: dict) -> bool:
    """
    Filtra mensagens repetidas vindas da janela deslizante do Flink.
    Retorna True se for uma chave nova ou se a quantidade de casos mudou.
    """
    chave = alerta.get("chave")
    qtd_atual = alerta.get("quantidade_casos", 0)

    if not chave:
        return True

    registro_anterior = ultimos_registros.get(chave)
    if (
        registro_anterior is None
        or registro_anterior.get("quantidade_casos") != qtd_atual
    ):
        return True

    return False


def recalcular_estatisticas():
    """
    Recalcula as métricas globais considerando apenas chaves de nível CIDADE
    para evitar a contagem dupla de registros agrupados por BAIRRO.
    """
    total = 0
    por_doenca = {}
    por_cidade = {}

    for reg in ultimos_registros.values():
        if reg.get("tipo_agregacao") == "CIDADE":
            qtd = reg.get("quantidade_casos", 0)
            doenca = reg.get("doenca", "DESCONHECIDA")
            cidade = reg.get("municipio", "DESCONHECIDA")

            total += qtd
            por_doenca[doenca] = por_doenca.get(doenca, 0) + qtd
            por_cidade[cidade] = por_cidade.get(cidade, 0) + qtd

    stats["total_casos"] = total
    stats["casos_por_doenca"] = por_doenca
    stats["casos_por_cidade"] = por_cidade


def consumir_kafka():
    """Thread dedicada a consumir as contagens do tópico Kafka."""
    consumer = KafkaConsumer(
        "epidemiologia.alertas",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        group_id="dashboard-server-v5",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    print("[Kafka] Consumer iniciado — aguardando contagens do Flink...")

    for msg in consumer:
        try:
            alerta = json.loads(msg.value)

            if deve_emitir_alerta(alerta):
                alerta["recebido_em"] = datetime.now().isoformat()

                chave = alerta.get("chave")
                if chave:
                    ultimos_registros[chave] = alerta

                recalcular_estatisticas()
                fila_alertas.put(alerta)

                print(
                    f"[Kafka] 📊 Contagem atualizada: {alerta.get('doenca')} em {alerta.get('municipio')} "
                    f"({alerta.get('quantidade_casos')} casos)"
                )
            else:
                print(
                    f"[Kafka] 🔇 Duplicado descartado (Janela sem alteração): {alerta.get('chave')}"
                )

        except Exception as e:
            print(f"[Kafka] Erro ao processar mensagem: {e}")


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