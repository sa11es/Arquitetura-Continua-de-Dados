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

stats = {
    "total_alertas": 0,
    "alertas_por_doenca": {},
    "alertas_por_cidade": {},
    "alertas_por_gravidade": {"MODERADA": 0, "ALTA": 0, "CRITICA": 0},
    "uptime_inicio": datetime.now().isoformat(),
}

def consumir_kafka():
    """Thread que consome alertas do Kafka indefinidamente."""
    consumer = KafkaConsumer(
        "epidemiologia.alertas",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        group_id="dashboard-server-v4",
        value_deserializer=lambda x: x.decode("utf-8"),
        # SEM consumer_timeout_ms — o consumer fica vivo para sempre
    )
    print("[Kafka] Consumer iniciado — aguardando alertas...")

    for msg in consumer:
        try:
            alerta = json.loads(msg.value)
            alerta["recebido_em"] = datetime.now().isoformat()
            fila_alertas.put(alerta)
            print(f"[Kafka] Alerta na fila: {alerta.get('doenca')} em {alerta.get('municipio')}")
        except Exception as e:
            print(f"[Kafka] Erro: {e}")

async def processar_fila():
    """Loop que processa a fila e envia para clientes WS."""
    while True:
        while not fila_alertas.empty():
            try:
                alerta = fila_alertas.get_nowait()

                alertas_recentes.append(alerta)
                if len(alertas_recentes) > MAX_ALERTAS:
                    alertas_recentes.pop(0)

                stats["total_alertas"] += 1
                doenca = alerta.get("doenca", "DESCONHECIDA")
                stats["alertas_por_doenca"][doenca] = \
                    stats["alertas_por_doenca"].get(doenca, 0) + 1
                cidade = alerta.get("municipio", "DESCONHECIDA")
                stats["alertas_por_cidade"][cidade] = \
                    stats["alertas_por_cidade"].get(cidade, 0) + 1
                gravidade = alerta.get("gravidade", "MODERADA")
                if gravidade in stats["alertas_por_gravidade"]:
                    stats["alertas_por_gravidade"][gravidade] += 1

                for ws in list(clientes_ws):
                    try:
                        await ws.send_json({"tipo": "alerta", "dados": alerta})
                        await ws.send_json({"tipo": "stats", "dados": stats})
                    except Exception:
                        if ws in clientes_ws:
                            clientes_ws.remove(ws)

                print(f"[Dashboard] Enviado: {doenca} em {cidade} ({gravidade})")

            except queue.Empty:
                break

        await asyncio.sleep(0.5)

@app.on_event("startup")
async def startup():
    thread = threading.Thread(target=consumir_kafka, daemon=True)
    thread.start()
    asyncio.create_task(processar_fila())

@app.get("/")
async def get_dashboard():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>index.html nao encontrado</h1>", status_code=404)

@app.get("/api/alertas")
async def get_alertas():
    return JSONResponse(content={
        "alertas": list(reversed(alertas_recentes)),
        "total": len(alertas_recentes)
    })

@app.get("/api/stats")
async def get_stats():
    return JSONResponse(content=stats)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clientes_ws.append(websocket)

    if alertas_recentes:
        await websocket.send_json({
            "tipo": "historico",
            "dados": list(reversed(alertas_recentes))
        })
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