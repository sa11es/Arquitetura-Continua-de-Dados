import csv
import time
import json
import argparse
import requests
from datetime import datetime
from kafka import KafkaConsumer

def get_job_id(flink_url):
    """Obtém o ID do job em execução no Flink."""
    try:
        resp = requests.get(f"{flink_url}/jobs/overview", timeout=5)
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if job.get("state") == "RUNNING":
                return job["jid"]
    except Exception as e:
        print(f"Erro ao obter job ID: {e}")
    return None

def get_flink_metrics(flink_url, job_id):
    """Coleta métricas do Flink via REST API."""
    metricas = {}
    try:
        resp = requests.get(
            f"{flink_url}/jobs/{job_id}/metrics",
            params={"get": "numRecordsInPerSecond,numRecordsOutPerSecond"},
            timeout=5
        )
        data = resp.json()
        for m in data.get("metrics", []):
            metricas[m["name"]] = float(m["value"])
    except Exception:
        pass
    return metricas

def run(kafka_host, kafka_port, topic, flink_url, arquivo_csv, duracao):
    print(f"\n{'='*60}")
    print(f"  COLETOR DE MÉTRICAS")
    print(f"  Kafka: {kafka_host}:{kafka_port}")
    print(f"  Tópico: {topic}")
    print(f"  Flink: {flink_url}")
    print(f"  Duração: {duracao} segundos")
    print(f"  CSV: {arquivo_csv}")
    print(f"{'='*60}\n")

    # Conecta ao Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
        auto_offset_reset="latest",
        group_id="metrics-collector",
        value_deserializer=lambda x: x.decode("utf-8"),
        consumer_timeout_ms=5000
    )
    print("✅ Conectado ao Kafka. Aguardando alertas...\n")

    # Arquivo CSV de métricas
    with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_iso", "timestamp_epoch", "segundo",
            "alertas_recebidos", "alertas_acumulados",
            "primeiro_alerta_ms", "ultimo_alerta_ms",
            "flink_records_in_ps", "flink_records_out_ps"
        ])

    job_id = get_job_id(flink_url)
    if job_id:
        print(f"✅ Job Flink encontrado: {job_id[:12]}...")
    else:
        print("⚠ Não foi possível obter o job ID do Flink.")

    alertas_total = 0
    segundo_atual = 1
    inicio = time.time()

    try:
        while segundo_atual <= duracao:
            ciclo_inicio = time.time()
            alertas_no_segundo = 0
            primeiro_ms = 0
            ultimo_ms = 0

            # Consome por ~1 segundo
            while time.time() - ciclo_inicio < 1.0:
                try:
                    records = consumer.poll(timeout_ms=500, max_records=100)
                    for partition_msgs in records.values():
                        for msg in partition_msgs:
                            alertas_no_segundo += 1
                            alertas_total += 1
                            recv_ms = round(time.time() * 1000)
                            if primeiro_ms == 0:
                                primeiro_ms = recv_ms
                            ultimo_ms = recv_ms
                except Exception:
                    pass

            # Coleta métricas do Flink a cada 5 segundos
            flink_in = 0
            flink_out = 0
            if segundo_atual % 5 == 0 and job_id:
                m = get_flink_metrics(flink_url, job_id)
                flink_in = m.get("numRecordsInPerSecond", 0)
                flink_out = m.get("numRecordsOutPerSecond", 0)

            timestamp_iso = datetime.now().isoformat()
            timestamp_epoch = int(time.time())

            with open(arquivo_csv, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp_iso, timestamp_epoch, segundo_atual,
                    alertas_no_segundo, alertas_total,
                    primeiro_ms, ultimo_ms,
                    flink_in, flink_out
                ])

            if alertas_no_segundo > 0 or segundo_atual % 5 == 0:
                print(
                    f"[{segundo_atual:3d}s] Alertas/s: {alertas_no_segundo} | "
                    f"Total: {alertas_total} | "
                    f"Flink IN: {flink_in} | Flink OUT: {flink_out}"
                )

            segundo_atual += 1

    except KeyboardInterrupt:
        print("\n⚠ Coleta interrompida pelo usuário.")

    finally:
        consumer.close()

    print(f"\n{'='*60}")
    print(f"  RESUMO DA COLETA")
    print(f"  Alertas totais recebidos: {alertas_total}")
    print(f"  Duração: {segundo_atual - 1} segundos")
    print(f"  Média alertas/s: {round(alertas_total / max(segundo_atual - 1, 1), 2)}")
    print(f"  Métricas salvas em: {arquivo_csv}")
    print(f"{'='*60}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coletor de métricas do pipeline")
    parser.add_argument("--kafka-host", default="localhost",
                        help="Host do Kafka (default: localhost)")
    parser.add_argument("--kafka-port", default="9092",
                        help="Porta do Kafka (default: 9092)")
    parser.add_argument("--topic", default="epidemiologia.alertas",
                        help="Tópico de alertas (default: epidemiologia.alertas)")
    parser.add_argument("--flink-url", default="http://localhost:8081",
                        help="URL do Flink REST API")
    parser.add_argument("--csv", default="metricas_coletadas.csv",
                        help="Arquivo CSV de saída")
    parser.add_argument("--duracao", type=int, default=120,
                        help="Duração da coleta em segundos (default: 120)")
    args = parser.parse_args()

    run(args.kafka_host, args.kafka_port, args.topic,
        args.flink_url, args.csv, args.duracao)