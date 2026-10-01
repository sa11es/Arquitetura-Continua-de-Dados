import csv
import time
import json
import argparse
import statistics
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition


def juntar_em_excel(arquivo_csv, arquivo_amostras, nome_excel="resultados_teste.xlsx"):
    """Lê os CSVs gerados e unifica-os num único ficheiro Excel com duas abas."""
    print(f"  A compilar os dados para o ficheiro Excel: {nome_excel}...")
    try:
        df_metricas = pd.read_csv(arquivo_csv)
        df_amostras = pd.read_csv(arquivo_amostras)
        
        with pd.ExcelWriter(nome_excel, engine='openpyxl') as writer:
            df_metricas.to_excel(writer, sheet_name='Por_Segundo', index=False)
            df_amostras.to_excel(writer, sheet_name='Amostras_Individuais', index=False)
            
        print("  ✅ Ficheiro Excel gerado com sucesso!")
    except Exception as e:
        print(f"  ⚠ Erro ao gerar Excel: {e}")


def abrir_consumer_desde_agora(kafka_host, kafka_port, topic, tentativas=30, espera_seg=2):
    """
    Abre um KafkaConsumer SEM group_id: atribui manualmente todas as partições
    do tópico e posiciona a leitura no FIM (seek_to_end).
    """
    for tentativa in range(1, tentativas + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
                enable_auto_commit=False,
                value_deserializer=lambda x: x.decode("utf-8"),
            )
            particoes = consumer.partitions_for_topic(topic)
            if not particoes:
                raise RuntimeError(f"tópico '{topic}' ainda não existe")

            tps = [TopicPartition(topic, p) for p in particoes]
            consumer.assign(tps)
            consumer.seek_to_end(*tps)
            return consumer
        except Exception as e:
            print(f"Aguardando o tópico '{topic}' ficar disponível (tentativa {tentativa}/{tentativas}): {e}")
            time.sleep(espera_seg)

    raise RuntimeError(f"Não foi possível localizar o tópico '{topic}' após {tentativas} tentativas.")


def percentil(valores_ordenados, p):
    """Percentil por interpolação linear."""
    if not valores_ordenados:
        return 0
    if len(valores_ordenados) == 1:
        return valores_ordenados[0]
    k = (len(valores_ordenados) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(valores_ordenados) - 1)
    if f == c:
        return valores_ordenados[f]
    return valores_ordenados[f] + (valores_ordenados[c] - valores_ordenados[f]) * (k - f)


def run(kafka_host, kafka_port, topic, arquivo_csv, arquivo_amostras, duracao):
    print(f"\n{'='*60}")
    print(f"  COLETOR DE MÉTRICAS DE PERFORMANCE (THROUGHPUT & LATÊNCIA)")
    print(f"  Kafka: {kafka_host}:{kafka_port}")
    print(f"  Tópico: {topic}")
    print(f"  Duração: {duracao} segundos")
    print(f"  CSV (série por segundo): {arquivo_csv}")
    print(f"  CSV (amostras individuais): {arquivo_amostras}")
    print(f"{'='*60}\n")

    consumer = abrir_consumer_desde_agora(kafka_host, kafka_port, topic)
    print("✅ Conectado ao Kafka, posicionado no fim do tópico. A aguardar novos alertas...\n")

    with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_iso", "timestamp_epoch", "segundo",
            "eventos_cidade_no_segundo", "eventos_cidade_acumulados",
            "alertas_bairro_no_segundo",
            "latencia_min_ms", "latencia_mediana_ms", "latencia_media_ms", "latencia_max_ms"
        ])

    with open(arquivo_amostras, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["segundo", "doenca", "municipio", "latencia_ms", "origem_ts_ms", "recebido_ts_ms"])

    eventos_cidade_total = 0
    alertas_bairro_total = 0
    latencias_totais = [] 
    avisos_latencia_alta = 0
    segundo_atual = 1

    try:
        while segundo_atual <= duracao:
            ciclo_inicio = time.time()
            eventos_cidade_no_segundo = 0
            alertas_bairro_no_segundo = 0
            latencias_no_segundo = []

            while time.time() - ciclo_inicio < 1.0:
                try:
                    records = consumer.poll(timeout_ms=200, max_records=500)
                    for partition_msgs in records.values():
                        for msg in partition_msgs:
                            recv_ms = round(time.time() * 1000)

                            try:
                                payload = json.loads(msg.value)
                            except Exception:
                                continue

                            tipo_agregacao = payload.get("tipo_agregacao")

                            if tipo_agregacao == "BAIRRO":
                                alertas_bairro_no_segundo += 1
                                alertas_bairro_total += 1
                                continue 

                            if tipo_agregacao != "CIDADE":
                                continue 

                            eventos_cidade_no_segundo += 1
                            eventos_cidade_total += 1

                            ts_origem = payload.get("origem_ts_ms")
                            if not ts_origem or not isinstance(ts_origem, (int, float)) or ts_origem <= 0:
                                continue

                            latencia = recv_ms - ts_origem

                            if latencia < 0:
                                print(
                                    f"[AVISO] Latência negativa ({latencia} ms) em "
                                    f"{payload.get('chave')} — possível dessincronia de relógio."
                                )
                                continue

                            if latencia > 60_000:
                                avisos_latencia_alta += 1
                                print(
                                    f"[AVISO] Latência de {latencia} ms em {payload.get('chave')} "
                                    f"— muito acima do esperado."
                                )

                            latencias_no_segundo.append(latencia)
                            latencias_totais.append(latencia)

                            with open(arquivo_amostras, "a", newline="", encoding="utf-8") as fa:
                                csv.writer(fa).writerow([
                                    segundo_atual, payload.get("doenca"), payload.get("municipio"),
                                    latencia, ts_origem, recv_ms
                                ])

                except Exception:
                    pass

            if latencias_no_segundo:
                ordenadas = sorted(latencias_no_segundo)
                lat_min = round(min(ordenadas), 2)
                lat_max = round(max(ordenadas), 2)
                lat_med = round(statistics.median(ordenadas), 2)
                lat_media = round(sum(ordenadas) / len(ordenadas), 2)
            else:
                lat_min, lat_max, lat_med, lat_media = 0, 0, 0, 0

            timestamp_iso = datetime.now().isoformat()
            timestamp_epoch = int(time.time())

            with open(arquivo_csv, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp_iso, timestamp_epoch, segundo_atual,
                    eventos_cidade_no_segundo, eventos_cidade_total,
                    alertas_bairro_no_segundo,
                    lat_min, lat_med, lat_media, lat_max
                ])

            if eventos_cidade_no_segundo > 0 or segundo_atual % 5 == 0:
                print(
                    f"[{segundo_atual:3d}s] Eventos/s: {eventos_cidade_no_segundo:4d} | "
                    f"Total: {eventos_cidade_total:6d} | "
                    f"Latência Med/Méd: {lat_med:6.1f}/{lat_media:6.1f} ms"
                )

            segundo_atual += 1

    except KeyboardInterrupt:
        print("\n⚠ Coleta interrompida pelo utilizador.")

    finally:
        consumer.close()

    duracao_real = max(segundo_atual - 1, 1)
    media_vazao = round(eventos_cidade_total / duracao_real, 2)

    ordenadas_totais = sorted(latencias_totais)
    lat_geral_media = round(sum(ordenadas_totais) / len(ordenadas_totais), 2) if ordenadas_totais else 0
    lat_geral_mediana = round(statistics.median(ordenadas_totais), 2) if ordenadas_totais else 0
    lat_geral_p95 = round(percentil(ordenadas_totais, 95), 2) if ordenadas_totais else 0
    lat_geral_p99 = round(percentil(ordenadas_totais, 99), 2) if ordenadas_totais else 0
    lat_geral_min = round(min(ordenadas_totais), 2) if ordenadas_totais else 0
    lat_geral_max = round(max(ordenadas_totais), 2) if ordenadas_totais else 0

    print(f"\n{'='*60}")
    print(f"  RESUMO DO TESTE DE PERFORMANCE")
    print(f"  Eventos (alertas CIDADE): {eventos_cidade_total}")
    print(f"  Alertas BAIRRO (não contam no throughput oficial): {alertas_bairro_total}")
    print(f"  Duração do teste: {duracao_real} segundos")
    print(f"  Throughput Médio: {media_vazao} eventos/segundo")
    print(f"  Latência Média:   {lat_geral_media} ms")
    print(f"  Latência Mediana: {lat_geral_mediana} ms")
    print(f"  Latência P95:     {lat_geral_p95} ms")
    print(f"  Latência P99:     {lat_geral_p99} ms")
    print(f"  Latência Mínima:  {lat_geral_min} ms")
    print(f"  Latência Máxima:  {lat_geral_max} ms")
    if avisos_latencia_alta:
        print(f"  ⚠ {avisos_latencia_alta} amostra(s) com latência > 60s — rever antes de usar nas tabelas.")
    print(f"  Série por segundo: {arquivo_csv}")
    print(f"  Amostras individuais: {arquivo_amostras}")
    print(f"{'='*60}")

    juntar_em_excel(arquivo_csv, arquivo_amostras, "resultados_performance_final.xlsx")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coletor de métricas do pipeline")
    parser.add_argument("--kafka-host", default="localhost", help="Host do Kafka (default: localhost)")
    parser.add_argument("--kafka-port", default="9092", help="Porta do Kafka (default: 9092)")
    parser.add_argument("--topic", default="epidemiologia.alertas", help="Tópico de alertas (default: epidemiologia.alertas)")
    parser.add_argument("--csv", default="metricas_coletadas.csv", help="Arquivo CSV de saída (série por segundo)")
    parser.add_argument("--csv-amostras", default="latencias_amostras.csv", help="Arquivo CSV de saída (amostra individual por alerta CIDADE)")
    parser.add_argument("--duracao", type=int, default=120, help="Duração da recolha em segundos (default: 120)")
    args = parser.parse_args()

    run(args.kafka_host, args.kafka_port, args.topic, args.csv, args.csv_amostras, args.duracao)