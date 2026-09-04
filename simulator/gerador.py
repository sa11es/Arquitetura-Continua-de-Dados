import psycopg2
import time
import random
import csv
import argparse
from datetime import date, timedelta, datetime
from psycopg2.extras import execute_values

DB_USER = "postgres"
DB_PASS = "postgrespassword"
DB_NAME = "sinan_db"

BANCOS = [
    {
        "cidade": "Ponta Grossa",
        "codigo_ibge": "4119905",
        "host": "localhost",
        "port": "5433",
        "bairros": [
            "Centro", "Estrela", "Oficinas", "Uvaranas",
            "Nova Rússia", "Contorno", "Jardim Carvalho",
            "Órfãs", "Neves", "Boa Vista", "Chapada", "Ronda"
        ]
    },
    {
        "cidade": "Curitiba",
        "codigo_ibge": "4106902",
        "host": "localhost",
        "port": "5434",
        "bairros": [
            "Centro", "Água Verde", "Batel", "Champagnat",
            "Santa Felicidade", "Cabral", "Portão", "Cajuru",
            "Boqueirão", "Tatuquara", "Pinheirinho", "Sítio Cercado"
        ]
    },
    {
        "cidade": "Castro",
        "codigo_ibge": "4104907",
        "host": "localhost",
        "port": "5435",
        "bairros": [
            "Centro", "Invernada", "Cantagalo",
            "Jardim das Araucárias", "Vila Rio Branco", "Morada do Sol",
            "Jardim Social", "Lacustre", "Termas de Riviera",
            "Castrolanda", "Abapan", "Socavão"
        ]
    }
]

DOENCAS = {
    "DENGUE": "A90",
    "MENINGITE": "G03.9",
    "INFLUENZA": "J11"
}

def gerar_lote(banco, quantidade):
    """Gera uma lista de tuplas para inserção em batch."""
    lote = []
    for _ in range(quantidade):
        dt_sintomas = date.today() - timedelta(days=random.randint(1, 15))
        bairro = random.choice(banco["bairros"])
        doenca = random.choice(list(DOENCAS.keys()))
        cid_10 = DOENCAS[doenca]
        idade = random.randint(5, 80)
        sexo = random.choice(["M", "F", "I"])
        lote.append((
            dt_sintomas, banco["cidade"], banco["codigo_ibge"],
            bairro, doenca, cid_10, idade, sexo
        ))
    return lote

def abrir_conexoes():
    """Abre conexões persistentes para os 3 bancos."""
    conexoes = {}
    for banco in BANCOS:
        conn = psycopg2.connect(
            host=banco["host"],
            port=banco["port"],
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        conexoes[banco["cidade"]] = conn
        print(f"✅ Conexão estabelecida: {banco['cidade']} ({banco['host']}:{banco['port']})")
    return conexoes

def inserir_lote(conexao, banco, lote):
    """Insere um lote de notificações usando execute_values (batch insert)."""
    cursor = conexao.cursor()
    query = """
        INSERT INTO notificacoes
        (dt_sintomas, municipio_residencia, codigo_ibge,
         bairro, doenca, cid_10, idade, sexo)
        VALUES %s
    """
    execute_values(cursor, query, lote)
    conexao.commit()
    cursor.close()

def run(rps, duracao_segundos, arquivo_csv):
    print(f"\n{'='*60}")
    print(f"  GERADOR DE CARGA EPIDEMIOLÓGICA")
    print(f"  Taxa alvo: {rps} inserções/segundo")
    print(f"  Duração: {duracao_segundos} segundos")
    print(f"  Total estimado: {rps * duracao_segundos} notificações")
    print(f"  Arquivo de métricas: {arquivo_csv}")
    print(f"{'='*60}\n")

    conexoes = abrir_conexoes()

    # Arquivo CSV para registrar métricas de inserção
    with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_iso", "timestamp_epoch",
            "segundo", "cidade", "registros_inseridos",
            "tempo_insercao_ms", "rps_real"
        ])

    total_inserido = 0
    print("Iniciando geração de carga... Pressione CTRL+C para parar.\n")

    try:
        for segundo in range(1, duracao_segundos + 1):
            inicio_ciclo = time.time()
            timestamp_iso = datetime.now().isoformat()
            timestamp_epoch = int(inicio_ciclo)

            # Distribui o RPS entre os 3 bancos
            registros_por_banco = max(1, rps // 3)
            resto = rps - (registros_por_banco * 3)

            for i, banco in enumerate(BANCOS):
                qtd = registros_por_banco + (1 if i < resto else 0)
                lote = gerar_lote(banco, qtd)

                t0 = time.time()
                inserir_lote(conexoes[banco["cidade"]], banco, lote)
                t1 = time.time()

                tempo_ms = round((t1 - t0) * 1000, 2)
                total_inserido += qtd

                with open(arquivo_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        timestamp_iso, timestamp_epoch,
                        segundo, banco["cidade"], qtd,
                        tempo_ms, round(qtd / (t1 - t0), 2) if t1 > t0 else 0
                    ])

            # Calcula RPS real e ajusta o sleep
            tempo_gasto = time.time() - inicio_ciclo
            sleep_restante = max(0, 1.0 - tempo_gasto)

            rps_real = round(rps / max(tempo_gasto, 0.001), 1)
            print(
                f"[{segundo:3d}s] Inseridos: {rps} | "
                f"Total: {total_inserido} | "
                f"Tempo ciclo: {round(tempo_gasto*1000)}ms | "
                f"RPS real: {rps_real} | "
                f"Sleep: {round(sleep_restante*1000)}ms"
            )

            if sleep_restante > 0:
                time.sleep(sleep_restante)

    except KeyboardInterrupt:
        print(f"\n⚠ Geração interrompida pelo usuário.")

    finally:
        for cidade, conn in conexoes.items():
            conn.close()
            print(f"🔌 Conexão fechada: {cidade}")

    print(f"\n{'='*60}")
    print(f"  RESUMO DA CARGA")
    print(f"  Total inserido: {total_inserido} notificações")
    print(f"  Duração: {segundo} segundos")
    print(f"  RPS médio: {round(total_inserido / segundo, 1)}")
    print(f"  Métricas salvas em: {arquivo_csv}")
    print(f"{'='*60}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerador de carga epidemiológica")
    parser.add_argument("--rps", type=int, default=20,
                        help="Inserções por segundo (default: 20)")
    parser.add_argument("--duracao", type=int, default=60,
                        help="Duração em segundos (default: 60)")
    parser.add_argument("--csv", type=str, default="carga_inserts.csv",
                        help="Arquivo CSV de saída (default: carga_inserts.csv)")
    args = parser.parse_args()

    run(args.rps, args.duracao, args.csv)