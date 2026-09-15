import time
import random
import psycopg2
from datetime import datetime

# =====================================================================
# ATENÇÃO: Verifique as portas! 
# Coloquei 5432, 5433 e 5434. Se no seu docker-compose as portas 
# dos bancos de Curitiba e Castro forem diferentes, altere aqui:
# =====================================================================
DB_CONFIGS = {
    "Ponta Grossa": {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4119905"},
    "Curitiba":     {"host": "localhost", "port": 5434, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4106902"},
    "Castro":       {"host": "localhost", "port": 5435, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4104907"}
}

DOENCAS = [
    ("DENGUE", "A90"),
    ("MENINGITE", "A39"),
    ("COVID-19", "U071")
]

BAIRROS = ["CENTRO", "OFICINAS", "UVARANAS", "BATEL", "BOQUEIRAO", "NOVA RUSSIA"]

def conectar_e_inserir():
    # Sorteia os dados do paciente
    cidade_nome, config = random.choice(list(DB_CONFIGS.items()))
    doenca, cid10 = random.choice(DOENCAS)
    bairro = random.choice(BAIRROS)
    
    # Timestamp atual em milissegundos para o Flink
    dt_notificacao = datetime.now()

   # Sorteia uma idade aleatória entre 1 e 90 anos
    idade = random.randint(1, 90)

    query = """
        INSERT INTO public.notificacoes 
        (municipio_residencia, codigo_ibge, bairro, doenca, cid_10, dt_notificacao, dt_sintomas, idade)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    valores = (cidade_nome, config["ibge"], bairro, doenca, cid10, dt_notificacao, dt_notificacao, idade)
    
    try:
        # Conecta no banco da cidade sorteada e insere o dado
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname=config["dbname"]
        )
        cursor = conn.cursor()
        cursor.execute(query, valores)
        conn.commit()
        
        print(f"✅ [+1 caso] {doenca} inserido em {cidade_nome} (Bairro: {bairro})")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao inserir em {cidade_nome}. A porta {config['port']} está certa? Erro: {e}")

print("🚀 Iniciando Simulador Epidemiológico Contínuo...")
print("Pressione CTRL+C no terminal para parar a simulação.\n")

try:
    while True:
        conectar_e_inserir()
        
        # Sorteia um tempo de espera entre 1 e 3 segundos antes do próximo caso
        tempo_espera = random.uniform(1.0, 3.0)
        time.sleep(tempo_espera)
        
except KeyboardInterrupt:
    print("\n🛑 Simulador parado com sucesso!")