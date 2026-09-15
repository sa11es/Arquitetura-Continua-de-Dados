import time
import random
import psycopg2
from datetime import datetime

# Configuração dos bancos de dados
DB_CONFIGS = {
    "Ponta Grossa": {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4119905"},
    "Curitiba":     {"host": "localhost", "port": 5434, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4106902"},
    "Castro":       {"host": "localhost", "port": 5435, "user": "postgres", "password": "postgrespassword", "dbname": "sinan_db", "ibge": "4104907"}
}

DOENCAS = [
    ("DENGUE", "A90"),
    ("MENINGITE", "A39"),
    ("INFLUENZA", "J11")
]

BAIRROS = ["CENTRO", "OFICINAS", "UVARANAS", "BATEL", "BOQUEIRAO", "NOVA RUSSIA"]

def rodar_teste_auditoria(total_casos=30, intervalo=0.3):
    print("=" * 65)
    print(f"📊 INICIANDO TESTE DE AUDITORIA DE FLUXO ({total_casos} CASOS)")
    print("=" * 65)
    
    # Contadores para auditoria
    enviados_por_cidade = {cidade: 0 for cidade in DB_CONFIGS}
    enviados_por_doenca = {doenca[0]: 0 for doenca in DOENCAS}
    enviados_total = 0
    
    query = """
        INSERT INTO public.notificacoes 
        (municipio_residencia, codigo_ibge, bairro, doenca, cid_10, dt_notificacao, dt_sintomas, idade)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    for i in range(1, total_casos + 1):
        cidade_nome, config = random.choice(list(DB_CONFIGS.items()))
        doenca, cid10 = random.choice(DOENCAS)
        bairro = random.choice(BAIRROS)
        idade = random.randint(1, 90)
        agora = datetime.now()
        
        try:
            conn = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                user=config["user"],
                password=config["password"],
                dbname=config["dbname"]
            )
            cursor = conn.cursor()
            valores = (cidade_nome, config["ibge"], bairro, doenca, cid10, agora, agora, idade)
            cursor.execute(query, valores)
            conn.commit()
            cursor.close()
            conn.close()
            
            enviados_total += 1
            enviados_por_cidade[cidade_nome] += 1
            enviados_por_doenca[doenca] += 1
            
            print(f"[{i:02d}/{total_casos}] ✅ Inserido no Postgres -> {cidade_nome:<12} | {doenca:<10} | Bairro: {bairro}")
        except Exception as e:
            print(f"[{i:02d}/{total_casos}] ❌ Erro ao inserir em {cidade_nome}: {e}")
            
        time.sleep(intervalo)
        
    print("\n" + "=" * 65)
    print("📋 RESUMO DA CARGA ENVIADA AO BANCO (EXPECTATIVA DE TOTALIZAÇÃO):")
    print("=" * 65)
    print(f"TOTAL DE CASOS ENVIADOS NESTA RODADA: {enviados_total}")
    print("\nPor Cidade:")
    for cid, cnt in enviados_por_cidade.items():
        print(f"  • {cid:<15}: {cnt} casos")
    print("\nPor Doença:")
    for doen, cnt in enviados_por_doenca.items():
        print(f"  • {doen:<15}: {cnt} casos")
    print("=" * 65)
    print("💡 Aguarde cerca de 10 segundos e verifique a soma no seu Dashboard!")

if __name__ == "__main__":
    # Define a quantidade exata de registros para o teste (exemplo: 30 registros)
    rodar_teste_auditoria(total_casos=30, intervalo=0.3)