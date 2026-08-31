import psycopg2
import time
import random
from datetime import date, timedelta

# Credenciais do banco (mesmas definidas no docker-compose)
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "sinan_db"
DB_USER = "postgres"
DB_PASS = "postgrespassword"

def gerar_dados_aleatorios():
    # Dados fictícios para simulação
    municipios = ["Ponta Grossa", "Curitiba", "Castro", "Carambeí"]
    codigo_ibge = ["4119905", "4106902", "4104907", "4104659"]
    idx = random.randint(0, 3)
    
    dt_sintomas = date.today() - timedelta(days=random.randint(1, 15))
    idade = random.randint(5, 80)
    sexo = random.choice(['M', 'F'])
    cid_10 = "A90" # CID para Dengue Clássica
    
    return (dt_sintomas, municipios[idx], codigo_ibge[idx], cid_10, idade, sexo)

def iniciar_simulacao():
    try:
        # Conecta ao banco de dados no Docker
        conexao = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conexao.cursor()
        print("✅ Conectado ao banco de dados legado com sucesso!")
        print("💉 Iniciando simulação de digitação de fichas do SINAN...\n")
        
        while True:
            dados = gerar_dados_aleatorios()
            query = """
                INSERT INTO notificacoes_dengue 
                (dt_sintomas, municipio_residencia, codigo_ibge, cid_10, idade, sexo) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, dados)
            conexao.commit()
            print(f"Nova notificação: {dados[1]} | Idade: {dados[4]} anos | Sexo: {dados[5]}")
            
            # Pausa de 3 segundos para não inundar o banco na fase de testes
            time.sleep(3) 
            
    except Exception as e:
        print(f"❌ Erro ao conectar ou inserir dados: {e}")
    finally:
        if 'conexao' in locals():
            cursor.close()
            conexao.close()

if __name__ == "__main__":
    iniciar_simulacao()