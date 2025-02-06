import psycopg2
from psycopg2 import sql
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
from tqdm import tqdm 
import re
import zipfile
import os

# 🔹 Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "has2582"
}

# Caminho relativo ou variável de ambiente para o arquivo ZIP
ZIP_FILE = os.getenv('ZIP_FILE', '/app/stone/data/Socios.zip')  # A variável de ambiente pode ser configurada no Docker
EXTRACT_PATH = os.getenv('EXTRACT_PATH', '/app/stone/temp')  # Pasta temporária para extração no contêiner
EXPECTED_CSV = "K3241.K03200Y1.D50111.SOCIOCSV"  # Nome do CSV dentro do ZIP

# 📌 1️⃣ Função para extrair o CSV do ZIP
def extract_csv(zip_path, extract_to, expected_file):
    os.makedirs(extract_to, exist_ok=True)  # Cria o diretório se não existir
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)  # Extrai todos os arquivos para a pasta
        
        extracted_path = os.path.join(extract_to, expected_file)  
        
        if os.path.exists(extracted_path):
            return extracted_path
        else:
            raise FileNotFoundError(f"❌ Arquivo esperado ({expected_file}) não encontrado no ZIP.")

# 📌 2️⃣ Conectar ao banco de dados
def connect_db():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None

# 📌 3️⃣ Criar partição
def create_partition(conn, data_ingestao):
    cursor = conn.cursor()
    partition_name = f"socios_{data_ingestao.strftime('%Y_%m')}"
    start_date = data_ingestao.strftime('%Y-%m-01')
    end_date = (data_ingestao.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-01')

    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS bronze.{partition_name} 
        PARTITION OF bronze.socios 
        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """

    try:
        cursor.execute(create_partition_sql)
        conn.commit()
        print(f"✅ Partição {partition_name} criada/verificada.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Erro ao criar partição: {e}")
    finally:
        cursor.close()

# 📌 4️⃣ Limpeza de caracteres
def clean_text(value):
    return re.sub(r'[^\x20-\x7E]', '', value).strip() if isinstance(value, str) else value

# 📌 5️⃣ Inserir dados
def insert_data(df):
    conn = connect_db()
    if conn is None:
        return

    create_partition(conn, datetime.now())
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO bronze.socios (cnpj, tipo_socio, nome_socio, documento_socio, 
                                  codigo_qualificacao_socio, data_entrada_sociedade, 
                                  faixa_etaria, pais, representante_legal, 
                                  nome_representante, qualificacao_representante, data_ingestao)
        VALUES %s
        ON CONFLICT (cnpj, documento_socio, data_ingestao) DO NOTHING;
    """

    try:
        df = df.drop_duplicates(subset=['cnpj', 'documento_socio'])
        values = [
            (
                row['cnpj'], row['tipo_socio'], row['nome_socio'], row['documento_socio'],
                row['codigo_qualificacao_socio'], row['data_entrada_sociedade'], row['faixa_etaria'],
                row['pais'], row['representante_legal'], row['nome_representante'],
                row['qualificacao_representante'], datetime.now()
            )
            for _, row in df.iterrows()
        ]
        
        for i in tqdm(range(0, len(values), 10000), desc="📥 Inserindo dados na Bronze", unit=" registros"):
            batch = values[i:i+10000]
            execute_values(cursor, insert_query, batch)
            conn.commit()
        
        print("✅ Dados inseridos na Bronze!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados: {e}")
    finally:
        cursor.close()
        conn.close()

# 📌 6️⃣ Executar ingestão
def main():
    print("📂 Extraindo arquivo ZIP...")
    csv_file = extract_csv(ZIP_FILE, EXTRACT_PATH, EXPECTED_CSV)

    print(f"📥 Lendo o arquivo CSV extraído: {csv_file}")
    df = pd.read_csv(csv_file, sep=";", header=None, dtype=str, encoding="latin1")
    df.columns = ["cnpj", "tipo_socio", "nome_socio", "documento_socio",
                  "codigo_qualificacao_socio", "data_entrada_sociedade", "faixa_etaria",
                  "pais", "representante_legal", "nome_representante", "qualificacao_representante"]
    df["data_entrada_sociedade"] = pd.to_datetime(df["data_entrada_sociedade"], errors='coerce')
    df = df.applymap(clean_text)

    print(f"📊 Processando {len(df)} registros para ingestão...")
    insert_data(df)

    # 🔥 Removendo arquivo temporário
    os.remove(csv_file)
    print("🧹 Arquivo temporário removido!")

if __name__ == "__main__":
    main()
