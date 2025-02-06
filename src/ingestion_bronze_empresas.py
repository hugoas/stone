import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
from tqdm import tqdm
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
ZIP_FILE = os.getenv('ZIP_FILE', '/app/stone/data/Empresas.zip')  # Variável de ambiente no Docker
EXTRACT_PATH = os.getenv('EXTRACT_PATH', '/app/stone/temp')  # Pasta temporária para extração no contêiner
EXPECTED_CSV = "K3241.K03200Y1.D50111.EMPRECSV"  # Nome do CSV dentro do ZIP

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

# 📌 3️⃣ Criar partição dinamicamente
def create_partition(conn, data_ingestao):
    cursor = conn.cursor()
    partition_name = f"empresas_{data_ingestao.strftime('%Y_%m')}"

    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS bronze.{partition_name} 
        PARTITION OF bronze.empresas 
        FOR VALUES FROM ('{data_ingestao.strftime('%Y-%m-01')}') 
        TO ('{(data_ingestao.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-01')}');
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

# 📌 4️⃣ Limpar caracteres estranhos
def clean_text(value):
    return value.encode('utf-8', 'ignore').decode('utf-8').strip() if isinstance(value, str) else value

# 📌 5️⃣ Inserir dados no PostgreSQL
def insert_data(df):
    conn = connect_db()
    if conn is None:
        return

    create_partition(conn, datetime.now())
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO bronze.empresas (cnpj, razao_social, natureza_juridica, 
                                     qualificacao_responsavel, capital_social, 
                                     cod_porte, data_ingestao)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    try:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="📥 Inserindo dados"):
            cursor.execute(insert_query, (
                row['cnpj'], row['razao_social'], row['natureza_juridica'],
                row['qualificacao_responsavel'], row['capital_social'],
                row['cod_porte'], datetime.now()
            ))
        conn.commit()
        print("✅ Dados inseridos na Bronze!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados: {e}")
    finally:
        cursor.close()
        conn.close()

# 📌 6️⃣ Executar o processo de ingestão
def main():
    print("📂 Extraindo arquivo ZIP...")
    csv_file = extract_csv(ZIP_FILE, EXTRACT_PATH, EXPECTED_CSV)

    print(f"📥 Lendo o arquivo CSV extraído: {csv_file}")
    df = pd.read_csv(csv_file, sep=";", header=None, dtype=str, encoding="latin1")
    df.columns = ["cnpj", "razao_social", "natureza_juridica", "qualificacao_responsavel", "capital_social", "cod_porte", "ignore"]
    df.drop(columns=["ignore"], inplace=True)
    df["capital_social"] = df["capital_social"].str.replace(",", ".").astype(float)
    df = df.applymap(clean_text)

    print(f"📊 Processando {len(df)} registros para ingestão...")
    insert_data(df)

    # 🔥 Removendo arquivo temporário
    os.remove(csv_file)
    print("🧹 Arquivo temporário removido!")

if __name__ == "__main__":
    main()
