import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime
from tqdm import tqdm  # 🔹 Adicionando barra de progresso

# 🔹 Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "has2582"
}

# 📌 1️⃣ Conectar ao banco de dados
def connect_db():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None

# 📌 2️⃣ Criar partição dinamicamente antes da inserção
def create_partition(conn, data_processamento):
    cursor = conn.cursor()
    partition_name = f"empresas_{data_processamento.strftime('%Y_%m')}"
    start_date = data_processamento.strftime('%Y-%m-01')
    end_date = (data_processamento.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS silver.{partition_name} 
        PARTITION OF silver.empresas 
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

# 📌 3️⃣ Extrair dados da Bronze
def extract_from_bronze():
    conn = connect_db()
    if conn is None:
        return None
    
    query = """
        SELECT cnpj, razao_social, natureza_juridica, capital_social, cod_porte, data_ingestao
        FROM bronze.empresas;
    """  # 🔹 Removida a limitação de MAX(data_ingestao)
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Erro ao extrair dados: {e}")
        return None

# 📌 4️⃣ Transformar os dados
def transform_data(df):
    if df is None or df.empty:
        print("⚠️ Nenhum dado para processar!")
        return None
    
    print("🔄 Transformando os dados...")
    df.dropna(subset=['cnpj', 'razao_social', 'natureza_juridica'], inplace=True)  # 🔹 Remover apenas valores nulos
    df['capital_social'] = df['capital_social'].fillna(0).astype(float)
    df['porte_descricao'] = df['cod_porte'].map({
        '01': 'Microempresa',
        '02': 'Pequeno Porte',
        '03': 'Médio Porte',
        '04': 'Grande Porte'
    }).fillna('Desconhecido')
    df['data_processamento'] = datetime.now()
    df.drop(columns=['cod_porte'], inplace=True)
    return df

# 📌 5️⃣ Carregar dados na Silver
def load_to_silver(df):
    conn = connect_db()
    if conn is None:
        return
    
    create_partition(conn, datetime.now())  # 🔹 Criar partição antes da inserção
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO silver.empresas (cnpj, razao_social, natureza_juridica, capital_social, porte_descricao, data_processamento)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (cnpj, data_processamento) DO NOTHING;
    """
    
    try:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="📥 Inserindo dados na Silver"):
            cursor.execute(insert_query, (
                row['cnpj'], row['razao_social'], row['natureza_juridica'],
                row['capital_social'], row['porte_descricao'], row['data_processamento']
            ))
        conn.commit()
        print(f"✅ {len(df)} registros carregados na Silver!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados na Silver: {e}")
    finally:
        cursor.close()
        conn.close()

# 📌 6️⃣ Executar ETL da Silver
def main():
    df_bronze = extract_from_bronze()
    df_transformed = transform_data(df_bronze)
    if df_transformed is not None:
        load_to_silver(df_transformed)

if __name__ == "__main__":
    main()