import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
from tqdm import tqdm  # Barra de progresso
import re

# 🔹 Configuração do banco de dados
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
    partition_name = f"socios_{data_processamento.strftime('%Y_%m')}"
    start_date = data_processamento.strftime('%Y-%m-01')
    end_date = (data_processamento.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS silver.{partition_name} 
        PARTITION OF silver.socios 
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
        SELECT cnpj, tipo_socio, nome_socio, documento_socio, codigo_qualificacao_socio,
               data_entrada_sociedade, faixa_etaria, pais, representante_legal,
               nome_representante, qualificacao_representante, data_ingestao
        FROM bronze.socios;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Erro ao extrair dados: {e}")
        return None

# 📌 4️⃣ Limpeza de caracteres
def clean_text(value):
    return re.sub(r'[^\x20-\x7E]', '', value).replace('*', '').strip() if isinstance(value, str) else value

# 📌 5️⃣ Transformar os dados
def transform_data(df):
    if df is None or df.empty:
        print("⚠️ Nenhum dado para processar!")
        return None
    
    print("🔄 Transformando os dados...")
    df.dropna(subset=['cnpj', 'nome_socio', 'documento_socio'], inplace=True)
    df['documento_socio'] = df['documento_socio'].apply(clean_text)
    df['pais'] = df['pais'].apply(clean_text)
    df['data_processamento'] = datetime.now()
    return df

# 📌 6️⃣ Carregar dados na Silver
def load_to_silver(df):
    conn = connect_db()
    if conn is None:
        return
    
    create_partition(conn, datetime.now())  # Criar partição antes da inserção
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO silver.socios (cnpj, tipo_socio, nome_socio, documento_socio,
                                  codigo_qualificacao_socio, data_entrada_sociedade,
                                  faixa_etaria, pais, representante_legal,
                                  nome_representante, qualificacao_representante, data_processamento)
        VALUES %s
        ON CONFLICT (cnpj, documento_socio, data_processamento) DO NOTHING;
    """
    
    try:
        values = [
            (
                row['cnpj'], row['tipo_socio'], row['nome_socio'], row['documento_socio'],
                row['codigo_qualificacao_socio'], row['data_entrada_sociedade'],
                row['faixa_etaria'], row['pais'], row['representante_legal'],
                row['nome_representante'], row['qualificacao_representante'], row['data_processamento']
            )
            for _, row in df.iterrows()
        ]
        
        for i in tqdm(range(0, len(values), 10000), desc="📥 Inserindo dados na Silver", unit=" registros"):
            batch = values[i:i+10000]
            execute_values(cursor, insert_query, batch)
            conn.commit()
        
        print(f"✅ {len(df)} registros carregados na Silver!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados na Silver: {e}")
    finally:
        cursor.close()
        conn.close()

# 📌 7️⃣ Executar ETL da Silver
def main():
    df_bronze = extract_from_bronze()
    df_transformed = transform_data(df_bronze)
    if df_transformed is not None:
        load_to_silver(df_transformed)

if __name__ == "__main__":
    main()
