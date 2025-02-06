import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
from tqdm import tqdm 

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
def create_partition(conn, data_analise):
    cursor = conn.cursor()
    partition_name = f"empresas_{data_analise.strftime('%Y_%m')}"
    start_date = data_analise.strftime('%Y-%m-01')
    end_date = (data_analise.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS gold.{partition_name} 
        PARTITION OF gold.empresas 
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

# 📌 3️⃣ Extrair dados da Silver
def extract_from_silver():
    conn = connect_db()
    if conn is None:
        return None
    
    query = """
        SELECT e.cnpj, e.razao_social, e.capital_social,
               COUNT(s.cnpj) AS total_socios,
               BOOL_OR(s.pais NOT IN ('BRASIL', 'BRA')) AS flag_socio_estrangeiro
        FROM silver.empresas e
        LEFT JOIN silver.socios s ON e.cnpj = s.cnpj
        GROUP BY e.cnpj, e.razao_social, e.capital_social;
    """
    
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
    
    df['data_analise'] = datetime.now()
    
    df['flag_socio_estrangeiro'] = df['flag_socio_estrangeiro'].fillna(False)
    

    df['doc_alvo'] = (df['porte'] == '03') & (df['total_socios'] > 1)
    
    return df

# 📌 5️⃣ Carregar dados na Gold
def load_to_gold(df):
    conn = connect_db()
    if conn is None:
        return
    
    create_partition(conn, datetime.now())  
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO gold.empresas (cnpj, razao_social, capital_social, total_socios, flag_socio_estrangeiro, data_analise)
        VALUES %s
        ON CONFLICT (cnpj, data_analise) DO NOTHING;
    """
    
    try:
        values = [
            (
                row['cnpj'], row['razao_social'], row['capital_social'],
                row['total_socios'], row['flag_socio_estrangeiro'], row['data_analise']
            )
            for _, row in df.iterrows()
        ]
        
        for i in tqdm(range(0, len(values), 10000), desc="📥 Inserindo dados na Gold", unit=" registros"):
            batch = values[i:i+10000]
            execute_values(cursor, insert_query, batch)
            conn.commit()
        
        print(f"✅ {len(df)} registros carregados na Gold!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados na Gold: {e}")
    finally:
        cursor.close()
        conn.close()

# 📌 6️⃣ Executar ETL da Gold
def main():
    df_silver = extract_from_silver()
    df_transformed = transform_data(df_silver)
    if df_transformed is not None:
        load_to_gold(df_transformed)

if __name__ == "__main__":
    main()
