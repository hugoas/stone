import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# 📌 1️⃣ Executando os scripts de ingestão em sequência
if __name__ == "__main__":
    try:
        print("🚀 Iniciando ingestão de dados...")

        # Ingestão Bronze Empresas
        print("📥 Iniciando ingestão de empresas (bronze)...")
        import ingestion_bronze_empresas  
        ingestion_bronze_empresas.main() 

        # Ingestão Bronze Sócios
        print("📥 Iniciando ingestão de sócios (bronze)...")
        import ingestion_bronze_socios  
        ingestion_bronze_socios.main()  

        # Ingestão Silver Empresas
        print("📥 Iniciando ingestão de empresas (silver)...")
        import ingestion_silver_empresas  
        ingestion_silver_empresas.main()  

        # Ingestão Silver Sócios
        print("📥 Iniciando ingestão de sócios (silver)...")
        import ingestion_silver_socios 
        ingestion_silver_socios.main() 

        # Ingestão Gold
        print("📥 Iniciando ingestão de dados (gold)...")
        import ingestion_gold 
        ingestion_gold.main()  

        print("✅ Ingestão concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante a ingestão de dados: {e}")
