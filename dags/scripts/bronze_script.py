import pandas as pd
import os
from datetime import datetime

os.makedirs('/opt/airflow/data/bronze', exist_ok=True)
os.makedirs('/opt/airflow/data/silver', exist_ok=True)
os.makedirs('/opt/airflow/data/gold', exist_ok=True)
print("Estrutura de pastas verificada.")

# 2. Carregar dados da fonte original
df_raw = pd.read_csv('/opt/airflow/data/source/twitter_sentiment.csv', 
                     header=None,
                     encoding='latin-1',
                     nrows=1000)
    
    # 3. Adicionar informações de metadados
df_raw['data_ingestao'] = datetime.now()
df_raw['fonte_arquivo'] = 'twitter_sentiment.csv'

print("\nPrimeiras linhas (com metadados):")
print(df_raw.head())

# 4. Salvar na camada Bronze
df_raw.columns = ['Sentimento_Bruto', 'Data_Bruta', 'Texto_Bruto', 'FLAG', 'USER', 'TEXTO_ORIGINAL', 'data_ingestao', 'fonte_arquivo']

output_path = '/opt/airflow/data/bronze/dados_brutos.csv' 
df_raw.to_csv(output_path, index=False, header=True)

print(f"\nDados brutos salvos com sucesso na camada Bronze: {output_path}")