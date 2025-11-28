import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from sqlalchemy import create_engine, text 
from config.database import get_engine

engine = get_engine()
print("Conexão com PostgreSQL estabelecida.")

silver_path = '/opt/airflow/data/silver/dados_limpos.csv' 
df_fato = pd.read_csv(silver_path)

df_fato.to_sql('FATO_SENTIMENTO', engine, if_exists='replace', index=False)
print(f"✅ Tabela FATO_SENTIMENTO carregada com {df_fato.shape[0]} registros.")

gold_metrics_path = '/opt/airflow/data/gold/metricas_diarias_sentimento.csv'
df_metricas = pd.read_csv(gold_metrics_path)

df_metricas.to_sql('METRICAS_DIARIAS', engine, if_exists='replace', index=False)
print(f"✅ Tabela METRICAS_DIARIAS carregada com {df_metricas.shape[0]} registros.")

df_datas = df_fato[['data_postagem']].drop_duplicates()
df_datas['data'] = pd.to_datetime(df_datas['data_postagem']).dt.date

df_datas['ano'] = df_datas['data'].apply(lambda x: x.year)
df_datas['mes'] = df_datas['data'].apply(lambda x: x.month)
df_datas['dia_da_semana'] = df_datas['data'].apply(lambda x: x.strftime('%A'))

df_dim_tempo = df_datas[['data', 'ano', 'mes', 'dia_da_semana']].drop_duplicates()
df_dim_tempo.to_sql('DIM_TEMPO', engine, if_exists='replace', index=False)
print(f"✅ Tabela DIM_TEMPO (Calendário) carregada com {df_dim_tempo.shape[0]} registros únicos.")

print("\n--- CARGA COMPLETA NO DATA WAREHOUSE ---")