import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# 2. IMPORTS LOCAIS
import pandas as pd
from sqlalchemy import create_engine, text 
from config.database import get_engine 

COLUNAS_FATO = ['data_postagem', 'score_sentimento', 'sentimento_binario', 'texto_original', 'texto_limpo', 'data_ingestao', 'data_processamento']
COLUNAS_METRICAS = ['data_metrica', 'total_posts', 'sentimento_medio', 'sentimento_max', 'sentimento_min']

# --- CONEXÃO COM O BANCO DE DADOS ---
engine = get_engine()
print("Conexão com PostgreSQL estabelecida.")

# ----------------------------------------------------------------------
## 3. CARGA DA FATO_SENTIMENTO (Silver Layer)
# ----------------------------------------------------------------------
silver_path = '/opt/airflow/data/silver/dados_limpos.csv' 
df_fato = pd.read_csv(silver_path)

# A. Conversão de Tipo Defensiva (Necessário para evitar falha no SQL)
df_fato['score_sentimento'] = pd.to_numeric(df_fato['score_sentimento'], errors='coerce')
df_fato['data_postagem'] = pd.to_datetime(df_fato['data_postagem'], errors='coerce')

# B. Limpeza Defensiva (Remove Nulos em colunas NOT NULL)
df_fato.dropna(subset=['data_postagem', 'score_sentimento'], inplace=True)
print(f"Linhas após dropna: {df_fato.shape[0]}") 

COLUNAS_FATO_EXISTENTES = [c for c in COLUNAS_FATO if c in df_fato.columns]
df_fato = df_fato[COLUNAS_FATO_EXISTENTES] 

# Carga na FATO (com colunas alinhadas)
df_fato.to_sql('FATO_SENTIMENTO', engine, if_exists='replace', index=False)
print(f"✅ Tabela FATO_SENTIMENTO carregada com {df_fato.shape[0]} registros.")

# ----------------------------------------------------------------------
## 4. CARGA DAS MÉTRICAS AGREGADAS (Gold Layer)
# ----------------------------------------------------------------------
gold_metrics_path = '/opt/airflow/data/gold/metricas_diarias_sentimento.csv'
df_metricas = pd.read_csv(gold_metrics_path)

# FILTRO RÍGIDO DE COLUNAS
df_metricas = df_metricas[COLUNAS_METRICAS] 

# Carga na METRICAS_DIARIAS
df_metricas.to_sql('METRICAS_DIARIAS', engine, if_exists='replace', index=False)
print(f"✅ Tabela METRICAS_DIARIAS carregada com {df_metricas.shape[0]} registros.") 

# ----------------------------------------------------------------------
## 5. GERAÇÃO E CARGA DA DIMENSÃO DE TEMPO
# ----------------------------------------------------------------------

# Cria a tabela de datas únicas (reutiliza o df_fato já limpo)
df_datas = df_fato[['data_postagem']].drop_duplicates()
df_datas['data'] = df_datas['data_postagem'].dt.date # Extrai apenas a data para a PK

df_datas['ano'] = df_datas['data'].apply(lambda x: x.year)
df_datas['mes'] = df_datas['data'].apply(lambda x: x.month)
df_datas['dia_da_semana'] = df_datas['data'].apply(lambda x: x.strftime('%A'))

# Carrega a DIM_TEMPO (apenas colunas necessárias para o Star Schema)
df_dim_tempo = df_datas[['data', 'ano', 'mes', 'dia_da_semana']].drop_duplicates()
df_dim_tempo.to_sql('DIM_TEMPO', engine, if_exists='replace', index=False)
print(f"✅ Tabela DIM_TEMPO (Calendário) carregada com {df_dim_tempo.shape[0]} registros únicos.")

print("\n--- CARGA COMPLETA NO DATA WAREHOUSE ---")