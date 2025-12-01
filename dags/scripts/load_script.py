import sys
import os

# 1. CORREÇÃO CRÍTICA DE IMPORTAÇÃO: 
# Adiciona o caminho do projeto (que contém 'config') ao path do Python ANTES de qualquer importação local.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# 2. IMPORTS LOCAIS (O Python agora pode encontrar 'config.database')
import pandas as pd
from sqlalchemy import create_engine, text 
from config.database import get_engine 
# Note: create_engine e text são mantidos, embora get_engine() chame create_engine.

# --- CONEXÃO COM O BANCO DE DADOS ---
engine = get_engine()
print("Conexão com PostgreSQL estabelecida.")

# ----------------------------------------------------------------------
## 3. CARGA DA FATO_SENTIMENTO (Silver Layer)
# ----------------------------------------------------------------------
silver_path = '/opt/airflow/data/silver/dados_limpos.csv' 
df_fato = pd.read_csv(silver_path)

# A. Conversão de Tipo Defensiva
df_fato['score_sentimento'] = pd.to_numeric(df_fato['score_sentimento'], errors='coerce')

# B. Conversão da Data (Crucial para o PostgreSQL rejeitar strings)
df_fato['data_postagem'] = pd.to_datetime(df_fato['data_postagem'], errors='coerce')

# C. Limpeza Defensiva (Remove linhas com Nulos em colunas NOT NULL)
# Garante que a data e o score sejam válidos antes de carregar
df_fato.dropna(subset=['data_postagem', 'score_sentimento'], inplace=True)
print(f"Linhas após dropna: {df_fato.shape[0]}") 

# Carga na FATO (com data e tipos limpos)
df_fato.to_sql('FATO_SENTIMENTO', engine, if_exists='replace', index=False)
print(f"✅ Tabela FATO_SENTIMENTO carregada com {df_fato.shape[0]} registros.")

# ----------------------------------------------------------------------
## 4. CARGA DAS MÉTRICAS AGREGADAS (Gold Layer)
# ----------------------------------------------------------------------
gold_metrics_path = '/opt/airflow/data/gold/metricas_diarias_sentimento.csv'
df_metricas = pd.read_csv(gold_metrics_path)

# Carga na METRICAS_DIARIAS
df_metricas.to_sql('METRICAS_DIARIAS', engine, if_exists='replace', index=False)
# CORREÇÃO DE SINTAXE na linha de print:
print(f"✅ Tabela METRICAS_DIARIAS carregada com {df_metricas.shape[0]} registros.") 

# ----------------------------------------------------------------------
## 5. GERAÇÃO E CARGA DA DIMENSÃO DE TEMPO
# ----------------------------------------------------------------------

# Cria a tabela de datas únicas
# Nota: Como 'data_postagem' já foi limpa e convertida acima, o código é seguro
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