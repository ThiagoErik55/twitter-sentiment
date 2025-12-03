import pandas as pd
import numpy as np
import re
from datetime import datetime
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob 
import sys
import os

# Define o caminho raiz do projeto para que o Python encontre outros módulos/pastas
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# --- CAMINHOS ABSOLUTOS ---
BRONZE_PATH = '/opt/airflow/data/bronze/dados_brutos.csv'
SILVER_PATH = '/opt/airflow/data/silver/dados_limpos.csv'

# 1. Carregar dados da camada Bronze
df = pd.read_csv(BRONZE_PATH, header=None)
print(f"Dados originais (Bronze): {df.shape[0]} linhas")

# 2. Renomear defensivamente para as 8 colunas brutas do CSV
df.columns = ['Sentimento_Bruto', 'ID', 'Data_Bruta', 'FLAG', 'USER', 'Texto_Bruto', 'data_ingestao', 'fonte_arquivo']

# 3. Filtrar colunas relevantes e renomear para nomes limpos
df_clean = df[['Sentimento_Bruto', 'Data_Bruta', 'Texto_Bruto', 'data_ingestao']].copy() # Mantém data_ingestao
df_clean.columns = ['sentimento_label', 'data_postagem', 'texto_original', 'data_ingestao']

# 4. Limpeza Estrutural
df_clean['data_postagem'] = pd.to_datetime(df_clean['data_postagem'], errors='coerce')
df_clean['sentimento_binario'] = df_clean['sentimento_label'].apply(lambda x: 1 if x == 4 else 0)
df_clean.drop_duplicates(inplace=True)
df_clean.dropna(inplace=True, subset=['data_postagem']) 

print(f"\nLinhas após limpeza estrutural: {len(df_clean)}. Duplicatas/Nulos removidos: {len(df) - len(df_clean)}")

# 5. Processamento de Linguagem Natural (NLP)
def clean_text(text):
    text = str(text)
    text = re.sub(r'http\S+|www.\S+', '', text) # Remove links
    text = re.sub(r'@\w+|#\w+', '', text) # Remove menções e hashtags
    text = re.sub(r'[^\w\s]', '', text).lower() # Remove pontuação
    return text.strip()

df_clean['texto_limpo'] = df_clean['texto_original'].apply(clean_text)

# 6. Análise de Sentimento (Geração da Métrica)
def get_textblob_sentiment(text):
    return TextBlob(text).sentiment.polarity

df_clean['score_sentimento'] = df_clean['texto_limpo'].apply(get_textblob_sentiment)

# 7. Adicionar metadado de processamento e salvar na Silver Layer
df_clean['data_processamento'] = datetime.now()

# Salvar na camada Silver (com TODOS os cabeçalhos para o script Load)
df_clean.to_csv(SILVER_PATH, index=False)

print(f"\nDados Silver salvos com sucesso: {df_clean.shape[0]} linhas")