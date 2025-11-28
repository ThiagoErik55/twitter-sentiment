import pandas as pd
import numpy as np
import re
from datetime import datetime
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob 

df = pd.read_csv('/opt/airflow/data/bronze/dados_brutos.csv', header=None)
print(f"Dados originais (Bronze): {df.shape[0]} linhas")

df.columns = ['Sentimento_Bruto', 'ID', 'Data_Bruta', 'FLAG', 'USER', 'Texto_Bruto', 'data_ingestao', 'fonte_arquivo']

TEXT_COL = 'Texto_Bruto' 
SENTIMENT_COL = 'Sentimento_Bruto'

# Filtrar colunas relevantes e renomear para nomes limpos
df_clean = df[['Sentimento_Bruto', 'Data_Bruta', 'Texto_Bruto']].copy()
df_clean.columns = ['sentimento_label', 'data_postagem', 'texto_original']

df_clean['data_postagem'] = pd.to_datetime(df_clean['data_postagem'], errors='coerce')
df_clean['sentimento_binario'] = df_clean['sentimento_label'].apply(lambda x: 1 if x == 4 else 0)

# C. Remoção de Duplicatas e Nulos
linhas_antes = len(df_clean)
df_clean.drop_duplicates(inplace=True)
df_clean.dropna(inplace=True) 
linhas_depois = len(df_clean)

print(f"\nLinhas após limpeza estrutural: {linhas_depois}. Duplicatas/Nulos removidos: {linhas_antes - linhas_depois}")

def clean_text(text):
    text = str(text)
    
    text = re.sub(r'http\S+|www.\S+', '', text)
    
    text = re.sub(r'@\w+|#\w+', '', text)
    
    text = re.sub(r'[^\w\s]', '', text).lower()
    
    return text.strip()

df_clean['texto_limpo'] = df_clean['texto_original'].apply(clean_text)

print("\nTexto Original vs. Texto Limpo:")
print(df_clean[['texto_original', 'texto_limpo']].head())

# O score vai de -1 (muito negativo) a +1 (muito positivo)
def get_textblob_sentiment(text):
    return TextBlob(text).sentiment.polarity

# Criar a coluna métrica principal (Polaridade)
df_clean['score_sentimento'] = df_clean['texto_limpo'].apply(get_textblob_sentiment)

print("\nMétrica de Sentimento Gerada:")
print(df_clean[['texto_limpo', 'score_sentimento']].head())

# Adicionar metadado de processamento
df_clean['data_processamento'] = datetime.now()

# Salvar na camada Silver
output_path = '/opt/airflow/data/silver/dados_limpos.csv'
df_clean.to_csv(output_path, index=False)

print(f"\nDados Silver salvos com sucesso: {df_clean.shape[0]} linhas")