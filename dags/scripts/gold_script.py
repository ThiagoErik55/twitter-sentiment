import pandas as pd

df_silver = pd.read_csv('/opt/airflow/data/silver/dados_limpos.csv')
df_silver['data_postagem'] = pd.to_datetime(df_silver['data_postagem'])
df_silver['score_sentimento'] = df_silver['score_sentimento'].astype(float) 

print(f"Dados Silver lidos: {df_silver.shape[0]} registros")

# A. Extrair a data (sem hora) e a hora do post para agrupamento
df_silver['data_pura'] = df_silver['data_postagem'].dt.date
df_silver['hora'] = df_silver['data_postagem'].dt.hour

# B. Criar a tabela de métricas diárias
metricas_diarias = df_silver.groupby('data_pura').agg(
    # 1. Total de Posts (Volume)
    total_posts=('score_sentimento', 'count'), 
    # 2. Sentimento Médio Diário (KPI Chave)
    sentimento_medio=('score_sentimento', 'mean'),
    # 3. Sentimento Máximo e Mínimo (Para análise de picos de emoção)
    sentimento_max=('score_sentimento', 'max'),
    sentimento_min=('score_sentimento', 'min')
).reset_index()

# C. Renomear e formatar para o Power BI
metricas_diarias.columns = ['data_metrica', 'total_posts', 
                            'sentimento_medio', 'sentimento_max', 'sentimento_min']
metricas_diarias['data_metrica'] = pd.to_datetime(metricas_diarias['data_metrica'])

print("\nAgregação 1: Métricas Diárias")
print(metricas_diarias.head())

# Criar a tabela de volume por hora
volume_por_hora = df_silver.groupby('hora').agg(
    total_posts=('score_sentimento', 'count'),
    sentimento_medio=('score_sentimento', 'mean')
).reset_index()

# Renomear e formatar
volume_por_hora.columns = ['hora_dia', 'total_posts', 'sentimento_medio']

print("\nAgregação 2: Volume por Hora do Dia")
print(volume_por_hora.head())

metricas_diarias.to_csv('/opt/airflow/data/gold/metricas_diarias_sentimento.csv', index=False)

volume_por_hora.to_csv('/opt/airflow/data/gold/volume_por_hora.csv', index=False)

print("\nDados agregados criados e salvos com sucesso na Camada Gold.")