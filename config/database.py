from sqlalchemy import create_engine

def get_connection_string():
    
    USER = "airflow"      
    PASSWORD = "airflow"
    HOST = "postgres"     
    PORT = "5432"          
    DATABASE = "airflow" 
    
    return f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

def get_engine():
    return create_engine(get_connection_string())