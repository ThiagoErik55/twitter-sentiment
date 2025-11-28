from sqlalchemy import create_engine # <-- Mantenha esta linha no topo!

def get_connection_string():
    
    USER = "postgres"      
    PASSWORD = "123456"
    HOST = "localhost"     
    PORT = "5432"          
    DATABASE = "pipeline_db" 
    
    return f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

def get_engine():
    return create_engine(get_connection_string())
