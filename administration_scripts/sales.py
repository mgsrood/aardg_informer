from a_modules.table_mapping import apply_mapping, select_columns
from a_modules.get_request import get_sales_dataframe
from a_modules.config import determine_script_id
from a_modules.log import setup_logging, end_log
from google.oauth2 import service_account
from a_modules.env_tool import env_check
from google.auth import credentials
import pandas_gbq as pd_gbq
import pandas as pd
import logging
import time
import os

def main():
    
    env_check()
    
    # Script configuratie
    klant = "Aard'g"
    script = "Verkoop"
    bron = 'Informer'
    tabelnaam = 'Verkoop'
    start_time = time.time()
    
    # Huidig jaar
    jaar = time.strftime("%Y")

    # BigQuery information
    project_id_2 = os.environ.get('SALES_PROJECT_ID')
    dataset_id = os.environ.get('SALES_DATASET_ID')
    table_id = os.environ.get('SALES_TABLE_ID')

    # Get the GCP keys
    gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys
    credentials = service_account.Credentials.from_service_account_file(gc_keys)
    project_id = credentials.project_id
    
    # Informer variabelen
    api_url = os.environ.get('INFORMER_URL')
    Apikey = os.environ.get('INFORMER_API_KEY')
    Securitycode = os.environ.get('INFORMER_SECURITY_CODE')
    
    # Database variabelen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)
    
    try:    
        # Dataframe ophalen
        df = get_sales_dataframe(api_url, Apikey, Securitycode)
        
        # Mapping toepassen
        df = apply_mapping(df, tabelnaam)
        
        # Kolom keuze
        df = select_columns(df, tabelnaam)
        
        # Type conversie
        df = df.fillna("").astype(str)
        
        # Data toeschrijven naar BigQuery
        try:
            # Voer de query uit en laad de resultaten in een DataFrame
            df = pd_gbq.to_gbq(df, destination_table=f'{project_id_2}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
            logging.info("Upload naar BigQuery succesvol voltooid!")
        except Exception as e:
            logging.error(f"Fout bij het uploaden naar BigQuery: {e}")
            
    except Exception as e:
        logging.error(f"Error: {e}")
    
    # Eindtijd logging
    end_log(start_time)
    
if __name__ == "__main__":    
    main()
            