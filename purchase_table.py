import pandas as pd
import pandas_gbq as pd_gbq
import json
from google.auth import credentials
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
from dotenv import load_dotenv
from google.oauth2 import service_account

# Load the variables from the .env-file
load_dotenv()

# Define the mail variables
smtp_server = os.environ.get('MAIL_SMTP_SERVER')
smtp_port = os.environ.get('MAIL_SMTP_PORT')
sender_email = os.environ.get('MAIL_SENDER_EMAIL')
sender_password = os.environ.get('MAIL_SENDER_PASSWORD')
recipient_mail = 'mgsrood@gmail.com'

# Verstuur een e-mail met het Excel-bestand als bijlage
def send_email(recipient_mail, smtp_server, smtp_port, sender_email, sender_password, error_message):
    # Configure the email
    smtp_server = smtp_server
    smtp_port = smtp_port
    sender_email = sender_email
    sender_password = sender_password
    recipient_email = recipient_mail

    # Define the subject
    email_subject = f"""
    Fout bij upload Bigquery van Informer purchase data
    """

    # Define the text
    email_text = f"""
    Goedemorgen,

    Er is net een fout opgetreden bij het uploaden van purchase data naar bigquery vanuit Google Function.
    Dit is de foutmelding: {error_message}

    Mooie dag!

    Groet,
    Jezelf
    """

    # Bericht samenstellen
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = email_subject
    message.attach(MIMEText(email_text, 'plain'))

    # Verbinding maken met de SMTP-server en e-mail verzenden
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, message.as_string())

    print('E-mail verzonden')

# Define the Informer API variables
api_url = os.environ.get('INFORMER_URL')
Apikey = os.environ.get('INFORMER_API_KEY')
Securitycode = os.environ.get('INFORMER_SECURITY_CODE')

# Define headers
headers = {
    'accept': 'application/json',
    'Securitycode': Securitycode,
    'Apikey': Apikey
}

# Define purchase endpoint
purchase_endpoint = "invoices/purchase/?records=1000&page="

# Create an empty DataFrame to store the data
df = pd.DataFrame()

# Loop through pages 0, 1, and 2
for page in range(3):
    # Define url for the current page
    purchase_url = api_url + purchase_endpoint + str(page)

    # Make the GET request to the API with headers
    purchase_response = requests.get(purchase_url, headers=headers)

    # Check if the response status code is 200 (OK)
    if purchase_response.status_code == 200:
        purchase_data = purchase_response.json()

        for invoice_id, purchase_item in purchase_data['purchase'].items():
            # Create a dictionary with the data
            data_dict = {
                'datum': purchase_item['date'],
                'relatie_nummer': purchase_item['relation_id'],
                'factuur_nummer': purchase_item['number'],
                'vervaldatum': purchase_item['expiry_date'],
                'prijs_excl_btw': purchase_item['total_price_excl_vat'],
                'prijs_incl_btw': purchase_item['total_price_incl_vat'],
                'bedrag_voldaan': purchase_item['paid'],
                'betaal_datum': purchase_item['payment_date'],
                'factuur_id': invoice_id  
            }
            
            # Append the data to the DataFrame
            df = df._append(data_dict, ignore_index=True)

    else:
        print(f"Error: {purchase_response.status_code} - {purchase_response.text}")

# Define relation endpoint
relation_endpoint = "relations/?records=1000"
relation_url = api_url + relation_endpoint

# Make the GET request to the API with headers
relation_response = requests.get(relation_url, headers=headers)

if relation_response.status_code == 200:
    relation_data = relation_response.json()

    data = []
    for relation_id, values in relation_data['relation'].items():
        data.append({'relatie_nummer': relation_id, 'bedrijfsnaam': values['company_name']})

    # Create a DataFrame
    df_2 = pd.DataFrame(data)

else:
    print(f"Error: {relation_response.status_code} - {relation_response.text}")

# Merge the two DataFrames
purchase_df = df.merge(df_2, on='relatie_nummer', how='left')

# Reorder DataFrame
desired_columns = [
    'datum',
    'relatie_nummer',
    'factuur_nummer',
    'vervaldatum',
    'prijs_excl_btw',
    'prijs_incl_btw',
    'bedrag_voldaan',
    'betaal_datum',
    'bedrijfsnaam',
    'factuur_id'
]

purchase_df = purchase_df[desired_columns]

# BigQuery information
project_id_2 = os.environ.get('PURCHASE_PROJECT_ID')
dataset_id = os.environ.get('PURCHASE_DATASET_ID')
table_id = os.environ.get('PURCHASE_TABLE_ID')

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

try:
    # Voer de query uit en laad de resultaten in een DataFrame
    df_gbq = pd_gbq.to_gbq(purchase_df, destination_table=f'{project_id_2}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
    print("Upload naar BigQuery succesvol voltooid!")
except Exception as e:
    error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"
    send_email(recipient_mail, smtp_server, smtp_port, sender_email, sender_password, error_message)