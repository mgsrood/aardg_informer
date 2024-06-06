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
    Fout bij upload Bigquery van Informer balance sheet data
    """

    # Define the text
    email_text = f"""
    Goedemorgen,

    Er is net een fout opgetreden bij het uploaden van balance sheet data naar bigquery vanuit Google Function.
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

# Define ledger account endpoint
ledgers_endpoint = "ledgers/"
ledgers_url = api_url + ledgers_endpoint

# Make the GET request to the API with headers
ledgers_response = requests.get(ledgers_url, headers=headers)

if ledgers_response.status_code == 200:
    ledgers_data = ledgers_response.json()

    data = []
    for ledgers, values in ledgers_data['ledgers'].items():
        data.append({'grootboekrekening_nummer': ledgers, 'grootboekrekening': values['description']})

    # Create a DataFrame
    df_1 = pd.DataFrame(data)

else:
    print(f"Error: {ledgers_response.status_code} - {ledgers_response.text}")

# Creëer een lege lijst om de gegevensframes per grootboekrekeningnummer op te slaan
df_list = []

# Loop over elk grootboekrekeningnummer
for grootboekrekeningnummer in df_1['grootboekrekening_nummer']:
    # Bouw de URL voor de specifieke oproep om alle gegevens voor de hele periode op te halen
    ledger_url = f"https://api.informer.eu/v1/reports/ledger/?ledger_id={grootboekrekeningnummer}&year_from=2018&year_to=2024&period_from=1&period_to=12"

    # Maak de GET request naar de API
    ledger_response = requests.get(ledger_url, headers=headers)

    if ledger_response.status_code == 200:
        ledger_entries = ledger_response.json().get('ledger_entries', [])
        df_temp = pd.DataFrame(ledger_entries)
        df_temp['grootboekrekening_nummer'] = grootboekrekeningnummer
        df_list.append(df_temp)

    else:
        print(f"Fout bij het ophalen van gegevens voor grootboekrekeningnummer {grootboekrekeningnummer}")

# Combineer alle gegevensframes in df_2
if df_list:
    df_2 = pd.concat(df_list, ignore_index=True)

# Nu heb je df_2 met de gewenste gegevens per grootboekrekeningnummer voor de hele periode

# Voeg hier het stukje code toe om 'relation_details' op te splitsen in 'id' en 'name'
if 'relation_details' in df_2.columns:
    df_2['id'] = df_2['relation_details'].apply(lambda x: x['id'] if isinstance(x, dict) and 'id' in x else None)
    df_2['name'] = df_2['relation_details'].apply(lambda x: x['name'] if isinstance(x, dict) and 'name' in x else None)

# Verwijder de oorspronkelijke 'relation_details' kolom
df_2.drop(columns=['relation_details'], inplace=True)

# Merge the two dataframes
df_balance_sheet = df_2.merge(df_1, on='grootboekrekening_nummer', how='left')

# Rename, select and order columns DataFrame
desired_column_names = {
    'date': 'datum',
    'period': 'maand',
    'year': 'jaar',
    'invoice_id': 'factuur_nummer',
    'number': 'lijn_nummer',
    'name': 'bedrijfsnaam',
    'entry_description': 'beschrijving',
    'line_description': 'lijn_beschrijving'
}

df_balance_sheet = df_balance_sheet.rename(columns=desired_column_names)

desired_columns = [
    'datum',
    'maand',
    'jaar',
    'grootboekrekening',
    'grootboekrekening_nummer',
    'factuur_nummer',
    'lijn_nummer',
    'type',
    'debit',
    'credit',
    'bedrijfsnaam',
    'beschrijving',
    'lijn_beschrijving'
]

df_balance_sheet = df_balance_sheet[desired_columns]
df_balance_sheet = df_balance_sheet.astype(str)

# BigQuery information
project_id_2 = os.environ.get('BALANCE_PROJECT_ID')
dataset_id = os.environ.get('BALANCE_DATASET_ID')
table_id = os.environ.get('BALANCE_TABLE_ID')

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

try:
    # Voer de query uit en laad de resultaten in een DataFrame
    df_gbq = pd_gbq.to_gbq(df_balance_sheet, destination_table=f'{project_id_2}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
    print("Upload naar BigQuery succesvol voltooid!")
except Exception as e:
    error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"
    send_email(recipient_mail, smtp_server, smtp_port, sender_email, sender_password, error_message)

# Test