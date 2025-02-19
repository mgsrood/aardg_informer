import pandas as pd
import requests
import os
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# Load the variables from the .env-file
load_dotenv()

# Load GCP environment variables
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

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

# Define sales endpoint
sales_endpoint = "invoices/sales/?records=1000&page="

# Create an empty DataFrame to store the data
df = pd.DataFrame()

# Loop through 2 pages (for testing, change to 20 for full extraction)
for page in range(2):
    # Define url for the current page
    sales_url = api_url + sales_endpoint + str(page)

    # Make the GET request to the API with headers
    sales_response = requests.get(sales_url, headers=headers, timeout=120)

    # Check if the response status code is 200 (OK)
    if sales_response.status_code == 200:
        sales_data = sales_response.json()

        if 'sales' in sales_data:
            for invoice_id, sales_item in sales_data['sales'].items():
                # Extract relevant data from sales_item
                date = sales_item['date']
                relation_id = sales_item['relation_id']
                number = sales_item['number']

                # Iterate over line items if present
                if 'line' in sales_item:
                    for line_id, line_item in sales_item['line'].items():
                        # Create a dictionary with the combined data
                        data_dict = {
                            'Datum': date,
                            'relatie_nummer': relation_id,
                            'Factuur_nr': number,
                            'Aantal': line_item['qty'],
                            'Omschrijving': line_item['description'],
                            'Bedrag': line_item['amount']
                        }
                        # Append the data to the DataFrame
                        df = df._append(data_dict, ignore_index=True)

# Define relation endpoint
relation_endpoint = "relations/?records=1000"
relation_url = api_url + relation_endpoint

# Make the GET request to the API with headers
relation_response = requests.get(relation_url, headers=headers, timeout=120)

if relation_response.status_code == 200:
    relation_data = relation_response.json()

    data = []
    for relation_id, values in relation_data['relation'].items():
        data.append({'relatie_nummer': relation_id, 'Bedrijfsnaam': values['company_name']})

    # Create a DataFrame
    df_2 = pd.DataFrame(data)

else:
    print(f"Error: {relation_response.status_code} - {relation_response.text}")

# Merge the two DataFrames
sales_df = df.merge(df_2, on='relatie_nummer', how='left')

# Functie om LOT-nummer en T.H.T.-datum te extraheren
def extract_lot_and_tht(description):
    # Zoek naar LOT-nummer
    lot_match = re.search(r'LOT(?:NUMMER)?:?\s*(\S+)', description, re.IGNORECASE)
    lotnummer = lot_match.group(1) if lot_match else None

    # Zoek naar T.H.T.-datum
    tht_match = re.search(r'T\.?H\.?T(?:\.)?:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})', description, re.IGNORECASE)
    if tht_match:
        tht = tht_match.group(1).replace('/', '-')  # Formaat aanpassen naar dd-mm-jjjj
    else:
        tht = None

    return lotnummer, tht

# Pas de functie toe op de 'lijn_beschrijving' kolom
sales_df[['Lotnummer', 'THT']] = sales_df['Omschrijving'].apply(lambda x: pd.Series(extract_lot_and_tht(x)))


def determine_base_product(row):
        omschrijving = row['Omschrijving']

        if omschrijving is None:
            return 'unknown'

        omschrijving = row['Omschrijving'].lower()

        if 'kombucha' in omschrijving and ('citroen' not in omschrijving and 'bloem' not in omschrijving):
            return 'Kombucha'
        elif 'citroen' in omschrijving:
            return 'Citroen'
        elif 'bloem' in omschrijving:
            return 'Bloem'
        elif 'waterkefir' in omschrijving:
            return 'Waterkefir'
        elif 'frisdrank' in omschrijving:
            return 'Frisdrank Mix'
        elif 'mix' in omschrijving and 'frisdrank' not in omschrijving:
            return 'Mix Originals'
        elif 'starter' in omschrijving or 'introductie' in omschrijving:
            return 'Starter Box'
        elif 'gember' in omschrijving:
            return 'Gember'
        elif 'probiotica' in omschrijving:
            return 'Probiotica'
        else:
            return 'unknown'

# Voeg de nieuwe kolom 'base_product' toe aan de DataFrame
sales_df['Product'] = sales_df.apply(determine_base_product, axis=1)

sales_df = sales_df.astype(str)

# Limit to desired columns
desired_columns = [
    'Datum',
    'Bedrijfsnaam',
    'Factuur_nr',
    'Aantal',
    'Product',
    'Lotnummer',
    'THT',
    'Omschrijving',
    'Bedrag'
]

df = sales_df[desired_columns]

# Limit to Holland & Barrett
df = df[df['Bedrijfsnaam'] == 'Holland and Barrett B.V.']

# Limit to desired rows
df = df[df['Product'] != 'unknown']

def upload_to_google_sheet(df, sheet_name, worksheetname):
    # Verbind met Google Sheets API
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gc_keys, scope)
    gc = gspread.authorize(credentials)

    # Open het Google Sheet
    spreadsheet = gc.open(sheet_name)

    # Selecteer het werkblad
    worksheet = spreadsheet.worksheet(worksheetname)

    # Converteer de DataFrame naar een lijst met lijsten
    data = df.values.tolist()

    # Voeg de data toe aan het werkblad    
    for row in data:
        worksheet.append_row(row, value_input_option='RAW')

    print(f"Gegevens succesvol geüpload naar Google Sheet '{sheet_name}'.")


# Testing with an example
if __name__ == "__main__":
    # Google Sheet details
    sheet_name = '3. Verkoop 2024'
    worksheetname = 'Directe Verkoop'

    # Verwerk alle PDF-bestanden in de directory
    upload_to_google_sheet(df, sheet_name, worksheetname)