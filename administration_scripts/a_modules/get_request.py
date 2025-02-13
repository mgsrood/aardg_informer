import pandas as pd
import requests
import logging

def get_balance_sheet_dataframe(api_url, Apikey, Securitycode, jaar):
    
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
    try:
        ledgers_response = requests.get(ledgers_url, headers=headers)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e}")

    if ledgers_response.status_code == 200:
        ledgers_data = ledgers_response.json()

        data = []
        for ledgers, values in ledgers_data['ledgers'].items():
            data.append({'grootboekrekening_nummer': ledgers, 'grootboekrekening': values['description']})

        # Create a DataFrame
        df_1 = pd.DataFrame(data)

    else:
        logging.error(f"{ledgers_response.status_code} - {ledgers_response.text}")

    # Creëer een lege lijst om de gegevensframes per grootboekrekeningnummer op te slaan
    df_list = []

    # Loop over elk grootboekrekeningnummer
    for grootboekrekeningnummer in df_1['grootboekrekening_nummer']:
        # Bouw de URL voor de specifieke oproep om alle gegevens voor de hele periode op te halen
        ledger_url = f"https://api.informer.eu/v1/reports/ledger/?ledger_id={grootboekrekeningnummer}&year_from=2018&year_to={jaar}&period_from=1&period_to=13"

        # Maak de GET request naar de API
        try:
            ledger_response = requests.get(ledger_url, headers=headers)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error: {e}")

        if ledger_response.status_code == 200:
            ledger_entries = ledger_response.json().get('ledger_entries', [])
            df_temp = pd.DataFrame(ledger_entries)
            df_temp['grootboekrekening_nummer'] = grootboekrekeningnummer
            df_list.append(df_temp)

        else:
            logging.error(f"Fout bij het ophalen van gegevens voor grootboekrekeningnummer {grootboekrekeningnummer}")

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
    
    return df_balance_sheet

def get_purchase_dataframe(api_url, Apikey, Securitycode):

    # Define headers
    headers = {
        'accept': 'application/json',
        'Securitycode': Securitycode,
        'Apikey': Apikey
    }

    # Define purchase endpoint
    purchase_endpoint = "invoices/purchase/?records=1000&page="

    # Create an empty list to store the data
    purchase_data_list = []

    page = 0  # Startpagina

    while True:
        # Definieer de URL voor de huidige pagina
        purchase_url = api_url + purchase_endpoint + str(page)

        # Voer de API-aanroep uit met headers
        purchase_response = requests.get(purchase_url, headers=headers)

        # Controleer of de response succesvol is
        if purchase_response.status_code == 200:
            purchase_data = purchase_response.json()

            # Controleer of er nog aankopen zijn, anders stop de loop
            if not purchase_data.get('purchase'):  
                logging.warning("Er zijn geen aankopen meer om te verwerken.")
                break

            # Verwerk de data
            for invoice_id, purchase_item in purchase_data['purchase'].items():
                # Basisgegevens van de inkoopfactuur
                base_data = {
                    'datum': purchase_item['date'],
                    'relatie_nummer': purchase_item['relation_id'],
                    'factuur_nummer': purchase_item['number'],
                    'vervaldatum': purchase_item['expiry_date'],
                    'prijs_excl_btw': purchase_item['total_price_excl_vat'],
                    'prijs_incl_btw': purchase_item['total_price_incl_vat'],
                    'bedrag_voldaan': purchase_item['paid'],
                    'betaal_datum': purchase_item['payment_date'],
                    'factuur_id': invoice_id,
                }

                purchase_data_list.append(base_data)

            # Ga naar de volgende pagina
            page += 1

        else:
            logging.error(f"Error {purchase_response.status_code} - {purchase_response.text}")
            break 

    # Zet de lijst met data om naar een DataFrame
    df = pd.DataFrame(purchase_data_list)

    # Define relation endpoint
    relation_endpoint = "relations/?records=1000"
    relation_url = api_url + relation_endpoint

    # Make the GET request to the API with headers
    relation_response = requests.get(relation_url, headers=headers)

    if relation_response.status_code == 200:
        relation_data = relation_response.json()

        relation_list = []
        for relation_id, values in relation_data['relation'].items():
            relation_list.append({'relatie_nummer': relation_id, 'bedrijfsnaam': values['company_name']})

        # Maak een DataFrame van de relaties
        df_relations = pd.DataFrame(relation_list)

    else:
        logging.error(f"Error {relation_response.status_code} - {relation_response.text}")
        df_relations = pd.DataFrame()  # Lege DataFrame als er een fout is

    # Merge de purchase data met de bedrijfsnamen
    purchase_df = df.merge(df_relations, on='relatie_nummer', how='left')

    return purchase_df

def get_sales_dataframe(api_url, Apikey, Securitycode):
    # Define headers
    headers = {
        'accept': 'application/json',
        'Securitycode': Securitycode,
        'Apikey': Apikey
    }

    # Define sales endpoint
    sales_endpoint = "invoices/sales/?records=1000&page="

    # Create an empty list to store the data
    sales_data_list = []

    page = 0  # Startpagina

    while True:
        # Define URL for the current page
        sales_url = api_url + sales_endpoint + str(page)

        # Make the GET request to the API with headers
        sales_response = requests.get(sales_url, headers=headers)

        # Check if the response status code is 200 (OK)
        if sales_response.status_code == 200:
            sales_data = sales_response.json()

            # Controleer of er nog facturen zijn, anders stop de loop
            if not sales_data.get('sales'):
                logging.warning("Er zijn geen facturen meer om te verwerken.")
                break

            # Verwerk de sales-data
            for invoice_id, sales_item in sales_data['sales'].items():
                # Basisgegevens van de factuur
                base_data = {
                    'datum': sales_item['date'],
                    'relatie_nummer': sales_item['relation_id'],
                    'factuur_nummer': sales_item['number'],
                    'referentie': sales_item['reference'],
                    'vervaldatum': sales_item['expiry_date'],
                    'bedrag_voldaan': sales_item['paid'],
                    'betaal_datum': sales_item['payment_date'],
                    'prijs_excl_btw': sales_item['total_price_excl_vat'],
                    'prijs_incl_btw': sales_item['total_price_incl_vat'],
                    'factuur_id': invoice_id,
                }
                
                sales_data_list.append(base_data)

            # Ga naar de volgende pagina
            page += 1

        else:
            logging.error(f"Error {sales_response.status_code} - {sales_response.text}")
            break 

    # Zet de lijst met data om naar een DataFrame
    df = pd.DataFrame(sales_data_list)

    # Define relation endpoint
    relation_endpoint = "relations/?records=1000"
    relation_url = api_url + relation_endpoint

    # Make the GET request to the API with headers
    relation_response = requests.get(relation_url, headers=headers)

    if relation_response.status_code == 200:
        relation_data = relation_response.json()

        relation_list = []
        for relation_id, values in relation_data['relation'].items():
            relation_list.append({'relatie_nummer': relation_id, 'bedrijfsnaam': values['company_name']})

        # Maak een DataFrame van de relaties
        df_relations = pd.DataFrame(relation_list)

    else:
        logging.error(f"Error {relation_response.status_code} - {relation_response.text}")
        df_relations = pd.DataFrame()  # Lege DataFrame als er een fout is

    # Merge de sales data met de bedrijfsnamen
    sales_df = df.merge(df_relations, on='relatie_nummer', how='left')

    return sales_df

def get_hour_dataframe(api_url, Apikey, Securitycode):
    # Define headers
    headers = {
        'accept': 'application/json',
        'Securitycode': Securitycode,
        'Apikey': Apikey
    }

    # Define sales endpoint
    sales_endpoint = "invoices/sales/?records=1000&page="

    # Create an empty list to store the data
    sales_data_list = []

    page = 0  # Startpagina

    while True:
        # Define URL for the current page
        sales_url = api_url + sales_endpoint + str(page)

        # Make the GET request to the API with headers
        sales_response = requests.get(sales_url, headers=headers)

        # Check if the response status code is 200 (OK)
        if sales_response.status_code == 200:
            sales_data = sales_response.json()

            # Controleer of er nog facturen zijn, anders stop de loop
            if not sales_data.get('sales'):
                logging.warning("Er zijn geen facturen meer om te verwerken.")
                break

            # Verwerk de sales-data
            for invoice_id, sales_item in sales_data['sales'].items():
                # Basisgegevens van de factuur
                base_data = {
                    'datum': sales_item['date'],
                    'relatie_nummer': sales_item['relation_id'],
                    'factuur_nummer': sales_item['number'],
                    'referentie': sales_item['reference'],
                    'vervaldatum': sales_item['expiry_date'],
                    'bedrag_voldaan': sales_item['paid'],
                    'betaal_datum': sales_item['payment_date'],
                    'prijs_excl_btw': sales_item['total_price_excl_vat'],
                    'prijs_incl_btw': sales_item['total_price_incl_vat'],
                    'factuur_id': invoice_id,
                }

                # Verwerk alle factuurregels (lines)
                if 'line' in sales_item:
                    for line_id, line_item in sales_item['line'].items():
                        line_data = {
                            'regel_id': line_id,
                            'product_id': line_item['product_id'],
                            'omschrijving': line_item['description'],
                            'aantal': line_item['qty'],
                            'prijs_per_stuk': line_item['amount'],
                            'korting': line_item['discount'],
                            'btw_percentage': line_item['vat_percentage'],
                            'grootboek_id': line_item['ledger_account_id'],
                        }

                        # Combineer factuurgegevens met regelgegevens
                        combined_data = {**base_data, **line_data}
                        sales_data_list.append(combined_data)

            # Ga naar de volgende pagina
            page += 1

        else:
            logging.error(f"Error {sales_response.status_code} - {sales_response.text}")
            break 

    # Zet de lijst met data om naar een DataFrame
    df = pd.DataFrame(sales_data_list)

    # Define relation endpoint
    relation_endpoint = "relations/?records=1000"
    relation_url = api_url + relation_endpoint

    # Make the GET request to the API with headers
    relation_response = requests.get(relation_url, headers=headers)

    if relation_response.status_code == 200:
        relation_data = relation_response.json()

        relation_list = []
        for relation_id, values in relation_data['relation'].items():
            relation_list.append({'relatie_nummer': relation_id, 'bedrijfsnaam': values['company_name']})

        # Maak een DataFrame van de relaties
        df_relations = pd.DataFrame(relation_list)

    else:
        logging.error(f"Error {relation_response.status_code} - {relation_response.text}")
        df_relations = pd.DataFrame()  # Lege DataFrame als er een fout is

    # Merge de sales data met de bedrijfsnamen
    hour_df = df.merge(df_relations, on='relatie_nummer', how='left')

    return hour_df