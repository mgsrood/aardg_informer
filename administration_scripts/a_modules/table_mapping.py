import logging

balance_sheet = {
    'date': 'datum',
    'period': 'maand',
    'year': 'jaar',
    'grootboekrekening': 'grootboekrekening',
    'grootboekrekening_nummer': 'grootboekrekening_nummer',
    'invoice_id': 'factuur_nummer',
    'number': 'lijn_nummer',
    'type': 'type',
    'name': 'bedrijfsnaam',
    'entry_description': 'beschrijving',
    'line_description': 'lijn_beschrijving',
    'debit': 'debit',
    'credit': 'credit',
}

balans_kolommen = [
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

sales = {
    'datum': 'datum',
    'relatie_nummer': 'relatie_nummer',
    'factuur_nummer': 'factuur_nummer',
    'vervaldatum': 'vervaldatum',
    'prijs_excl_btw': 'prijs_excl_btw',
    'prijs_incl_btw': 'prijs_incl_btw',
    'bedrag_voldaan': 'bedrag_voldaan',
    'betaal_datum': 'betaal_datum',
    'bedrijfsnaam': 'bedrijfsnaam',
    'factuur_id': 'factuur_id',
}

sales_kolommen = [
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

purchase = {
    'datum': 'datum',
    'relatie_nummer': 'relatie_nummer',
    'factuur_nummer': 'factuur_nummer',
    'vervaldatum': 'vervaldatum',
    'prijs_excl_btw': 'prijs_excl_btw',
    'prijs_incl_btw': 'prijs_incl_btw',
    'bedrag_voldaan': 'bedrag_voldaan',
    'betaal_datum': 'betaal_datum',
    'bedrijfsnaam': 'bedrijfsnaam',
    'factuur_id': 'factuur_id',
}

purchase_kolommen = [
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

hours = {
    'datum': 'Datum',
    'bedrijfsnaam': 'Bedrijfsnaam',
    'factuur_nummer': 'Factuur_nummer',
    'referentie': 'Referentie',
    'omschrijving': 'Omschrijving',
    'aantal': 'Aantal',
}

hours_kolommen = [
    'Aantal',
    'Omschrijving',
    'Datum',
    'Bedrijfsnaam',
    'Factuur_nummer',
    'Referentie',
]

def transform_columns(df, column_mapping):
    # Controleer of de DataFrame leeg is
    
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def apply_mapping(df, tabelnaam):
    # Kolom mappin
    column_mapping = {
        'Balans': balance_sheet,
        'Verkoop': sales,
        'Inkoop': purchase,
        'Uren': hours
    }

    # Tabel mapping
    for mapping_table, mapping in column_mapping.items():
        if tabelnaam == mapping_table:

            # Transformeer de kolommen
            try:
                transformed_df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                
                return transformed_df
            except Exception as e:
                logging.error(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                
def select_columns(df, tabelnaam):
    # Kolom selectie
    column_selection = {
        'Balans': balans_kolommen,
        'Verkoop': sales_kolommen,
        'Inkoop': purchase_kolommen,
        'Uren': hours_kolommen
    }
    
    for selection_table, selection in column_selection.items():
        if tabelnaam == selection_table:
            return df[selection]
            
            