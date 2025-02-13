from pandas.errors import OutOfBoundsDatetime
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
import numpy as np
import logging


balance_sheet = {
    'Datum': 'date',
    'Periode': 'int',
    'Jaar': 'int',
    'Grootboekrekening': 'nvarchar',
    'Grootboekrekening_nummer': 'int',
    'Factuur_nummer': 'nvarchar',
    'Lijn_nummer': 'nvarchar',
    'Type': 'nvarchar',
    'Bedrijfsnaam': 'nvarchar',
    'Beschrijving': 'nvarchar',
    'Lijn_beschrijving': 'nvarchar',
    'Debit': 'float',
    'Credit': 'float',
}

sales = {
    'Datum': 'date',
    'Relatie_nummer': 'int',
    'Factuur_nummer': 'nvarchar',
    'Vervaldatum': 'date',
    'Referentie': 'nvarchar',
    'Prijs_excl_btw': 'float',
    'Prijs_incl_btw': 'float',
    'Bedrag_voldaan': 'float',
    'Betaal_datum': 'date',
    'Bedrijfsnaam': 'nvarchar',
    'Factuur_id': 'nvarchar',
    'Regel_id': 'nvarchar',
    'Omschrijving': 'nvarchar',
    'Aantal': 'float',
    'Prijs_per_stuk': 'float',
    'Btw_percentage': 'float',
    'Grootboek_id': 'nvarchar',
}

purchase = {
    'Datum': 'date',
    'Relatie_nummer': 'int',
    'Factuur_nummer': 'nvarchar',
    'Vervaldatum': 'date',
    'Prijs_excl_btw': 'float',
    'Prijs_incl_btw': 'float',
    'Bedrag_voldaan': 'float',
    'Betaal_datum': 'date',
    'Bedrijfsnaam': 'nvarchar',
    'Factuur_id': 'nvarchar',
    'Regel_id': 'nvarchar',
    'Omschrijving': 'nvarchar',
    'Bedrag': 'float',
    'Btw_percentage': 'float',
    'Grootboek_id': 'nvarchar',
}


def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)

    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                # Vervang None-waarden met een standaardwaarde voordat je de conversie uitvoert
                if dtype == 'int':
                    # Zet niet-numerieke waarden om naar NaN en vul None in met 0
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)

                    # Controleer of de waarden binnen het bereik van SQL Server's INT vallen
                    min_int = -2147483648
                    max_int = 2147483647
                    df[column] = df[column].apply(
                        lambda x: x if min_int <= x <= max_int else None
                    )
                elif dtype == 'nvarchar':
                    df[column] = df[column].fillna('').astype(str)  # Vervang None door lege string
                elif dtype == 'decimal':
                    # Hier wordt de precisie ingesteld
                    df[column] = df[column].apply(
                        lambda x: Decimal(str(x)).quantize(Decimal('1.00'), rounding=ROUND_HALF_UP) 
                        if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else None
                    )
                elif dtype == 'float':
                    df[column] = df[column].apply(
                        lambda x: float(x) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else None
                    )
                    
                    # Vervang NaN door NULL (None in Python)
                    df[column] = df[column].apply(
                        lambda x: None if isinstance(x, float) and np.isnan(x) else x
                    )

                    # Zorg ervoor dat de float binnen een specifiek bereik valt, bijvoorbeeld voor SQL Server
                    df[column] = df[column].apply(
                        lambda x: x if x is None or (-1.79e+308 <= x <= 1.79e+308) else None
                    )
                elif dtype == 'bit':
                    df[column] = df[column].apply(
                        lambda x: True if x in [-1, 'Ja', 'ja', 'YES', 'yes'] else False if x in [0, 'Nee', 'nee', 'NO', 'no'] else x
                    ).fillna(False)
                elif dtype == 'date' or dtype == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce')

                    # SQL Server's datetime heeft een bereik van 1753-01-01 tot 9999-12-31
                    min_date = pd.Timestamp('1753-01-01')
                    max_date = pd.Timestamp('9999-12-31')

                    df[column] = df[column].apply(lambda x: x if pd.isna(x) or (min_date <= x <= max_date) else None)

                    # Ongeldige datums zoals '0000-00-00' omzetten naar None
                    df[column] = df[column].apply(lambda x: None if str(x) == '0000-00-00' else x)
                elif dtype == 'time':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.time
                    df[column] = df[column].fillna(pd.NaT)

                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
            except OutOfBoundsDatetime:
                # Als er een 'OutOfBoundsDatetime' fout is (zoals een datum buiten het bereik van SQL Server),
                # zet dan die waarde op NaT.
                df[column] = pd.NaT
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

def apply_conversion(df, tabelnaam):
    column_typing = {
        'Balans': balance_sheet,
        'Verkoop': sales,
        'Inkoop': purchase,
    }

    # Update typing van kolommen
    for typing_table, typing in column_typing.items():
        if tabelnaam == typing_table:
            
            # Type conversie
            try:
                converted_df = convert_column_types(df, typing)
                logging.info(f"Kolommen type conversie")
                
                return converted_df
            except Exception as e:
                logging.error(f"Kolommen type conversie mislukt: {e}")
                
            