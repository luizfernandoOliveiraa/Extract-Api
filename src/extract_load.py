#import libs

import yfinance as yf
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

commodities = [ 'GC=F', 'SI=F', 'ZN=F', 'ES=F']

def collect_data_api(commodities, periodo="1y", intervalo = "1d"):
    ticker = yf.Ticker(commodities)
    data = ticker.history(period=periodo, interval=intervalo)[['Close']]
    data['ticker'] = commodities
    return data


def concat_df(commodities):
    all_data = []
    for commodity in commodities:
        data = collect_data_api(commodity)
        all_data.append(data)
    return pd.concat(all_data, axis=0).reset_index()

def save_to_db(df, schema='public', table_name='commodities'):
    load_dotenv()
    user = os.getenv('DB_USER_PROD')
    password = os.getenv('DB_PASS_PROD')
    host = os.getenv('DB_HOST_PROD')
    port = os.getenv('DB_PORT_PROD')
    database = os.getenv('DB_NAME_PROD')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    # Save the DataFrame to the database
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    
if __name__ == "__main__":
    search_data = concat_df(commodities)
    save_to_db(search_data, schema='public', table_name='commodities')
    print("Data collected and saved to database successfully.")
    
    
    