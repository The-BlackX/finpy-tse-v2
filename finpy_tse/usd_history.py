# finpy_tse/usd_history.py
import pandas as pd
import os

def Get_USD_History():
    """
    Load historical USD data from the embedded usd_history.csv file.
    Returns a DataFrame with historical USD data.
    """
    file_path = os.path.join(os.path.dirname(__file__), 'data', 'usd_history.csv')
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df
    except Exception as e:
        raise Exception(f"Error loading USD history data: {e}")
