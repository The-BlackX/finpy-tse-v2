import requests
import pandas as pd
import os
import jdatetime

def get_orderbook_me(output="dataframe", filename="OrderBook", add_timestamp=False, save_path=None):
    # Set headers for requests
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.39'}
    
    # Fetch market watch and order book data
    r = requests.get('http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx', headers=headers)
    main_text = r.text
    
    # Extract Symbol and WEB-ID from market watch data
    Mkt_df = pd.DataFrame((main_text.split('@')[2]).split(';'))
    Mkt_df = Mkt_df[0].str.split(",", expand=True)
    Mkt_df = Mkt_df.iloc[:, :23]  # Just need first few columns
    Mkt_df.columns = ['WEB-ID', 'Ticker-Code', 'Symbol', 'Name', 'Time', 'Open', 'Final', 'Close', 'Count', 'Volume', 'Value',
                      'Low', 'High', 'Yesterday', 'EPS', 'Base_Vol', 'Unk1', 'Unk2', 'Sector', 'Day_UL', 'Day_LL', 'Share_No', 'Mkt-ID']
    Mkt_df = Mkt_df[['WEB-ID', 'Symbol']]
    Mkt_df['Symbol'] = Mkt_df['Symbol'].apply(lambda x: str(x).replace('ي', 'ی').replace('ك', 'ک'))
    Mkt_df['WEB-ID'] = Mkt_df['WEB-ID'].apply(lambda x: x.strip())
    Mkt_df = Mkt_df.set_index('WEB-ID')

    # Process order book data
    OB_df = pd.DataFrame((main_text.split('@')[3]).split(';'))
    OB_df = OB_df[0].str.split(",", expand=True)
    OB_df.columns = ['WEB-ID', 'OB-Depth', 'Sell-No', 'Buy-No', 'Buy-Price', 'Sell-Price', 'Buy-Vol', 'Sell-Vol']
    OB_df['WEB-ID'] = OB_df['WEB-ID'].apply(lambda x: x.strip())
    OB_df = OB_df.set_index('WEB-ID')
    
    # Format columns
    cols = ['Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No']
    OB_df[cols] = OB_df[cols].apply(pd.to_numeric, axis=1)
    OB_df['OB-Depth'] = OB_df['OB-Depth'].astype(int)

    # Merge with Symbol
    final_df = OB_df.join(Mkt_df).reset_index(drop=False)
    
    # Set column order
    columns_order = ['WEB-ID', 'Symbol', 'OB-Depth', 'Sell-No', 'Buy-No', 'Buy-Price', 'Sell-Price', 'Buy-Vol', 'Sell-Vol']
    final_df = final_df[columns_order]

    # Save to Excel if output is 'excel'
    if output == "excel":
        if save_path is None:
            save_path = os.getcwd()
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        
        excel_filename = filename
        if add_timestamp:
            j_now = jdatetime.datetime.now()
            timestamp = j_now.strftime('%Y-%m-%d_%H-%M-%S')  # e.g., 1404-01-20_14-20-38
            excel_filename = f"{filename}_{timestamp}"
        
        excel_file = os.path.join(save_path, f"{excel_filename}.xlsx")
        final_df.to_excel(excel_file, index=False)
        print(f"File saved at: {excel_file}")

    return final_df