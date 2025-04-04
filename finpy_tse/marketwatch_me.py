import requests
import pandas as pd
import os
import jdatetime

def get_marketwatch_me(output="dataframe", filename="MarketWatch", add_timestamp=False, save_path=None):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.39'}
    
    # Market watch data
    r = requests.get('http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx', headers=headers)
    main_text = r.text
    Mkt_df = pd.DataFrame((main_text.split('@')[2]).split(';'))
    Mkt_df = Mkt_df[0].str.split(",", expand=True)
    Mkt_df = Mkt_df.iloc[:, :23]
    Mkt_df.columns = ['WEB-ID', 'Ticker-Code', 'Symbol', 'Name', 'Time', 'Open', 'Final', 'Close', 'Count', 'Volume', 'Value',
                      'Low', 'High', 'Yesterday', 'EPS', 'Base_Vol', 'Unk1', 'Unk2', 'Sector', 'Day_UL', 'Day_LL', 'Share_No', 'Market']
    
    # Order book data (depth 1)
    OB_df = pd.DataFrame((main_text.split('@')[3]).split(';'))
    OB_df = OB_df[0].str.split(",", expand=True)
    OB_df.columns = ['WEB-ID', 'OB-Depth', 'Sell-No', 'Buy-No', 'Buy-Price', 'Sell-Price', 'Buy-Vol', 'Sell-Vol']
    OB_df = OB_df[OB_df['OB-Depth'] == '1']
    for col in ['Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No']:
        OB_df[col] = pd.to_numeric(OB_df[col], errors='coerce')
    OB_df['WEB-ID'] = OB_df['WEB-ID'].apply(lambda x: x.strip())
    
    # Merge market and OB data
    Mkt_df['WEB-ID'] = Mkt_df['WEB-ID'].apply(lambda x: x.strip())
    Mkt_df['Symbol'] = Mkt_df['Symbol'].apply(lambda x: str(x).replace('ي', 'ی').replace('ك', 'ک'))
    Mkt_df = Mkt_df.set_index('WEB-ID')
    OB_df = OB_df.set_index('WEB-ID')
    final_df = Mkt_df.join(OB_df).reset_index(drop=False)
    
    numeric_cols = ['Open', 'Final', 'Close', 'Count', 'Volume', 'Value', 'Low', 'High', 'Yesterday', 'EPS', 
                    'Base_Vol', 'Share_No', 'Day_UL', 'Day_LL']
    final_df[numeric_cols] = final_df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # Client type data
    r = requests.get('http://old.tsetmc.com/tsev2/data/ClientTypeAll.aspx', headers=headers)
    Client_df = pd.DataFrame(r.text.split(';'))
    Client_df = Client_df[0].str.split(",", expand=True)
    Client_df.columns = ['WEB-ID', 'R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol', 
                         'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol']
    Client_df['WEB-ID'] = Client_df['WEB-ID'].apply(lambda x: x.strip())
    Client_df = Client_df.set_index('WEB-ID')
    for col in ['R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol', 'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol']:
        Client_df[col] = pd.to_numeric(Client_df[col], errors='coerce')
    final_df = final_df.join(Client_df).reset_index(drop=False)
    
    # Add Download
    final_df['Download'] = jdatetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Add Trade_Type
    final_df['Trade_Type'] = final_df['Symbol'].apply(lambda x: 'تابلو' if ((not x[-1].isdigit()) or (x in ['انرژی1', 'انرژی2', 'انرژی3'])) 
                                             else ('بلوکی' if x[-1] == '2' else ('عمده' if x[-1] == '4' else ('جبرانی' if x[-1] == '3' else 'تابلو'))))
    
    # Column order
    columns_order = [
        'Symbol', 'Download', 'Open', 'High', 'Low', 'Close', 'Final', 'Yesterday', 'Day_UL', 'Day_LL',
        'Count', 'Volume', 'Value', 'R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol',
        'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol', 'Sell-No', 'Sell-Vol', 'Sell-Price',
        'Buy-Price', 'Buy-Vol', 'Buy-No', 'Time', 'Name', 'Market', 'Sector', 'Trade_Type',
        'EPS', 'Base_Vol', 'Share_No', 'WEB-ID', 'Ticker-Code', 'Unk1', 'Unk2'
    ]
    final_df = final_df[columns_order]
    
    # Save to Excel if requested
    if output == "excel":
        if save_path is None:
            save_path = os.getcwd()
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        excel_filename = filename
        if add_timestamp:
            j_now = jdatetime.datetime.now()
            timestamp = j_now.strftime('%Y-%m-%d_%H-%M-%S')
            excel_filename = f"{filename}_{timestamp}"
        excel_file = os.path.join(save_path, f"{excel_filename}.xlsx")
        final_df.to_excel(excel_file, index=False)
        print(f"File saved at: {excel_file}")

    return final_df
