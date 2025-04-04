import requests
import pandas as pd
import os
import jdatetime

def get_marketwatch_me(output="dataframe", filename="MarketWatch", add_timestamp=False, save_path=None):
    # Set headers for requests
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.39'}
    
    # Fetch client type data
    r = requests.get('http://old.tsetmc.com/tsev2/data/ClientTypeAll.aspx', headers=headers)
    Mkt_RI_df = pd.DataFrame(r.text.split(';'))
    Mkt_RI_df = Mkt_RI_df[0].str.split(",", expand=True)
    Mkt_RI_df.columns = ['WEB-ID', 'R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol', 'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol']
    cols = ['R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol', 'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol']
    Mkt_RI_df[cols] = Mkt_RI_df[cols].apply(pd.to_numeric, axis=1)
    Mkt_RI_df['WEB-ID'] = Mkt_RI_df['WEB-ID'].apply(lambda x: x.strip())
    Mkt_RI_df = Mkt_RI_df.set_index('WEB-ID')

    # Fetch market watch and order book data
    r = requests.get('http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx', headers=headers)
    main_text = r.text
    Mkt_df = pd.DataFrame((main_text.split('@')[2]).split(';'))
    Mkt_df = Mkt_df[0].str.split(",", expand=True)
    Mkt_df = Mkt_df.iloc[:, :23]
    Mkt_df.columns = ['WEB-ID', 'Ticker-Code', 'Symbol', 'Name', 'Time', 'Open', 'Final', 'Close', 'Count', 'Volume', 'Value',
                      'Low', 'High', 'Yesterday', 'EPS', 'Base_Vol', 'Unk1', 'Unk2', 'Sector', 'Day_UL', 'Day_LL', 'Share_No', 'Mkt-ID']
    
    # Map market codes
    Mkt_df['Market'] = Mkt_df['Mkt-ID'].map(lambda x: {'300':'بورس', '303':'فرابورس', '305':'صندوق قابل معامله', '309':'پایه',
                                                        '400':'حق تقدم بورس', '403':'حق تقدم فرابورس', '404':'حق تقدم پایه'}.get(x, 'نامشخص'))
    Mkt_df.drop(columns=['Mkt-ID'], inplace=True)

    # Decode sector names
    r = requests.get('https://cdn.tsetmc.com/api/StaticData/GetStaticData', headers=headers)
    sec_df = pd.DataFrame(r.json()['staticData'])
    sec_df['code'] = sec_df['code'].astype(str).apply(lambda x: '0' + x if len(x) == 1 else x)
    sec_df['name'] = sec_df['name'].apply(lambda x: x.replace('\u200c', '').strip())
    sec_df = sec_df[sec_df['type'] == 'IndustrialGroup'][['code', 'name']]
    Mkt_df['Sector'] = Mkt_df['Sector'].map(dict(sec_df[['code', 'name']].values))

    # Format numeric columns and strings
    cols = ['Open', 'Final', 'Close', 'Count', 'Volume', 'Value', 'Low', 'High', 'Yesterday', 'EPS', 'Base_Vol', 'Day_UL', 'Day_LL', 'Share_No']
    Mkt_df[cols] = Mkt_df[cols].apply(pd.to_numeric, axis=1)
    Mkt_df['Time'] = Mkt_df['Time'].apply(lambda x: x[:-4] + ':' + x[-4:-2] + ':' + x[-2:])
    Mkt_df['Symbol'] = Mkt_df['Symbol'].apply(lambda x: str(x).replace('ي', 'ی').replace('ك', 'ک'))
    Mkt_df['Name'] = Mkt_df['Name'].apply(lambda x: str(x).replace('ي', 'ی').replace('ك', 'ک').replace('\u200c', ' '))
    Mkt_df['WEB-ID'] = Mkt_df['WEB-ID'].apply(lambda x: x.strip())
    Mkt_df = Mkt_df.set_index('WEB-ID')

    # Process order book (best queue)
    OB_df = pd.DataFrame((main_text.split('@')[3]).split(';'))
    OB_df = OB_df[0].str.split(",", expand=True)
    OB_df.columns = ['WEB-ID', 'OB-Depth', 'Sell-No', 'Buy-No', 'Buy-Price', 'Sell-Price', 'Buy-Vol', 'Sell-Vol']
    OB1_df = OB_df[OB_df['OB-Depth'] == '1'].copy()
    OB1_df.drop(columns=['OB-Depth'], inplace=True)
    OB1_df['WEB-ID'] = OB1_df['WEB-ID'].apply(lambda x: x.strip())
    OB1_df = OB1_df.set_index('WEB-ID')
    cols = ['Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No']
    OB1_df[cols] = OB1_df[cols].apply(pd.to_numeric, axis=1)

    # Merge data
    final_df = Mkt_df.join(Mkt_RI_df).join(OB1_df)
    
    # Add Trade_Type
    final_df['Trade_Type'] = final_df['Symbol'].apply(lambda x: 'تابلو' if (not x[-1].isdigit() or x in ['انرژی1', 'انرژی2', 'انرژی3'])
                                                     else ('بلوکی' if x[-1] == '2' else ('عمده' if x[-1] == '4' else ('جبرانی' if x[-1] == '3' else 'تابلو'))))
    
    # Add Download column
    final_df['Download'] = jdatetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Set column order
    columns_order = [
        'Symbol', 'Open', 'High', 'Low', 'Close', 'Final', 'Yesterday', 'Day_UL', 'Day_LL',
        'Count', 'Volume', 'Value',
        'R_Buy_C', 'Co_Buy_C', 'R_Buy_Vol', 'Co_Buy_Vol', 'R_Sell_C', 'Co_Sell_C', 'R_Sell_Vol', 'Co_Sell_Vol',
        'Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No',
        'Time', 'Name', 'Market', 'Sector', 'Trade_Type', 'EPS', 'Base_Vol', 'Share_No',
        'WEB-ID', 'Ticker-Code', 'Unk1', 'Unk2', 'Download'
    ]
    final_df = final_df.reset_index(drop=False)[columns_order]

    # Save to Excel if output is 'excel'
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
