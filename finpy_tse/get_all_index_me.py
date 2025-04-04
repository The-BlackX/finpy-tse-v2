import pandas as pd
import requests
import jdatetime

# Global headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/39.0.2171.95 Safari/537.36'
}

# Predefined list of indices and corresponding web IDs
sector_list = [
    'زراعت', 'ذغال سنگ', 'کانی فلزی', 'سایر معادن', 'منسوجات', 'محصولات چرمی', 'محصولات چوبی',
    'محصولات کاغذی', 'انتشار و چاپ', 'فرآورده های نفتی', 'لاستیک', 'فلزات اساسی', 'محصولات فلزی',
    'ماشین آلات', 'دستگاه های برقی', 'وسایل ارتباطی', 'خودرو', 'قند و شکر', 'چند رشته ای',
    'تامین آب، برق و گاز', 'غذایی', 'دارویی', 'شیمیایی', 'خرده فروشی', 'کاشی و سرامیک',
    'سیمان', 'کانی غیر فلزی', 'سرمایه گذاری', 'بانک', 'سایر مالی', 'حمل و نقل', 'رادیویی',
    'مالی', 'اداره بازارهای مالی', 'انبوه سازی', 'رایانه', 'اطلاعات و ارتباطات', 'فنی مهندسی',
    'استخراج نفت', 'بیمه و بازنشستگی', 'شاخص کل', 'شاخص کل هموزن', 'شاخص قیمت وزنی ارزشی',
    'شاخص 50 شركت فعالتر - بازار بورس', 'شاخص 30 شركت بزرگ - بازار بورس'
]
sector_web_id = [
    34408080767216529, 19219679288446732, 13235969998952202, 62691002126902464, 59288237226302898,
    69306841376553334, 58440550086834602, 30106839080444358, 25766336681098389, 12331083953323969,
    36469751685735891, 32453344048876642, 1123534346391630, 11451389074113298, 33878047680249697,
    24733701189547084, 20213770409093165, 21948907150049163, 40355846462826897, 54843635503648458,
    15508900928481581, 3615666621538524, 33626672012415176, 65986638607018835, 57616105980228781,
    70077233737515808, 14651627750314021, 34295935482222451, 72002976013856737, 25163959460949732,
    24187097921483699, 41867092385281437, 61247168213690670, 61985386521682984, 4654922806626448,
    8900726085939949, 18780171241610744, 47233872677452574, 65675836323214668, 59105676994811497,
    32097828799138957, 67130298613737946, 5798407779416661, 46342955726788357, 10523825119011581
]

# Weekday mapping
weekday_mapping = {
    'Saturday': 'شنبه', 'Sunday': 'یک‌شنبه', 'Monday': 'دوشنبه', 'Tuesday': 'سه‌شنبه',
    'Wednesday': 'چهارشنبه', 'Thursday': 'پنج‌شنبه', 'Friday': 'جمعه'
}

def convert_to_persian_weekday(eng_weekday):
    return weekday_mapping.get(eng_weekday, eng_weekday)

def fetch_index_data_to_dataframe(web_id, index_name):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{web_id}', headers=headers)
            if r_cl.status_code != 200:
                print(f"Attempt {attempt + 1}/{max_retries} - Failed to fetch data for {index_name} (web_id {web_id}): HTTP {r_cl.status_code}")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(5)
                continue
            try:
                df_adj = pd.DataFrame(r_cl.json()['indexB2'])[['dEven']]
            except (ValueError, KeyError) as e:
                print(f"Attempt {attempt + 1}/{max_retries} - Invalid JSON response for {index_name} (web_id {web_id}): {e}")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(5)
                continue
            
            df_adj['dEven'] = df_adj['dEven'].apply(lambda x: str(x))
            df_adj['dEven'] = df_adj['dEven'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[-2:]}")
            df_adj['dEven'] = pd.to_datetime(df_adj['dEven'])
            df_adj.rename(columns={"dEven": "Date"}, inplace=True)
            
            df_adj['JDate'] = df_adj['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
            df_adj['Weekday'] = df_adj['Date'].dt.day_name()
            
            r_raw = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={web_id}&t=ph', headers=headers)
            if r_raw.status_code != 200:
                print(f"Attempt {attempt + 1}/{max_retries} - Failed to fetch raw data for {index_name} (web_id {web_id}): HTTP {r_raw.status_code}")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(5)
                continue
            
            raw_df = pd.DataFrame(r_raw.text.split(';'))
            raw_df.columns = ['Raw']
            raw_df = raw_df['Raw'].str.split(",", expand=True)
            raw_df.columns = ['Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'D']
            raw_df.drop(columns=['D'], inplace=True)
            raw_df['Date'] = pd.to_datetime(raw_df['Date'], format='%Y%m%d', errors='coerce')
            
            df_merged = pd.merge(raw_df, df_adj, on='Date', how='inner')
            df_merged = df_merged[['Date', 'JDate', 'Weekday', 'Open', 'High', 'Low', 'Close', 'Volume']]
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            df_merged[numeric_cols] = df_merged[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            df_merged['Weekday'] = df_merged['Weekday'].apply(convert_to_persian_weekday)
            df_merged.insert(0, 'Index', index_name)
            df_merged['Date'] = df_merged['Date'].dt.date
            
            time.sleep(1)
            return df_merged
        
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} - Error processing index {index_name} (web_id {web_id}): {e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(5)
    return pd.DataFrame()

def get_all_index_me():
    """
    Fetch all index data sequentially and return as a DataFrame.
    :return: DataFrame with all index data
    """
    all_index_df = pd.DataFrame()
    for index_name, web_id in zip(sector_list, sector_web_id):
        df_temp = fetch_index_data_to_dataframe(web_id, index_name)
        if not df_temp.empty:
            all_index_df = pd.concat([all_index_df, df_temp], ignore_index=True)
    return all_index_df
