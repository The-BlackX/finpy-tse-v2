import requests

url = 'http://tsetmc.com/tsev2/data/MarketWatchPlus.aspx'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

response = requests.get(url, headers=headers)
print("Status Code:", response.status_code)
print("First 500 characters of response:")
print(response.text[:500])