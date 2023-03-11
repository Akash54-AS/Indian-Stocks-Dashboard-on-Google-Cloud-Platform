import requests
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
from datetime import datetime

def strToInt(price):
    for i in price:
        if i.isnumeric() or i==".":
            continue
        else:
            price= price.replace(i, '')
    return float(price)

def Get_Prices(symbols):
    headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'} 
    urls=[f"https://www.google.com/finance/quote/{symbol}" for symbol in symbols]
    def fetch_url(url):
        response = requests.get(url,headers=headers)
        soup= BeautifulSoup(response.text, 'html.parser')
        price= soup.find("div",{"class":"YMlKec fxKbKc"}).get_text()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return [strToInt(price),timestamp]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_url, url) for url in urls]
        prices = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    #url=f'https://www.google.com/finance/quote/{symbol}'
    #r = requests.get(url)
    #soup= BeautifulSoup(r.text, 'html.parser')
    #prices= soup.find("div",{"class":"YMlKec fxKbKc"}).get_text()
    return prices

    #print(price) 

def Get_StockINFO ():
    url = "https://en.wikipedia.org/wiki/NIFTY_50"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "constituents"})

    stocks = {}

    for row in table.findAll("tr")[1:]:
        symbol = row.findAll("td")[1].text.strip()
        name=row.findAll("td")[0].text.strip()
        symbol=symbol+":NSE"
        stocks.update({name:symbol})
    #print(stocks)
    return stocks

if __name__ == "__main__":
    stocks_info = Get_StockINFO()
    data = [[name,stocks_info[name]] for name in stocks_info]
    df_stocks= pd.DataFrame(data,columns=['Names','Symbols'])
    prices= Get_Prices(df_stocks.iloc[:,1])
    #print(prices)
    # adding price to the final result data frame
    df_stocks['Prices']=[column[0] for column in prices]

    # adding timestamps to the final result dataframe
    df_stocks['Timestamp']=[column[1] for column in prices]
    print(df_stocks)
