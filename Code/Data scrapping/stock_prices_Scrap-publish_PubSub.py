import requests
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
from google.cloud import pubsub_v1
import json
import time
from datetime import datetime

def strToInt(price):
    for i in price:
        if i.isnumeric() or i==".":
            continue
        else:
            price= price.replace(i, '')
    return float(price)

def publish_message(df):
    """
    Publish the stock data to pub sub in every 1 minute interval

    Input--> df
    Output--> None
    """
    topic_path = publisher.topic_path(project_id, topic_name)
    message = json.dumps(df.to_dict(orient='records')).encode('utf-8')
    try:
        future = publisher.publish(topic_path, data=message)
        print(f"Message published to {topic_path}. Message ID: {future.result()}")
    except Exception as e:
        print(f"Error publishing message to {topic_path}: {str(e)}")


def Get_Prices(symbols):
    """
    This function fetch the data from google finance and scrap the prices of all the NIFTY50 stocks
    
    Input--> symbols
    Output--> prices 
    """
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
    return prices

def Get_StockINFO ():
    """
    This function fetch the NIFTY50 stocks names and symbols
    Input: None
    Output: stocks
    """
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
    return stocks

if __name__ == "__main__":
    project_id = "your-project-id"
    topic_name = "your-topic-name"
    
    stocks_info = Get_StockINFO()
    data = [[name,stocks_info[name]] for name in stocks_info]
    df_stocks= pd.DataFrame(data,columns=['Names','Symbols'])
    
    publisher = pubsub_v1.PublisherClient()
    
    # looping the process of publishing the message to pubsub every 60 sec
    while True:
        prices= Get_Prices(df_stocks.iloc[:,1])
        
        # adding price to the final result data frame
        df_stocks['Prices']=[column[0] for column in prices]

        # adding timestamps to the final result dataframe
        df_stocks['Timestamp']=[column[1] for column in prices]

        publish_message(df_stocks)
        time.sleep(60) # Wait for 1 minute before publishing the next message

