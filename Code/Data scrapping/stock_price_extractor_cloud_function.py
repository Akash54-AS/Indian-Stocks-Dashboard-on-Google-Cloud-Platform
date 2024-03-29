import requests
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
from google.cloud import pubsub_v1
import json
from datetime import datetime

def strToInt(price):
    '''
    This function will change the float looking string whihc is a price into integer 
    '''
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
    project_id = "stock-analysis-project-379015"
    topic_name = "stock_prices"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    message = json.dumps(df.to_dict(orient='records')).encode('utf-8')
    try:
        future = publisher.publish(topic_path, data=message)
        return f"Message published to {topic_path}. Message ID: {future.result()}"
    except Exception as e:
        return f"Error publishing message to {topic_path}: {str(e)}"
        
def Get_Prices(stock):
    '''
    This function will the scrap data from Google finance for each stock in NIFTY50
    and return the prices for individual stocks
    '''

    headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'} 
    urls=[[f"https://www.google.com/finance/quote/{symbols}",symbols,names] for (names,symbols) in zip(stock["Names"],stock["Symbols"])]

    
    def fetch_url(url):
        response = requests.get(url[0],headers=headers)
        soup= BeautifulSoup(response.text, 'html.parser')
        price= soup.find("div",{"class":"YMlKec fxKbKc"}).get_text()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return [url[2],url[1],strToInt(price),timestamp]
    
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
    '''
    This function will give the info regarding the stocks which is in NIFTY50
    output --> dict{names:symbols}
    '''
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


def run(request):
    request = request.get_data()
    try: 
        request_json = json.loads(request.decode())
    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        return "JSON Error", 400

    try:
            
        stocks_info = Get_StockINFO()
        data = [[name,stocks_info[name]] for name in stocks_info]
        df_stocks= pd.DataFrame(data,columns=['Names','Symbols'])
        prices= Get_Prices(df_stocks)
        #print(prices)
        #df= pd.DataFrame(prices, columns =["Prices","Timestamp","Symbols"]) 

        '''# adding price to the final result data frame
        df_stocks['Prices']=[column[0] for column in prices]

        # adding timestamps to the final result dataframe
        df_stocks['Timestamp']=[column[1] for column in prices]
        print(df_stocks)
        '''
        df=pd.DataFrame(prices,columns=['Names','Symbols','Prices','Timestamp'])

        publish_func_log= publish_message(df)
        return publish_func_log
    except Exception as e:
        return f'{request_json.get("name")} Failed'


