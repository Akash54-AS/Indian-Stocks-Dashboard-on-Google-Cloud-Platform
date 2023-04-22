import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

def Get_StockINFO ():
    '''
    This function will give the info regarding the stocks which is in NIFTY50
    '''
    url = "https://en.wikipedia.org/wiki/NIFTY_50"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "constituents"})

    stocks = {}

    for row in table.findAll("tr")[1:]:
        symbol = row.findAll("td")[1].text.strip()
        name=row.findAll("td")[0].text.strip()
        stocks.update({name:symbol})
    #print(stocks)
    return stocks

def GetData(Stock):
    '''
    This function get the data from yahoo finance related to all the Nifity 50 stocks for historical date.

    '''
    # Please add the required start and end date if you want to backload the data
    StartDate= str(int(datetime.datetime(1996,1,1).timestamp()))
    EndDate= str(int(datetime.datetime(2023,4,22).timestamp()))

    # Its a interval in which data we want , which willl be daily
    interval='1d'
    # we need historical data, so events will be set to history
    events='history'
    Historical_Data = pd.DataFrame()
    for name in Stock:
        Symbol=Stock[name]
        url='https://query1.finance.yahoo.com/v7/finance/download/'\
            +Symbol+'.NS?period1='+StartDate+'&period2='+EndDate+'&interval='\
            +interval+'&events='+events+'&includeAdjustedClose=true'
        data= pd.read_csv(url)
        data["Symbols"]=[Symbol for i in data['Close']]
        data["Names"]=[name for i in data['Close']]
        data=data.loc[:,['Names','Symbols','Date','Close']]
        data=data.rename(columns={'Close':'Prices'})
        print(data)
        Historical_Data= pd.concat([Historical_Data,data],ignore_index=True)
    return Historical_Data

if __name__=="__main__":
    # First get the stock info by calling Get_StockINFO function
    Data =Get_StockINFO()

    #Run the GetData function to get Historical data
    Historical_Data=GetData(Data)
    print(Historical_Data)


