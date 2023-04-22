import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.http import HttpSource

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

def GetData(Symbol):
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
    url='https://query1.finance.yahoo.com/v7/finance/download/'\
        +Symbol+'.NS?period1='+StartDate+'&period2='+EndDate+'&interval='\
        +interval+'&events='+events+'&includeAdjustedClose=true'
    data= pd.read_csv(url)
    return data

# Defining the add column class for distrubuted processing
class AddColumns(beam.DoFn):
    def process(self, element):
        data,Symbol,name=element
        data["Symbols"]=[Symbol for i in data['Close']]
        data["Names"]=[name for i in data['Close']]
        data=data.loc[:,['Names','Symbols','Date','Close']]
        data=data.rename(columns={'Close':'Prices'})
        return data

# Define the pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'YOUR_PROJECT_ID'
google_cloud_options.region = 'YOUR_REGION'
google_cloud_options.job_name = 'dataflow-job-name'
google_cloud_options.staging_location = 'gs://YOUR_BUCKET_NAME/staging'
google_cloud_options.temp_location = 'gs://YOUR_BUCKET_NAME/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'


#Get the stock INFO
Stock=Get_StockINFO ()

for name in Stock:

    # Define the pipeline
    p = beam.Pipeline(options=options)
    data= GetData(Stock[name])

    pcoll = p | 'Create PCollection' >> beam.Create(data)

    # apply the ParDo transform to add the 'gender' column
    pcoll = pcoll | 'Add Symbol and Names' >> beam.ParDo(AddColumns())

    # convert the PCollection back to a DataFrame
    df = (pcoll
        | 'Convert to List' >> beam.combiners.ToList()
        | 'Convert to DataFrame' >> beam.Map(lambda lst: pd.DataFrame(lst)))

    # Write the data to BigQuery
    table_name = 'stock-analysis-project.StockData.Historic_Data'
    #table_schema = 'YOUR_BIGQUERY_TABLE_SCHEMA'
    write_to_bigquery = WriteToBigQuery(
        table=table_name,
        project=google_cloud_options.project,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
    df | write_to_bigquery

# Run the pipeline
result = p.run()
result.wait_until_finish()
