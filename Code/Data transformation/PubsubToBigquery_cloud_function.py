import base64
import json
from google.cloud import bigquery

def pubsub_to_bq(event, context):
    # Parse the Pub/Sub message
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    
    # Create a BigQuery client and table reference
    bq_client = bigquery.Client()
    table_ref = bq_client.dataset('my_dataset').table('my_table')

    # Insert the message data into BigQuery
    errors = bq_client.insert_rows_json(table_ref, [pubsub_message])
    if errors:
        print('Encountered errors while inserting rows: {}'.format(errors))
    else:
        print('Rows successfully inserted into BigQuery')
