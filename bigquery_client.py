from google.cloud import bigquery

def connect_big_query():
    client = bigquery.Client()
    return client