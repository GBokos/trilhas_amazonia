from google.cloud import bigquery
from google.cloud import secretmanager

def connect_big_query(projeto):
    client = bigquery.Client(project=projeto)
    print(client.project)
    return client

def carregar_segredo(project_id, nome_segredo):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{nome_segredo}/versions/latest"
    response = client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    return payload