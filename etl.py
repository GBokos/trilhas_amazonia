import logging
import requests
import pandas as pd

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def buscar_dados(token, api_url):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        dados = response.json()
        logging.info("Dados obtidos com sucesso.")
        return dados
    else:
        logging.error(f"Erro ao buscar dados: {response.status_code} - {response.text}")
        return None

def atualiza_dados(token, API_URL, client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    filtro = "?%24orderby=dataInclusao%20desc"

    for categoria in categorias:
        table_id = projeto+'.'+aplicativo+'.'+categoria.lower()

        query = f"""
            SELECT MAX(dataInclusao) 
            FROM `{table_id}`
        """

        try:
           client.get_table(table_id)
           tabela_existe = True
        except NotFound:
            tabela_existe = False

        if tabela_existe:
            print(table_id)

            result = client.query(query).result()
            data = next(result)

            data_mais_recente = pd.to_datetime(data[0])

            print(data_mais_recente)

            dados = buscar_dados(token, API_URL+aplicativo+'/'+categoria+filtro)

            df = pd.DataFrame(dados['value'])

            df['dataInclusao'] = pd.to_datetime(df['dataInclusao'], format="ISO8601", utc=True)

            df = df[df['dataInclusao'] > data_mais_recente]

            df['dataInclusao'] = pd.to_datetime(df['dataInclusao']).astype(str)
        else:
            print(table_id)

            dados = buscar_dados(token, API_URL+aplicativo+'/'+categoria+filtro)

            df = pd.DataFrame(dados['value'])

        print(df)
        
        carrega_dados(client, df, table_id)

def busca_historico(client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    for categoria in categorias:
        query = f"""
            SELECT MIN(dataInclusao) 
            FROM {projeto}.{aplicativo}.{categoria}
        """

        query_job = client.query(query)
        result = query_job.result()

def carrega_dados(client, df, table_id):
    try:
        job = client.load_table_from_dataframe(
                            df,
                            table_id,
                            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        )

        job.result()

        print('Tabela carregada com sucesso.')
    except Exception as e:
        raise e