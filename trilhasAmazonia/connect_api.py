import os
import json
import logging
import requests
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

categorias_por_aplicativo = {
    "faturamento": [
        'OrdemVenda'
        , 'OrdemVendaProduto'
        , 'Produto'
        #'notaFiscal',
        , 'NFeProduto'
        , 'categoria'
        , 'pessoa'
    ],
    "financeiro": [
        'centroCusto',
        'contaPagar',  
        'contaReceber'
    ],
    "compras": [
        'pedido'
    ]
}

def connect_big_query():
    client = bigquery.Client()
    return client


def obter_token(client_id, client_secret, platform_id, auth_url):
    headers = {
        'Content-Type': 'application/json'
        }

    payload = {
        'grant_type': 'client_credentials',
        'UserName': client_id,
        'Password': client_secret,
        "PlataformaId": platform_id
    }

    print(payload)

    response = requests.post(auth_url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        token = response.json().get('access_token')
        logging.info("Token obtido com sucesso.")
        return token
    else:
        token = logging.error(f"Erro ao obter token: {response.status_code} - {response.text}")
        print(token)
        return token

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


def salvar_csv(dados, nome_base='relatorio'):
    data_hoje = datetime.today().strftime('%Y-%m-%d')
    try:
        df = pd.DataFrame(dados['value'])
        
        df.to_csv(f'{nome_base}_{data_hoje}.csv', index=False, encoding="utf-8")
        print("✅ CSV criado com sucesso.")
    except Exception as e:
        print("❌ Erro ao converter:", e)

    print(df)

    logging.info(f'Dados salvos em {nome_base}_{data_hoje}.csv')

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
           tabela = client.get_table(table_id)
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

        job = client.load_table_from_dataframe(
                        df,
                        table_id,
                        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    )

        job.result()

        print('Tabela carregada com sucesso.')



def busca_historico(client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    for categoria in categorias:
        query = f"""
            SELECT MIN(dataInclusao) 
            FROM {projeto}.{aplicativo}.{categoria}
        """

        query_job = client.query(query)
        result = query_job.result()


# Função principal
def main():
    load_dotenv()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/gpbokos/OneDrive - Stefanini/Documentos/Giorgios/python/python/trilhasamazonia-6d08d8d68c11.json"
    client = connect_big_query()

    projeto = 'trilhasamazonia'

    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    PLATFORM_ID = os.getenv("PLATFORM_ID")
    AUTH_URL = os.getenv("AUTH_URL")
    API_URL = os.getenv("API_URL")

    token = obter_token(CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL)

    aplicativos = [
        'faturamento'
        # , 'financeiro'
        # , 'compras'
        ]

    if token:
        for app in aplicativos:
            atualiza_dados(token=token
                           , client=client
                           , API_URL=API_URL
                           , projeto=projeto
                           , aplicativo=app
                           , categorias_por_aplicativo=categorias_por_aplicativo 
                           )

    # if token:
    #     for app in aplicativos:
    #         if app == 'faturamento':
    #             for categoria in categoria_faturamento:
    #                 filtro = "?%24orderby=dataInclusao%20desc"
    #                 #skip = "&%24skip=1000"
    #                 dados = buscar_dados(token, API_URL+app+'/'+categoria)

    #                 df = pd.DataFrame(dados['value'])

    #                 print(df)

    #                 #df = df.drop(columns=['@odata.type'])

    #                 table_id = projeto+'.'+app+'.'+categoria.lower()

    #                 job = client.load_table_from_dataframe(
    #                     df,
    #                     table_id,
    #                     job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    #                 )

    #                 job.result()

    #                 print('Tabela carregada com sucesso.')

    #                 #salvar_csv(dados, app+'_'+categoria)

    #         if app == 'financeiro':
    #             for categoria in categoria_financeiro:
    #                 #filtro = '?%24orderby=dataVencimento%20desc'
    #                 dados = buscar_dados(token, API_URL+app+'/'+categoria)
    #                 #print(dados)
    #                 salvar_csv(dados, app+'_'+categoria)
            
    #         if app == 'compras':
    #             for categoria in categoria_compras:
    #                 #filtro = '?%24orderby=dataVencimento%20desc'
    #                 dados = buscar_dados(token, API_URL+app+'/'+categoria)
    #                 #print(dados)
    #                 salvar_csv(dados, app+'_'+categoria)

if __name__ == "__main__":
    main()