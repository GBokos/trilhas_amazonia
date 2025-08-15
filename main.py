import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="google.cloud.bigquery._pandas_helpers")

from auth import obter_token
from dotenv import load_dotenv
from google.cloud import bigquery
from dotenv_utils import load_environment
from etl import atualiza_dados, busca_historico, verifica_alteracoes
from config import aplicativos, categorias_por_aplicativo

# Função principal
def main():
    load_dotenv()

    AMBIENTE = os.getenv("AMBIENTE")
    print(f"\nAmbiente: {AMBIENTE}")
    
    if AMBIENTE == 'PRODUCAO':
        bq_client, bqstorage_client, PROJETO, CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL, API_URL = load_environment(AMBIENTE)

        token = obter_token(CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL)

        if token:
            for app in aplicativos:
                # atualiza_dados(token=token
                #             , bq_client=bq_client
                #             , API_URL=API_URL
                #             , projeto=PROJETO
                #             , aplicativo=app
                #             , categorias_por_aplicativo=categorias_por_aplicativo 
                #             )
                
                # busca_historico(token=token
                #             , bq_client=bq_client
                #             , API_URL=API_URL
                #             , projeto=projeto
                #             , aplicativo=app
                #             , categorias_por_aplicativo=categorias_por_aplicativo 
                #             )

                verifica_alteracoes(token=token
                                , bq_client=bq_client
                                , bqstorage_client=bqstorage_client
                                , API_URL=API_URL
                                , projeto=PROJETO
                                , aplicativo=app
                                , categorias_por_aplicativo=categorias_por_aplicativo 
                                )
        else:
            print("Falha ao obter token de acesso")
    
    else:
        PROJETO = os.getenv("PROJETO").split(',')
        print("Projetos:", PROJETO)

        for projeto in PROJETO:
            bq_client, bqstorage_client, CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL, API_URL = load_environment(AMBIENTE, projeto=projeto)

            token = obter_token(CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL)

            print(token)

            if token:
                for app in aplicativos:
                    # atualiza_dados(token=token
                    #             , client=client
                    #             , API_URL=API_URL
                    #             , projeto=projeto # projeto em 'str' é o minúsculo em modelagem
                    #             , aplicativo=app
                    #             , categorias_por_aplicativo=categorias_por_aplicativo 
                    #             )
                    
                    # busca_historico(token=token
                    #             , client=client
                    #             , API_URL=API_URL
                    #             , projeto=projeto
                    #             , aplicativo=app
                    #             , categorias_por_aplicativo=categorias_por_aplicativo 
                    #             )

                    verifica_alteracoes(token=token
                                , bq_client=bq_client
                                , bqstorage_client=bqstorage_client
                                , API_URL=API_URL
                                , projeto=projeto # projeto em 'str' é o minúsculo em modelagem
                                , aplicativo=app
                                , categorias_por_aplicativo=categorias_por_aplicativo 
                                )
            else:
                print("Falha ao obter token de acesso")


if __name__ == "__main__":
    main()