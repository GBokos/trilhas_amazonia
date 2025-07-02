import os

from auth import obter_token
from dotenv import load_dotenv
from bigquery_client import connect_big_query
from etl import atualiza_dados, busca_historico
from config import projetos, aplicativos, categorias_por_aplicativo

# Função principal
def main():
    for projeto in projetos:
        print("\n")
        print("Iniciando carga para ", projeto)
        print("\n")
    
        env_path = projeto + "/.env"
        load_dotenv(dotenv_path=env_path, override=True)

        cred_path = projeto + "/credenciais_google.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        client = connect_big_query()

        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")
        PLATFORM_ID = os.getenv("PLATFORM_ID")
        AUTH_URL = os.getenv("AUTH_URL")
        API_URL = os.getenv("API_URL")

        token = obter_token(CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL)

        if token:
            for app in aplicativos:
                atualiza_dados(token=token
                            , client=client
                            , API_URL=API_URL
                            , projeto=projeto
                            , aplicativo=app
                            , categorias_por_aplicativo=categorias_por_aplicativo 
                            )
                
                # busca_historico(token=token
                #             , client=client
                #             , API_URL=API_URL
                #             , projeto=projeto
                #             , aplicativo=app
                #             , categorias_por_aplicativo=categorias_por_aplicativo 
                #             )

if __name__ == "__main__":
    main()