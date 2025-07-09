import os

from auth import obter_token
from dotenv import load_dotenv
from etl import atualiza_dados, busca_historico
from bigquery_client import connect_big_query, carregar_segredo
from config import projetos, id_projetos, aplicativos, categorias_por_aplicativo

# Função principal
def main():
    for i in range(0,len(projetos)):
        print("\n")
        print("Iniciando carga para ", projetos[i])
        print("\n")
    
        # env_path = projetos[i] + "/.env"
        # load_dotenv(dotenv_path=env_path, override=True)

        # cred_path = projetos[i] + "/credenciais_google.json"
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

        client = connect_big_query()

        env_conteudo = carregar_segredo(id_projetos[i], f"{projetos[i]}-env")
        for linha in env_conteudo.splitlines():
            chave, valor = linha.split("=", 1)
            os.environ[chave] = valor
        
        json_credenciais = carregar_segredo(id_projetos[i], f"{projetos[i]}-credenciais-json")
        with open("/tmp/credenciais.json", "w") as f:
            f.write(json_credenciais)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credenciais.json"

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
                            , projeto=projetos[i]
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
        else:
            print("Falha ao obter token de acesso")

if __name__ == "__main__":
    main()