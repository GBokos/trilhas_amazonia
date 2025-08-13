import os
from dotenv import load_dotenv

from bigquery_client import carregar_segredo, connect_big_query

# Conectar em Modelagem

def load_environment(AMBIENTE, projeto=None):
    if AMBIENTE == 'PRODUCAO':

        PROJETO = os.getenv("PROJETO")
        PROJETO_ID = os.getenv("PROJETO_ID")

        print("\n")
        print("Iniciando carga para ", PROJETO + ' - ' + PROJETO_ID)
        print("\n")

        client = connect_big_query(PROJETO)

        env_conteudo = carregar_segredo(PROJETO_ID, f"{PROJETO}-env")
        for linha in env_conteudo.splitlines():
            chave, valor = linha.split("=", 1)
            os.environ[chave] = valor
        
        json_credenciais = carregar_segredo(PROJETO_ID, f"{PROJETO}-credenciais-json")

        with open("/tmp/credenciais.json", "w") as f:
            f.write(json_credenciais)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credenciais.json"

        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")
        PLATFORM_ID = os.getenv("PLATFORM_ID")
        AUTH_URL = os.getenv("AUTH_URL")
        API_URL = os.getenv("API_URL")

        print(CLIENT_ID)
        print(CLIENT_SECRET)
        print(PLATFORM_ID)
        print(AUTH_URL)
        print(API_URL)

        return client, PROJETO, CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL, API_URL
    
    else:
        env_path = projeto + "/.env"
        load_dotenv(dotenv_path=env_path, override=True)

        cred_path = projeto + "/credenciais_google.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

        client = connect_big_query(projeto)

        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")
        PLATFORM_ID = os.getenv("PLATFORM_ID")
        AUTH_URL = os.getenv("AUTH_URL")
        API_URL = os.getenv("API_URL")

        print(CLIENT_ID)
        print(CLIENT_SECRET)
        print(PLATFORM_ID)
        print(AUTH_URL)
        print(API_URL)

        return client, CLIENT_ID, CLIENT_SECRET, PLATFORM_ID, AUTH_URL, API_URL
    