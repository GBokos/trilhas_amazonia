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
        print(token)

        if token:
            for app in aplicativos:
                atualiza_dados(token=token
                            , bq_client=bq_client
                            , API_URL=API_URL
                            , projeto=PROJETO
                            , aplicativo=app
                            , categorias_por_aplicativo=categorias_por_aplicativo 
                            )
                
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

            # token = {
            #     "access_token": "LwADoVcSTmiV5yz0Ss5IXiXSida4FgD38RK2qr2WjwlJBzdnfZMRvsgCykWLYOvcjC3oEqJuV_Yt7nGBgu9J5zsz70VrkHV9B10ShH-QlhHSFtMIwzdKso1e5Va0Yk4HjbQD_xslajrdW5ClC4Wryl6A4oFj7f9OFAcx-c6NIqASY0SEUoBooRGE6uAhIrmh2A8LJDaiBoSoi5gi30PsYV8SFAk2JTN7v98xLNr6qOk0KVKMAttUK3isScxh0wNXda9h7J-zGXC97Z5dWkEKpGK_hy0HT5gMOPKo-qjZLY8yN6vo9c0diwZxntgz8SBYf0cyYEar8QCPv2Wu6mVypBCHzPrKNgUn46fZkGFxr8fJcAw2RrKine64ULY4te_8YUUy5VkvSdhLyju9u86n1RVtJQNPwnIj8rd3kYwE3iEENgQnmmpDZPEVEiWh_tK-Q2KB4jhSdmYaVACn5V9Cas8f0Ba0CdTyVce_WPGuirjmq-F4YtZns2726B7bRPPbBqSYPFIXzjIFPKsJQ1rEMgWvtzHX5Q_lbUT7zFKqOlE2GKS8eK6g1n8JEEIHb1mVt9geMeA4Fmf8HU7wX7tCwVU3FF2X0hufPq4Yfb4kji5dh2THFlvNJ3hEeq_KbZPDTjEtrlKC5Abr689VkDSyVTSB98uX4Y_WH8Sx2HxVnTU_EFuxIyc0TMSJ31fD43LT_8MioWnsnT--r9ZRdlHneUlyJAgM6lVewG42Q9K0t5lidyqGguOoMl4QoDXedJ3_LNKF3IRZA-Zoz3t1M91U3yb1dNpp7xUpgMiRGKcN5YFtFq4PAh9XJ0ccRBDy2m_fjLvK4XNgweFDmNHIY31MokeuLTYtWLtOXwfYgrzN8f_9Tms9D0mp8N9N5CbmsRuQKryqiydYavtcvuLEwB0dn4YZ9wGzfb22Lsd_4FaUqjtW8-gKWll5lYZ3UexggTfgmNYHXEeitApKfSJ-e2LEpCMDd1qjr_rhCvbhsO2-LQNjE9xeY1SuH1JOFgqIC39TtY45zRmuH1kE3mYVya51ECa2a9qFb4ecJ4c9jzrUpj48dIFGGZRGNwyi5iLqvV-8qtvRKA",
            #     "token_type": "bearer",
            #     "expires_in": 86399,
            #     ".expires": "Thu, 11 Sep 2025 16:41:44 GMT",
            #     "platformUser": 'null'
            # }

            # print(token)

            if token:
                for app in aplicativos:
                    # atualiza_dados(token=token
                    #             , bq_client=bq_client
                    #             , API_URL=API_URL
                    #             , projeto=projeto # projeto em 'str' é o minúsculo em modelagem
                    #             , aplicativo=app
                    #             , categorias_por_aplicativo=categorias_por_apli
                    # cativo 
                    #             )
                    
                    busca_historico(token=token
                                , bq_client=bq_client
                                , API_URL=API_URL
                                , projeto=projeto
                                , aplicativo=app
                                , categorias_por_aplicativo=categorias_por_aplicativo 
                                )

                    # verifica_alteracoes(token=token
                    #             , bq_client=bq_client
                    #             , bqstorage_client=bqstorage_client
                    #             , API_URL=API_URL
                    #             , projeto=projeto # projeto em 'str' é o minúsculo em modelagem
                    #             , aplicativo=app
                    #             , categorias_por_aplicativo=categorias_por_aplicativo 
                    #             )
            else:
                print("Falha ao obter token de acesso")


if __name__ == "__main__":
    main()