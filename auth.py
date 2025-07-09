import json
import logging
import requests

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
        return token