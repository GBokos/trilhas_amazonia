#!/bin/bass

# Restaura o .env da API1
echo "$API1_ENV" > ./trilhasAmazonia/.env

# Restaura o arquivo de credenciais JSON
echo "$API1_CRED" > ./trilhasAmazonia/credenciais_api.json

echo "✅ Arquivos restaurados em ./trilhasAmazonia"