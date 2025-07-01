#!/bin/bass

# Restaura o .env da API2
echo "$API2_ENV" > ./trilhasAmazonia2/.env

# Restaura o arquivo de credenciais JSON
echo "$API2_CRED" > ./trilhasAmazonia2/credenciais_api.json

echo "✅ Arquivos restaurados em ./trilhasAmazonia2"