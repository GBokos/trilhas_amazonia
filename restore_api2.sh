#!/bin/bass

# Restaura o .env da API2
echo "$API2_ENV" > ./trilhasAmazonia/.env

# Restaura o arquivo de credenciais JSON
echo "$API2_CRED" > ./trilhasAmazonia/credenciais_api.json

echo "✅ Arquivos restaurados em ./trilhasAmazonia2"