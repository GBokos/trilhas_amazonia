#!/bin/bass

cat ./secrets/api1_env.txt > ./trilhasAmazonia/.env
cat ./secrets/api1_cred.json > ./trilhasAmazonia/credenciais_google.json

echo "✅ Arquivos restaurados em ./trilhasAmazonia"