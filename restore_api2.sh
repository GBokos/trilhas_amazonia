#!/bin/bass

cat ./secrets/api2_env.txt > ./trilhasAmazonia2/.env
cat ./secrets/api2_cred.json > ./trilhasAmazonia2/credenciais_api2.json

echo "✅ Arquivos restaurados em ./trilhasAmazonia2"