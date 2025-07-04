#!/bin/bass

#TASP
mkdir -p ./trilhasamazonia2

cat ./secrets/api2_env.txt > ./trilhasamazonia2/.env
cat ./secrets/api2_cred.json > ./trilhasamazonia2/credenciais_google.json

echo "✅ Arquivos restaurados em ./trilhasamazonia2"