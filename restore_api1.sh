#!/bin/bass

#MGF
mkdir -p ./trilhasamazonia/

echo "abc"

cat ./secrets/api1_env.txt > ./trilhasamazonia/.env
cat ./secrets/api1_cred.json > ./trilhasamazonia/credenciais_google.json

echo "✅ Arquivos restaurados em ./trilhasamazonia"