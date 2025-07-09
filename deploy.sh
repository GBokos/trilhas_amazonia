#!/bin/bash

# Lista de projetos
PROJETOS=("trilhasamazonia2")

# Nome do job e região
JOB_NAME="trilhas-script"
REGIAO="southamerica-east1"

# Loop por projeto
for PROJETO in "${PROJETOS[@]}"
do
  echo "============================"
  echo "🔄 Atualizando projeto: $PROJETO"
  echo "============================"

  # Muda o projeto atual do gcloud
  gcloud config set project "$PROJETO"

  # Build da imagem para o projeto
  gcloud builds submit --tag "gcr.io/${PROJETO}/${JOB_NAME}"

  # Atualiza o job Cloud Run no projeto
  gcloud run jobs update "$JOB_NAME" \
    --image "gcr.io/${PROJETO}/${JOB_NAME}" \
    --region "$REGIAO"
done