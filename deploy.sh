#!/bin/bash

# Script para deploy do job em múltiplos projetos GCP
# Rodar este script requer que o gcloud esteja instalado e autenticado
# Além disso, o usuário deve ter permissões adequadas em todos os projetos listados

# Usar 'gcloud auth login' para autenticar se necessário

# Lista de projetos
PROJETOS=("trilhasmgf-1" "trilhastasp")

# Nome do job e região
JOB_NAME="trilhas-script"
REGIAO="southamerica-east1"

# Loop por projeto
for PROJETO in "${PROJETOS[@]}"
do
  echo "============================"
  echo "Projeto: $PROJETO"
  echo "============================"

  # Muda o projeto atual do gcloud
  gcloud config set project "$PROJETO"

  echo "Projeto ativo: $(gcloud config get-value project)"

  # Build da imagem para o projeto
  gcloud builds submit --tag "gcr.io/${PROJETO}/${JOB_NAME}"

  if gcloud run jobs describe "$JOB_NAME" --region "$REGIAO" > /dev/null 2>&1; then
    echo "✅ Job já existe em $PROJETO → atualizando..."
    gcloud run jobs update "$JOB_NAME" \
      --image "gcr.io/${PROJETO}/${JOB_NAME}" \
      --region "$REGIAO" \
      #--service-account=deploy-cloudrun@$PROJETO.iam.gserviceaccount.com
  else
    echo "🆕 Job não existe em $PROJETO → criando..."
    gcloud run jobs create "$JOB_NAME" \
      --image "gcr.io/${PROJETO}/${JOB_NAME}" \
      --region "$REGIAO" --quiet \
      #--service-account=deploy-cloudrun@$PROJETO.iam.gserviceaccount.com
  fi
done