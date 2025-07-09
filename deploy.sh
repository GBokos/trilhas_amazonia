#!/bin/bash

gcloud builds submit --tag gcr.io/trilhasamazonia/trilhas-script &&
gcloud run jobs update trilhas-script \
  --image gcr.io/trilhasamazonia/trilhas-script \
  --region southamerica-east1