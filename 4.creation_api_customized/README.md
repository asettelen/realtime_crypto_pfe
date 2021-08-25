# Déploiement d'une API 

## L'objectif de cette API est de mettre à disposition les données de prédiction (automatisées et obtenues quotidiennement) depuis une requête HTTP réalisable depuis n'importe quelle plateforme.

### 1. Depuis le terminal de la VM : 

``` bash
cd /home/jupyter/github_final_pfe_crypto/4.creation_api_customized
```

### 2. Dockeriser l'image (depuis le terminal de la VM) :

``` bash
docker build -t api_endpoint_bigquery:latest -f Dockerfile .
```

### 3. Pusher l'image sur Container Registry 

``` bash
project=$(gcloud config get-value project 2> /dev/null)
docker tag api_endpoint_bigquery:latest gcr.io/${project}/api_endpoint_bigquery:unsecured
gcloud auth configure-docker puis Y
docker push gcr.io/${project}/api_endpoint_bigquery:unsecured
```

### 4. Enable Cloud Run API : 

``` bash
gcloud services list --available
gcloud services enable run.googleapis.com
```

### 5. Déployer l'image sur Cloud Run : Changer l'image par le container correspond dans Container Registry

``` bash
gcloud run deploy cr-api-endpoint-bigquery \
--image=gcr.io/verdant-cargo-321713/api_endpoint_bigquery@sha256:b5cf916791b201d0988aa1459ffda0120a35586a4fde056ceb4258535e978faa \
--platform=managed \
--region=us-central1 \
--allow-unauthenticated \
--cpu=4 \
--memory=8G \
--timeout=600 
```

### 6. Depuis Cloud Shell : 

``` bash
gcloud beta run services add-iam-policy-binding --region=us-central1 --member=allUsers --role=roles/run.invoker cr-api-endpoint-bigquery
```

### 7. Tester l'api depuis un jupyter notebook (local ou sur le cloud) avec la commande suivante : 

``` bash
from requests import get 
import requests
url = 'https://cr-api-endpoint-bigquery-6764xcvr7q-uc.a.run.app'
params = {'model': 'ARIMA', 
         'stat':'AVG', 
         'currencies':["ETH", "BTC", "DOGE"], 
         'start_date': '2021-01-01 08:00:00 UTC',
         'end_date': '2021-04-01 08:00:00 UTC',
         }
response = requests.get(url, params)
response
import pandas
df = pandas.DataFrame(response.json())
df
```

