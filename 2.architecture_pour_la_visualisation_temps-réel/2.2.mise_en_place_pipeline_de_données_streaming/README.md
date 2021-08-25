# Mise en place d’un pipeline de données STREAMING

***Utilisation de rejeu (batch répété) afin de mettre en place une simulation de streaming***
Rappelons-nous que ***la table source présentée précédemment n’est pas alimentée en temps-réel***. Il ne suffirait donc pas d’alimenter directement un topic (dans Cloud Pub/Sub) en fonction des insertions dans BigQuery. Ainsi, nous allons utiliser un template permettant de rejouer des données temporelles depuis une table BigQuery jusqu’à un topic Pub/Sub (développé par Evgeny Medvedev et accessible en open-source sur https://github.com/blockchain-etl/bigquery-to-pubsub). Ce template peut être utilisé pour rejouer n’importe quelle table BigQuery comportement des données temporelles et donc un champ défini comme Timestamp. Le template est un programme python qui extrait séquentiellement des morceaux de données BigQuery partitionnée et publie les lignes sous forme de messages JSON dans un sujet Pub/Sub en temps voulu. 

## Voici toutes les étapes à suivre pour mettre en place le streaming : 

### A. Mise en place de la publication (depuis la VM Apache-Beam-Crypto-Test créée précédemment ou Cloud Shell):

#### 1. Create a Service Account (stream-bq-to-pubsub) with the following roles: 

-BigQuery Admin 
<br>
-Storage Admin 
<br>
-Pub/Sub Publisher 
<br>

#### 2. Create a key file for the Service Account and download it as credentials_file.json. 

Upload it in the folder 2.2 Mise en place d'un pipeline de données STREAMING 
<br>

#### 3. Enable Cloud Pub/Sub API (from the VM terminal) :

``` bash
  gcloud services enable pubsub.googleapis.com  
```

#### 4. Create a Pub/Sub topic called bigquery-to-pubsub-test0 (from the VM terminal): 

``` bash
  gcloud pubsub topics create bigquery-to-pubsub-test0
```

#### 5. Create a temporary GCS bucket and a temporary BigQuery dataset: <br>

from bigquery-to-pubsub (from the VM terminal), launch : 

``` bash
  cd bigquery-to-pubsub
  chmod 777 get_temp_resource_name.sh 
  bash create_temp_resources.sh
```

#### 6. Run replay for Ethereum transactions (Depuis un terminal): 

``` bash
  docker build -t bigquery-to-pubsub:latest -f Dockerfile .
  project=$(gcloud config get-value project 2> /dev/null)
  temp_resource_name=$(./get_temp_resource_name.sh)
  cd ..
  PATH_DOCK=$(pwd)
  echo $PATH_DOCK
  docker run -v $PATH_DOCK/:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/credentials_file.json \
  bigquery-to-pubsub:latest --bigquery-table public-data-finance.crypto_band.oracle_requests --timestamp-field block_timestamp_truncated \
  --start-timestamp 2021-05-23T05:23:00 --end-timestamp 2021-05-23T05:23:15 --batch-size-in-seconds 3600 --replay-rate 1 --pubsub-topic \
  projects/${project}/topics/bigquery-to-pubsub-test0 --temp-bigquery-dataset ${temp_resource_name} --temp-bucket ${temp_resource_name}
```
Si la commande précédente a bien fonctionné, cela signifie que la publication fonctionne. Il faut mettre en place une souscription pour récupérer les messages. 

![](images/lancement_template_streaming_simule.png)

### B. Mise en place de la souscription locale (interactiverunner()): <br>
Ouvrir le notebook Streaming_Souscription et suivre les étapes jusqu'à la partie 2 inclue. 

### C. Relancer la publication de messages (à partir de la minute suivant le maximum du batch): <br>

``` bash
  docker run -v $PATH_DOCK/:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/credentials_file.json \
  bigquery-to-pubsub:latest --bigquery-table public-data-finance.crypto_band.oracle_requests --timestamp-field block_timestamp_truncated \
  --start-timestamp 2021-05-01T00:00:00 --end-timestamp 2021-05-01T00:03:00 --batch-size-in-seconds 3600 --replay-rate 1 --pubsub-topic \
  projects/${project}/topics/bigquery-to-pubsub-test0 --temp-bigquery-dataset ${temp_resource_name} --temp-bucket ${temp_resource_name}
```

### D. Vérifier que l'insertion en base fonctionne correctement depuis BigQuery et en temps réel: <br>

Dans cette étape, il faut vérifier que l'insertion dans la table temp_crypto_batch.streaming_timestamp fonctionne correctement. <br>
Pour cela, juste regarder s'il y a bien des insertions en temps réel dans cette table. <br>
Vous pouvez aussi lancer la requête suivante plusieurs fois et voir que le timestamp max évolue car l'insertion se fait en temps-réel. <br>

``` bash
  SELECT max(timestamp) FROM `crypto-beta-test.temp_crypto_batch.streaming_timestamp` LIMIT 1
```

### E. Mise en place de la souscription sur le cloud (dataflowrunner()): <br>
Continuer le notebook Streaming_Souscription et suivre les étapes à partir de la partie 3 inclue (sans la partie 4). 
<br>

### F. Relancer la publication de messages (à partir de la minute suivant le maximum du batch): <br>

``` bash
  docker run -v $PATH_DOCK/:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/credentials_file.json
  bigquery-to-pubsub:latest --bigquery-table public-data-finance.crypto_band.oracle_requests --timestamp-field block_timestamp_truncated
  --start-timestamp 2021-05-01T00:03:00 --end-timestamp 2021-05-01T00:06:00 --batch-size-in-seconds 3600 --replay-rate 1 --pubsub-topic 
  projects/${project}/topics/bigquery-to-pubsub-test0 --temp-bigquery-dataset ${temp_resource_name} --temp-bucket ${temp_resource_name}
```

### G. Vérifier que l'insertion en base fonctionne correctement depuis BigQuery et en temps réel: <br>
-Prévoir un temps de latence sur les premières insertions le temps de la mise en place du dataflow en mode streaming <br>
-Dans ce cas-là, préférer le lancement de la souscription depuis la VM ou alors passer directement à l'étape d'AI Platform <br>


### H. Lancement de la publication de messages en temps-réel directement dans le cloud (sans la VM) et avec AI Platform : 

Pour les répétitions de longue durée, il est pratique de démarrer le processus directement dans le Cloud, sans être dépendant d’un terminal local ou d’un terminal d’une VM sur le Cloud (Cloud Shell ou VM créée). 
Pour cela, l’idéal est d’utiliser AI Platform. Conçu pour entrainer les modèles sur le cloud, AI Platform permet également de faire tourner notre framework python de manière « serverless » sur le cloud. <br>

#### 1. Créer un docker pour notre image et le pusher sur Container Registry: <br>

``` bash
  cp credentials_file.json bigquery-to-pubsub/credentials_file.json
  cd bigquery-to-pubsub
```

ouvrir le Dockerfile <br>
rajouter ligne 4 : ENV GOOGLE_APPLICATION_CREDENTIALS=credentials_file.json <br>
<br>

``` bash
  docker build -t bigquery-to-pubsub:latest -f Dockerfile .
  docker image ls
  docker tag bigquery-to-pubsub:latest gcr.io/${project}/bigquery-to-pubsub:aiplatform
  gcloud services enable containerregistry.googleapis.com
  gcloud auth configure-docker puis Y
  docker push gcr.io/${project}/bigquery-to-pubsub:aiplatform
```

#### 2.Depuis le notebook, relancer le streaming sur Dataflow (DataflowRunner()) (Uniquement la partie 3) <br>

IMPORTANT : AVANT DE LANCER LA COMMANDE SUIVANTE, VERIFIER QUE LA SOUSCRIPTION EST TOUJOURS EN COURS DE FONCTIONNEMENT SUR DATAFLOW
SINON RELANCER UNE SOUSCRIPTION (PARTIE 3 NOTEBOOK UNIQUEMENT)

#### 3.Lancement de la publication sur AI Platform : <br>

``` bash
  gcloud services enable ml.googleapis.com
```

Dans la commande suivante, venir changer l'image dockerisée par celle créée précédemment <br>
(aller directement dans Container Registry).

``` bash
  gcloud ai-platform jobs submit training auto_streaming_with_ai_platform \
  --region us-central1 \
  --master-image-uri gcr.io/verdant-cargo-321713/bigquery-to-pubsub@sha256:69565ff4626ba9859b3b6d0c17b11702f9f5b5fe4c03eea5b1ca384683bbe4b3 \
  -- \
  --bigquery-table public-data-finance.crypto_band.oracle_requests \
  --timestamp-field block_timestamp_truncated \
  --start-timestamp 2021-05-01T00:06:00 \
  --end-timestamp 2021-05-01T00:09:00 \
  --batch-size-in-seconds 3600 \
  --replay-rate 1 \
  --pubsub-topic projects/${project}/topics/bigquery-to-pubsub-test0 \
  --temp-bigquery-dataset ${temp_resource_name} \
  --temp-bucket ${temp_resource_name}
```

***A ce stade, vous devriez avoir déployé sur la plateforme GCP le schéma d'architecture suivant :***

![](images/architecture_visualisation_temps_reel.png)

### I. Automatisation du lancement de la souscription et de la publication par une cloud function : <br>

***L'objectif est maintenant de déployer l'architecture suivante :***

![](images/architecture_automatisee_visualisation_temps_reel.png)

#### 1. Dans Streaming_Souscription.ipynb (automatisation de la souscription), créer le template associé au streaming <br>
(Partie 4. Automatisation avec Cloud Functions : Création du template) <br>

#### 2. Créer un topic pub/sub (qui servira à déclencher la cloud function): <br>

``` bash
  gcloud pubsub topics create notificationStreamingCanBegin
```

#### 3. Dans le main de la cloud function, venir changer l'image dockerisée par celle créée précédemment <br>
(aller directement dans Container Registry) :

``` bash
  cd cloud_function_ai_platform
```

#### 4. Lancer la cloud function <br>

``` bash
  gcloud services enable cloudfunctions.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud functions deploy StreamingCanBegin \
  --runtime=python37 \
  --trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \
  --trigger-resource=notificationStreamingCanBegin \
  --entry-point=hello_pubsub \
  --timeout=540
```

#### 5. Faire un test de la fonction (éteindre la souscription Dataflow à la fin de la publication). <br>
La publication se fait sur une fenêtre de 4 minutes (peut-être changer manuellement dans le main de la cloud function). <br>
Dans le cadre de nos essais, comme nous avons déjà commencé à remplir la table streaming précédemment, la cloud function vient regarder le maximum de timestamp de la table en streaming (au lieu de la table batch normalement). Cete ligne pourra être changée par l'utilisateur à la demande (dans le main de la cloud function). <br>

#### 6. Activation de la fonction cloud grâce aux logs de BigQuery et à StackDriver Logging. <br>
L'objectif est d'activer le streaming dès la fin du batch.<br>
<br>
Dans la console de GCP, allez sur StackDriver Logging puis Logs Router. <br>
Cliquer sur CREATE SINK <br>

``` bash
  Sink Name : StreamingCanBegin 
  Description : Batch part has just finished. We can launch the streaming. 
  Service : Cloud Pub/Sub topic 
  Destination : notificationStreamingCanBegin 
  Inclusion filter : 
  resource.type="bigquery_resource" 
  protoPayload.methodName="jobservice.jobcompleted" 
  protoPayload.serviceData.jobCompletedEvent.eventName="load_job_completed" 
  protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.datasetId="temp_crypto_batch" 
  protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId="batch_timestamp" 
```

Puis CREATE SINK <br>
<br>

### J. Mise en place d'alertes pour une reprise du signal <br>

***L'objectif est maintenant de déployer l'architecture suivante :***

![](images/architecture_finale_visualisation_temps_reel_comportant_les_alertes.png)

1. Créer un topic pub/sub (servira à déclencher la cloud function): <br>

``` bash
  gcloud pubsub topics create notificationTopicSignalIsAbsent
```

2. Sur Stackdriver Monitoring, dans Alerting, créer une alerte qui publiera un message dans le topic en cas d'absence de signal : <br>

``` bash
  Find resource type and metric : Cloud Pub/Sub Topic 
  Metric : Publish message operation
  Filter : topic_id = bigquery-to-pubsub-test-0 
  Period : 1 minute 
  Advance Options : Aligner : MEAN 
  Conditon : Is Absent 
  Who should be notified ? Manage Notification Channels (dans un autre onglet) 
  Pub Sub : Alert Signal Is Absent 
  Topic : projects/verdant-cargo-321713/topics/notificationTopicSignalIsAbsent 
  Alert Name : Pub-Sub-Crypto-Flow-Is-Absent-Or-Too-Low 
  Then, SAVE and DISABLED Alert until Cloud Function est opérationnel. 
```

#### 3. Connecter un canal pubsub <br>

https://cloud.google.com/monitoring/support/notification-options?_ga=2.177843820.-872056851.1620303234 <br>

#### 3bis. Permission Denied : Venir donner l'autorisation à Stackdriver Monitoring de publier un message (depuis Cloud Shell) : <br>

``` bash
  project=$(gcloud config get-value project 2> /dev/null)
  projectId=$(gcloud projects describe $project | grep 'projectNumber' 2> /dev/null) 
  echo ${projectId:16:12} 
  gcloud pubsub topics add-iam-policy-binding \
  projects/${projectId:16:12}/topics/notificationTopicSignalIsAbsent --role=roles/pubsub.publisher \
  --member=serviceAccount:service-${projectId:16:12}@gcp-sa-monitoring-notification.iam.gserviceaccount.com
```

Result : <br>
Updated IAM policy for topic [notificationTopicSignalIsAbsent]. <br>
bindings: <br>
- members: <br>
  - serviceAccount:service-594901883524@gcp-sa-monitoring-notification.iam.gserviceaccount.com <br>
  role: roles/pubsub.publisher <br>
etag: BwXIPwHm4jk= <br>
version: 1 <br>

#### 4. Nous allons mettre en place la cloud function responsable des alertes, dans le dossier cloud_function_alerting, le main de la cloud function, venir changer l'image dockerisée par celle créée précédemment <br>
(aller directement dans Container Registry) 

#### 5. Lancer la cloud function <br>

``` bash
  cd cloud_function_alerting
  gcloud services enable cloudfunctions.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud functions deploy RelauchStreaming \
  --runtime=python37 \
  --trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \
  --trigger-resource=notificationTopicSignalIsAbsent \
  --entry-point=hello_pubsub \
  --timeout=540
```

#### 6. Tester la cloud function (relancer une souscription depuis DataflowRunner() car nous avions éteint la précédente le temps de la mise en place de cette architecture. En temps normal, la souscription, Streaming DataflowRunner() ne serait jamais éteinte et serait toujours opérationnelle). Vérifier le bon fonctionnement attendu. 

#### 7. Enable l'alerte. L'alerte met un peu de temps à comprendre que le signal est opérationnel. Attendre un peu et, si un job est lancé trop tôt, le supprimer manuellement. <br> L'incident se fermera de lui-même car le signal est déjà existant. Il n'y aura plus de problème par la suite et le fonctionnement devrait être au cohérent. 

### K. Mise en place de la visualisation temps-réel dans Data-Studio. <br>

![](images/visualisation_temps_reel_simule_du_cours_du_Bitcoin.png)

#### 1. Sur Google Chrome, installer l'extension Data Studio Auto Refresh. <br>

#### 2. Aller sur Data Studio : https://datastudio.google.com/ <br>
   Appuyez sur Créer et choisissez Report <br>
   Connecter une source BigQuery <br>
   Autoriser <br>
   Source : <br>
   <br>
   SELECT * FROM `temp_crypto_batch.batch_timestamp` <br>
   WHERE timestamp >= '2021-04-20 00:00:00.000000 UTC' <br>
   UNION ALL <br>
   SELECT * FROM `temp_crypto_batch.streaming_timestamp` <br>
   <br>
   Supprimer le tableau <br>
   <br>
   Renommer le dashboard : Visualisation temps-réel <br>
   <br>
   Add a chart <br>
   <br>
   TimeSeries <br>
   <br>
   Dimension : Date Hour Minute <br>
   <br> 
   Metric : AVG (rate) <br>
   <br>
   Style : General : Missing Data : Linear Interpolation <br>
   <br>
   Add a Date Range Control <br>
   <br>
   Add a Drop Down List : <br>
   - Control Field : Symbol <br>
   - Metric : CT (Count) Symbol <br>
   <br>
   Add an Advanced Filter : <br>
   - Create Field <br>
   - Name : Start DateTime <br>
   - Formula : CAST(date_str as NUMBER) <br>
   - Style : Search Type : >= <br>
   <br>
   Theme And Layout : Simple Dark <br>
   <br>
   Détailler les filtres et le graphe. Ne pas oublier l'histoire de >= et l'interpolation <br>
   <br>
   Appuyer sur View : <br>
   - Ne pas toucher à Select Date Range <br>
   - Format du zoom : 20210430230000 <br>
   - Choisir une monnaie : Exemple, ETH <br>
   <br>
   Data Studio Refresh : Refresh Every 60 secondes. <br>
   <br>

### L. Rester sur le Data Studio et attendre que le signal reprenne (si jamais il n'est pas là) automatiquement grâce à l'alerte. <br> 
NB : Entre la perte de signal et la reprise, il faut quelques minutes à l'alerte pour déclencher un incident et donc la reprise du signal. <br>
Dans un objectif futur (perspectives), il faudrait trouver une solution pour que ce temps d'attente soit inexistant. <br>
<br>

### M. Disabled l'alerte et éteindre le dataflow streaming pour éviter des surcharges. <br>

Nous ne les utiliserons pas pour la partie 3.Apprentissage et 4.API. <br>
<br>




