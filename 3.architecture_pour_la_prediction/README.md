# Déploiement d'une architecture de prédiction

Dans l'objectif de mettre en place notre apprentissage automatique, nous devons passer par un certain nombre d'étapes.

## Si ce n'est pas déjà fait, créer le bucket correspondant :

``` bash
project=$(gcloud config get-value project 2> /dev/null)
echo gs://temp_crypto_batch_${project//-/_}
gsutil mb gs://temp_crypto_batch_${project//-/_}
```

## Voici toutes les étapes à suivre pour mettre en place la prédiction : 

### A. Constats avant mise en place de la prédiction : Interpolation, Windowing et coût de pub/sub et l’insertion en temps-réel dans BigQuery

***Windowing et Interpolation nécessaires pour la mise en place de l’apprentissage***

Pour pouvoir prédire les valeurs futures des 213 cryptomonnaies, encore faut-il déterminer ce que l’on souhaite prédire et à quelle fréquence. Si l’on souhaite prédire des valeurs futures par heure, il faut faudra alors mettre en place des fenêtres sur nos données initiales : Au lieu de conserver les timestamps comme donnés d’entrée, nous allons les regrouper et ***obtenir une valeur moyenne par heure (et par cryptomonnaie)***. Pour cela, il nous faut être sûr que nous avons dans notre table au moins une valeur pour chaque cryptomonnaie et par heure. 
En prenant l’exemple de l’Ethereum sur le mois de novembre, après fenêtrage d’une heure, nous arrivons donc au résultat suivant (visuel) : 

![](images/fenetrage_par_heure_sans_interpolation.png)

***Nous constatons qu'il n'y a pas présence d'une valeur sur chaque fenêtre d'une heure (il existe des fenêtres vides) donc il est nécessaire de mettre en place de l'interpolation en plus du fenêtrage.***

#### 1. Création des fonctions d'interpolation

Depuis la vm créée dans une des parties précédentes, créer un dataset "timeseries"

``` bash
  cd /home/jupyter/realtime-crypto-viz-and-pred/3.architecture_pour_la_prediction/3.1.interpolation
  bq mk --location US --dataset timeseries
```

#### 2. Mise en place des 4 fonctions qui nous permettront de mettre en place l'interpolation. 
Executer dans la console BigQuery fonction_interpolation.sql <br>
Pour plus d'infos, lire : https://medium.com/google-cloud/time-series-analytics-with-bigquery-f65867c1ce74

#### 3. Executer les scripts SQL (pour obtenir le streaming et la validation) dans BigQuery dans cet ordre : 
- simulate_daily_streaming.sql
- simulate_validation.sql

#### 4. Une fois les requêtes précédentes terminées, appel des fonctions de windowing et d'interpolation sur batch + streaming + validation, executez dans BigQuery : 
- windowing_and_interpolation_lauching.sql

Après mise en place de l'interpolation, si l'on revisualise le graphique précédent (Cours de l'Ethereum sur le mois de Novembre) : 

![](images/fenetrage_par_heure_avec_interpolation.png)

Conséquence, avec l’***interpolation, il n’y a plus de valeur manquante en base***. Nous avons une valeur par heure et par un cour de cryptomonnaie, on peut donc imaginer appliquer des modèles d’apprentissage automatique sur nos données, prêtes à l’emploi. 

***Coûts exubérants de cloud pub/sub et de l’insertion en temps-réel dans BigQuery nous amène à repenser l’architecture globale***

Notre architecture temps-réel fonctionnant (partie précédente ***2.architecture_pour_la_visualisation_temps-reel***), tout semblait bon pour faire une interpolation (quotidienne ou plusieurs fois par jour) des données et lancer des modèles d’apprentissage automatique dès que possible. 
Cependant, il est important de noter que ce projet était un POC (Prove of Concept), le travail sur GCP a été fait avec avec un compte d’essai. Celui-ci ne me fournissait que 254.5 euros pour mes essais. Hors, laisser tourner pub/sub + ai platform + dataflow streaming +  bigquery reviendrait extrêmement cher (METTRE PHOTO SIMULATEUR GCP A L’APPUIE). <br>

C’est en ce sens qu'il a fallu ***repenser l’architecture (uniquement pour la partie prédiction) de telle sorte à minimiser les coûts***. Cette architecture ne fait pas appel à une insertion en streaming (dans Cloud Pub/Sub), ne lance pas le framework de données en streaming (dans AI Platform), n’a pas besoin d’un traitement en temps-réel (dans dataflow streaming) et n’a pas besoin d’être insérée en streaming dans BigQuery. Et pourtant, cela nous permet d’***avoir exactement le même résultat dans notre objectif de mise en place d’apprentissage automatique***. L’idée est de récupérer la même quantité d’informations qui aurait été générée en streaming (batch répété) mais cette fois-ci de récupérer ces données en une seule fois (une fois par jour dans notre cas) et donc d’éviter toute surcharge de coût lié à la mise en place du streaming. A cela, on vient rajouter également des données de validation pour comparer avec les données prédites.

En conséquence, voici l'architecture réelle pour la prédiction, tenant compte de l'interpolation, du fenêtrage et de la réduction des coûts : 

![](images/architecture_reelle_pour_la_prediction.png)

### B. Choix des deux modèles utilisés pour la prédiction : FbProphet et Arima

Pour mettre en place de l’apprentissage automatique à partir de nos données, il m’a fallu prendre certaines considérations (en fonction du temps que j’avais pour me concentrer sur la prédiction, son implémentation et son automatisation, à savoir trois semaines) : <br>
-	***Type de modèles d’apprentissage*** utilisé à savoir machine learning ou deep learning <br>
-	Modèles prêts à l’emploi (API, librairies) ou modèles à implémenter de zéro <br>
-	***Fréquence d’apprentissage*** et nombre de prédictions futures <br>

Considérant que je devais aussi me concentrer sur l’intégration de modèles sur des pipelines ainsi que leur automatisation, j’ai choisi de me concentrer sur deux modèles de machine learning, associant dépendance entre données et temporalité, prêts à l’emploi : ***La librairie FbProphet, implémentée sur une pipeline de données et le modèle ARIMA, implémenté directement sur BigQueryML par un simple appel d’API***.  
Le choix de modèles de machine learning se justifie aussi car les données ne sont pas si nombreuses par cours de crypto. Une interpolation par minute (au lieu d’une interpolation horaire) aurait pu, dans ce cas, nécessité l’utilisation de modèles de deep learning. 

### C. Apprentissage automatique avec FbProphet sur Cloud Dataproc (Apache Spark) 

***Justification du choix d'Apache Spark pour le développement de la pipeline***

Les données présentent en base correspondent à 213 cours de cryptomonnaie. Après interpolation (horaire), cela correspond tout de même à plus de 5000 valeurs (24 valeurs de plus chaque jour) d’entraînement par cours de cryptomonnaie. Les prédictions étaient considérées dans notre cas de manière indépendante pour chacun des cours de cryptomonnaie, il est d’autant plus pertinent de profiter de la plateforme fournie par GCP pour effectuer de la parallélisation en distribuant la charge sur plusieurs nœuds appartenant à un même cluster. <br>

Sur Cloud Dataproc (cluster Hadoop sur GCP), les paradigmes Map Reduce et Spark étant les deux principales technologies utilisées, je me suis finalement orienté vers ***Spark pour sa rapidité*** (stockage en mémoire cache des données contrairement à Hadoop Map Reduce dont la multiplication des appels de lecture et d’écriture sur disque auraient pu se faire sentir lors du clean et de l’apprentissage). ***100 fois plus rapide que Map Reduce, Spark et sa simplification dans le requêtage, son interactivité et sa flexibilité*** (RDD, Spark DataFrame et Spark SQL, Spark DataFrame API) et l’apprentissage mis-en-place directement sur les nœuds. 

***Configuration du cluster Spark (éphémère) et justification des paramètres***

L’idée est, qu’à raison d’une fois quotidiennement, dès mise-à-jour de notre table de données (batch + streaming + validation), il y ait provisionnement d’un cluster Spark de manière automatique (encore une fois à l’aide d’une cloud function). Ce cluster ne va être provisionné que pour l’exécution de notre job (entrainement de notre modèle). Aussitôt l’entrainement terminé, le cluster se supprimera pour éviter toute dépense inutile de crédits grâce au paramètre « idle_delete_ttl ».

#### 1. Penser à activer l'API de Cloud Dataproc avant lancement depuis la Cloud Function

``` bash
cd /home/jupyter/realtime-crypto-viz-and-pred/3.architecture_pour_la_prediction/3.2.prediction_cloud_dataproc_prophet
gcloud services enable dataproc.googleapis.com
```

#### 2.Copier le job spark_prophet_ml.py vers notre bucket : 

``` bash
project=$(gcloud config get-value project 2> /dev/null)
gsutil cp spark_prophet_ml.py 'gs://temp_crypto_batch_'${project//-/_}'/prediction/spark_prophet_ml.py'
```

#### 3. Créer un topic pub/sub (qui déclenchera la cloud function)

``` bash
gcloud pubsub topics create notificationModelNeedToBeRetrained
```

#### 4. Lancer la cloud function

``` bash
gcloud functions deploy DataprocRetrainFbProphetModel \
--runtime=python37 \
--trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \
--trigger-resource=notificationModelNeedToBeRetrained \
--entry-point=trigger_spark_job \
--timeout=540
```

#### 5. Tester la cloud function (Si version d'essai, alors la valeur max de CPU pour une région est de 8. Or le cluster dataproc nécessite 6 CPU et la VM (Apache-crypto) 4 CPU donc il faut fermer la VM)

#### 6. A la fin du dataproc, vérifier la bonne exécution de la requête SQL suivante :  

``` bash
SELECT
  s.symbol, s.ds, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', s.ds) AS date_str, 
  s.yhat, s.yhat_lower, s.yhat_upper, 
  if (b.tumble <= (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_spark_global`), b.intrp, NULL) as y, 
  if (b.tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_spark_global`), b.intrp, NULL) as intrp, 
FROM
  `temp_crypto_batch.test_spark_global` s
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble <= (SELECT max(ds) FROM `temp_crypto_batch.test_spark_global`)
) b
ON
b.tumble = s.ds AND s.symbol = b.symbol;
```

Si elle fonctionne, votre modèle prophet sur cloud dataproc est fonctionnel, passez à FbProphet sur Dataflow et à Arima sur BigQueryML.

### D. Apprentissage automatique avec FbProphet sur Cloud Dataflow (Apache Beam)

Ayant réussi à exécuter Prophet sur un cluster Dataproc, il m’a également été possible de le lancer sur Cloud Dataflow, solution serverless, sans aucune maintenance de cluster à gérer. Pour cela, il a fallu adapter le code en créant une pipeline avec Apache Beam et en utilisant des templates pour pouvoir lancer son exécution depuis une cloud function. 

#### 1. Ouvrir Cloud_Function_Dataflow.ipynb, puis exécuter toutes les cellules pour sauvegarder le template

Choisir le noyau Apache Beam 2.25.0 for Python 3

#### 2. Lancer la cloud function 

``` bash
cd /home/jupyter/realtime-crypto-viz-and-pred/3.architecture_pour_la_prediction/3.3.prediction_cloud_dataflow_prophet
gcloud functions deploy DataflowRetrainProphetModel \
--runtime=python37 \
--trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \
--trigger-resource=notificationModelNeedToBeRetrained \
--entry-point=hello_pubsub \
--timeout=540
```

#### 3. Tester la cloud function

#### 4. A la fin du dataflow, vérifier la bonne exécution de la requête SQL suivante : 

``` bash
SELECT
  p.symbol, 
  p.ds,  
  FORMAT_TIMESTAMP('%Y%m%d%H%M%S', p.ds) AS date_str, 
  p.y,  
  p.yhat, 
  p.yhat_lower, 
  p.yhat_upper, 
  b.intrp
FROM
`temp_crypto_batch.test_prophet_global` p
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_prophet_global`)
AND tumble <= (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`)
) b
ON
b.tumble = p.ds AND p.symbol = b.symbol
;
```

Si la requête fonctionne, passez à l'étape suivante avec Arima sur BigQueryML

### E. Apprentissage automatique avec ARIMA utilisant BigQueryML (GCP Native)

Pour le modèle ARIMA (Auto), l’idée est de profiter des modèles existants directement sur la plateforme GCP et notamment accessibles par BigQueryML.
Ainsi, pour utiliser ces modèles (payant à chaque exécution, voir partie suivante), il suffit de les appeler par une commande SQL directement dans BigQuery et d’attendre la fin de l’exécution. 

#### 1. Lancer la cloud function 

``` bash
cd /home/jupyter/realtime-crypto-viz-and-pred/3.architecture_pour_la_prediction/3.4.prediction_cloud_bigqueryml_arima
gcloud functions deploy BQMLRetrainArimaModel \
--runtime=python37 \
--trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \
--trigger-resource=notificationModelNeedToBeRetrained \
--entry-point=hello_pubsub \
--timeout=540
```

#### 2. Tester la cloud function

#### 3. Vérifier la bonne execution de l'entrainement du modèle avec cette commande : 

``` bash
bq ls -j -a --max_results 100
```

#### 4. A la fin de bigqueryml, vérifier la bonne exécution de la requête SQL suivante :  

``` bash
SELECT
 a.symbol, 
 time_series_timestamp, 
 FORMAT_TIMESTAMP('%Y%m%d%H%M%S', time_series_timestamp) AS date_str, 
 if (time_series_timestamp <= (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            ), time_series_data, NULL) as y, 
 if (time_series_timestamp > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            ), time_series_data, NULL) as yhat,
 prediction_interval_lower_bound as yhat_lower, 
 prediction_interval_upper_bound as yhat_upper, 
 b.intrp
FROM
 ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                     STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            )
AND tumble <= (SELECT max(time_series_timestamp) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            )
) b
ON
b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
; 
```

Si bon, passez à l'automatisation et au dashboard.

### F. Automatisation avec Stackdriver Logging et BigQueryScheduler

***L'objectif est de mettre en place l'architecture suivante (reposant pour beaucoup sur les parties précédentes)*** :

![](images/automatisation_prediction.png)

#### 1. Stackdriver Logging

``` bash
Dans la console de GCP, allez sur StackDriver Logging puis Logs Router. 
Cliquer sur CREATE SINK
Sink Name : modelNeedToBeRetrained
Description : when the interpolation is over and windowing is finished, we need to publish a message into a pub/sub topic to trigger then the cloud functions responsible to retrain the model.
Service : Cloud Pub/Sub topic
Destination : notificationModelNeedToBeRetrained
Inclusion filter : 
resource.type="bigquery_resource"
protoPayload.methodName="jobservice.jobcompleted"
protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"
protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId="temp_crypto_batch"
protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId="batch_hour_interpo"

Puis CREATE SINK
```

#### 2. BigQueryScheduler 

***Depuis la console BigQuery :*** 

``` bash
Enable Schedule Queries 
Enable API
```

***BigQuery Scheduler :***

``` bash
CREATE NEW SCHEDULE QUERY
Programmer les 3 requêtes (d'abord Simulate_Daily_Streaming.sql puis Simulate_Validation.sql puis
5 mn après Windowing_And_Interpolation_Launching.sql)
```

### G. Détails d'implémentation du dashboard sur DataStudio : 

***Dashboard obtenu à la fin***

![](images/visualisation_prediction.png)

#### 1. Rename the Dashboard : Automated_ML_Prophet_And_ARIMA_Time_Series_Modelling

#### 2. Theme and Layout : Lagoon

#### 3. Dashboard Prophet - Dataproc 

 - Add Data Source : (la requête à tester pour Dataproc : voir ***C.6***)
 - Create a timeseries chart 
 - Dimension : Date Hour
 - Metric : AVG(y) en bleu, AVG(yhat) en vert, AVG(yhat_lower) en orange, AVG(yhat_upper) en orange, AVG(intrp) en jaune
 - Dans Style : Missing Data line Break
 - Add a Title : PROPHET DATAPROC
 
#### 4. Dashboard Prophet - Dataflow 

 - Add Data Source : (la requête à tester pour Dataflow : voir ***D.4***)
 - Create a timeseries chart 
 - Dimension : Date Hour
 - Metric : AVG(y) en bleu, AVG(yhat) en vert, AVG(yhat_lower) en orange, AVG(yhat_upper) en orange, AVG(intrp) en jaune
 - Dans Style : Missing Data line Break
 - Add a Title : PROPHET DATAPROC
 
#### 5. Dashboard Arima - BigqueryML 

 - Add Data Source : (la requête à tester pour BigqueryML : voir ***E.4***)
 - Create a timeseries chart 
 - Dimension : Date Hour
 - Metric : AVG(y) en bleu, AVG(yhat) en vert, AVG(yhat_lower) en orange, AVG(yhat_upper) en orange, AVG(intrp) en jaune
 - Dans Style : Missing Data line Break
 - Add a Title : PROPHET DATAPROC
 
#### 6. RMSE - DATAPROC

- Add a scorecard 
- Add a datasource with the sql query that follows :  

``` bash
SELECT
  s.symbol, s.ds, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', s.ds) AS date_str, 
  s.yhat, s.yhat_lower, s.yhat_upper, 
  if (b.tumble <= (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_spark_global`), b.intrp, NULL) as y, 
  if (b.tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_spark_global`), b.intrp, NULL) as intrp, 
FROM
  `temp_crypto_batch.test_spark_global` s
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble <= (SELECT max(ds) FROM `temp_crypto_batch.test_spark_global`)
) b
ON
b.tumble = s.ds AND s.symbol = b.symbol
WHERE ds > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_spark_global`);
``` 

- Create metric : rmse_dataproc. Formula : SQRT(AVG(POWER(intrp-yhat, 2)))

- Copy the scorecard two times : 
- Create metric : rmse_upper_dataproc. Formula : SQRT(AVG(POWER(intrp-yhat_upper, 2)))
- Create metric : rmse_dataproc_lower. Formula : SQRT(AVG(POWER(intrp-yhat_lower, 2)))

#### 7. RMSE - DATAFLOW

- Add a scorecard 
- Add a datasource with the sql query that follows : 

``` bash
SELECT
  p.symbol, 
  p.ds,  
  FORMAT_TIMESTAMP('%Y%m%d%H%M%S', p.ds) AS date_str, 
  p.y,  
  p.yhat, 
  p.yhat_lower, 
  p.yhat_upper, 
  b.intrp
FROM
`temp_crypto_batch.test_prophet_global` p
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_prophet_global`)
AND tumble <= (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`)
) b
ON
b.tumble = p.ds AND p.symbol = b.symbol
WHERE ds > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) FROM `temp_crypto_batch.test_prophet_global`);
``` 

- Create metric : rmse_dataflow. Formula : SQRT(AVG(POWER(intrp-yhat, 2)))

- Copy the scorecard two times : 
- Create metric : rmse_upper_dataflow. Formula : SQRT(AVG(POWER(intrp-yhat_upper, 2)))
- Create metric : rmse_dataflow_lower. Formula : SQRT(AVG(POWER(intrp-yhat_lower, 2)))

#### 8. RMSE - BIGQUERYML

- Add a scorecard 
- Add a datasource with the sql query that follows :  

``` bash
SELECT
 a.symbol, 
 time_series_timestamp, 
 FORMAT_TIMESTAMP('%Y%m%d%H%M%S', time_series_timestamp) AS date_str, 
 if (time_series_timestamp <= (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            ), time_series_data, NULL) as y, 
 if (time_series_timestamp > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            ), time_series_data, NULL) as yhat,
 prediction_interval_lower_bound as yhat_lower, 
 prediction_interval_upper_bound as yhat_upper, 
 b.intrp
FROM
 ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                     STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
LEFT JOIN
(SELECT 
*
FROM  
`temp_crypto_batch.batch_hour_interpo`
WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            )
AND tumble <= (SELECT max(time_series_timestamp) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level))

                            )
) b
ON
b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
WHERE time_series_timestamp > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                                FROM 
                                ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                                STRUCT(120 AS horizon, 0.8 AS confidence_level)))
;
``` 

- Create metric : rmse_arima. Formula : SQRT(AVG(POWER(intrp-yhat, 2)))

- Copy the scorecard two times : 
- Create metric : rmse_upper_dataflow. Formula : SQRT(AVG(POWER(intrp-yhat_upper, 2)))
- Create metric : rmse_dataflow_lower. Formula : SQRT(AVG(POWER(intrp-yhat_lower, 2)))

#### 9. Date Range Control : 

- Add a Date Range Control

- Add a Drop Down List :
  - Control Field : Symbol
  - Metric : None
  - Order : Ascending

Add an Advanced Filter :
- Create Field
- Name : Start DateTime
- Formula : CAST(date_str as NUMBER)
- Style : Search Type : >=

Appuyer sur View :
- Ne pas toucher à Select Date Range
- Format du zoom : 20210430230000
- Choisir une monnaie : Exemple, ETH

