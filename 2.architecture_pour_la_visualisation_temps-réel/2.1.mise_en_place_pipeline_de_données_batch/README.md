# 2.1.mise_en_place_pipeline_de_données_batch

Dans notre architecture, on peut déjà remarquer que le produit Google allant jouer le rôle de ***Data Lake et Data Warehouse est BigQuery***. 
La raison est double (voir Annexe 10. Comment choisir son data lake/son data warehouse ?) : <br>
-	Les données structurées sont initialement accessibles par BigQuery <br>
-	Après traitement, les données resteront structurées et ne nécessiteront pas de latence inférieure à la seconde <br>


Concernant la pipeline et le traitement de données lui-même, il aurait pu être directement fait sur BigQuery (ELT). Le choix de ***Cloud Dataflow pour réaliser le traitement*** se justifie par : 
-	La facilité de réaliser des pipelines Apache Beam à la fois pour le Batch et le Streaming 
-	La possibilité de réaliser des opérations plus complexes avec un langage de programmation (python) simple <br>


Le choix a été fait de travailler directement depuis une machine virtuelle sur GCP (accessible grâce aux 300 dollars offerts) par la plateforme pendant 90 jours. <br>
Cela permet d'éviter des conflits de dépendances possibles localement (si l'on développe sur un IDE en python par exemple). <br>
Néanmoins, cela reste possible et permettrait d'éviter certains frais à l'avenir (en installant les dépendances python adéquates localement) notamment si un développement doit se faire une fois la période d'essai terminée. <br>

- Enable Notebook API (Depuis Dataflow -> Notebook). <br>
- Depuis Dataflow, Notebooks, créer une nouvelle instance : Apache Beam, Without GPU. <br>
- Appelez là apache-beam-crypto-test. <br>
- Lancez jupyterlab et cloner le git depuis un terminal dans jupyterlab (depuis /home/jupyter). <br>
- Allez directement au notebook de ce dossier. <br>
- Choisir comme noyau Apache Beam 2.25.0. <br>
- Lisez attentivement le notebook et executez les commandes jusqu'à lancement de la pipeline batch sur Dataflow. <br>
