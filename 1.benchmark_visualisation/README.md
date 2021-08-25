# 1.benchmark_visualisation

Le jeu de données source utilisé provient de l’ensemble de données publiques Band Protocol directement disponible dans BigQuery.  La plateforme GCP défini cet ensemble de données de la manière suivante : « Band Protocol is a cross-chain data oracle platform that aggregates and connects real-world data and APIs to smart contracts. Band's flexible oracle design allows developers to query any data including real-world events, sports, weather, random numbers and more. Developers can create custom-made oracles using WebAssembly to connect smart contracts with traditional web APIs within minutes. BandChain is designed to be compatible with most smart contract and blockchain development frameworks. It does the heavy lifting jobs of pulling data from external sources, aggregating them, and packaging them into the format that’s easy to use and verified efficiently across multiple blockchains. This dataset is one of many crypto datasets that are available within the Google Cloud Public Datasets ».
<br>

Dans BigQuery, après avoir sélectionné le Dataset Band Protocol, ici « crypto_band », nous nous intéressons plus particulièrement à la table oracle_requests qui rassemble les valeurs de cours de cryptomonnaie avec des timestamps associés. 

![](images/description_table_source.png)

Comme on peut déjà le voir, la table est très lourde. Elle contient des données depuis la fin d’année 2020 et la veille. Tous les jours, la table est alimentée par les données du jour précédent. Cette information est très importante : ***la table n’est pas alimentée en temps-réel***. Ainsi, pour reproduire un comportement temps-réel, il nous faudra simuler le streaming à partir du batch. Notons que la table est également alimentée par des scripts venant complétés des données manquantes sur certaines périodes de temps. 
<br>

Une requête SQL (voir Annexe 9. Visualisation des cours de cryptomonnaie depuis BigQuery) permet d’obtenir un format désagrégé et où l’on peut visualiser ce que contient réellement la table. <br>

![](images/donnees_desagregees_visualisees_directement_dans_BQ.png)

Dans la console, choisissez BigQuery. <br>

Cliquez sur Done. <br>

Visualisez les données en copiant la requête contenue dans requet_sql_visualisation.sql <br>

Cliquez sur RUN. <br>

Visualisez les données. <br>

A l’instant où cette requête a été exécutée, la table contenait moins de 10 mois de données (Novembre 2020 – Juillet 2021) et pourtant, cela correspondait à presque 52 millions de ligne, une fois la table désagrégée.
