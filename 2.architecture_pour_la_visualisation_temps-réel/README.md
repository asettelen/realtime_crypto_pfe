# Architecture pour la visualisation temps-réel 

![](images/architecture_visualisation_temps_reel.png)

Sur cette première architecture, on distingue ***deux branches*** : <br>
•	Une première branche (en bas) qui va s’occuper de traiter les données contenues dans la table sur les dix derniers mois (jusqu’à une date bien déterminée) : Il s’agit ici de mettre en place une ***pipeline Batch***. <br>
•	Une seconde branche (en haut) qui va s’occuper de traiter les données en continu. Comme indiqué précédemment, la table n’est pas alimentée en temps-réel, il va donc s’agir de simuler le comportement d’une ***pipeline Streaming à partir de rejeu (Batch répété)***. Ce rejeu sera réalisé à partir de la date maximale correspondant aux données Batch. L’insertion en temps-réel va également permettre d’avoir une visualisation en temps-réel. <br>
