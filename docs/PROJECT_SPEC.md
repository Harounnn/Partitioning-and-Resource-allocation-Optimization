# Spécification du projet

## Sujet
Optimiser le nombre de ressources et le partitionnement pour analyser des données réelles de capteurs.

## Problématique
Comment choisir une configuration de calcul qui respecte un délai de traitement tout en limitant les ressources consommées, sans modifier la qualité des résultats ?

## Périmètre
- Calcul batch.
- Infrastructure homogène, mais expérimentation disponible sur une seule machine physique.
- Comparaison Python local et PySpark local.
- Préparation des données pour le datamining.
- Aucun modèle complet de prédiction de panne exigé.
- Kafka, streaming et modèles profonds hors périmètre obligatoire.

## Questions
- Q1 : seuil de volume où Spark devient préférable à Python local optimisé.
- Q2 : influence du parallélisme et des partitions sur durée, mémoire et échanges.
- Q3 : capacité d'une règle de sélection à réduire les ressources tout en respectant une échéance sur des lots de test.

## MetroPT-3
Agrégation par fenêtre de cinq minutes :
- nombre de mesures ;
- moyenne, écart-type population (diviseur N) et maximum de température ;
- pression moyenne ;
- variation de température depuis la fenêtre précédente.

Vérifier la cadence réelle à partir des horodatages. Les incidents documentés ne sont pas des étiquettes de panne pour chaque ligne.

## UK-DALE
Décoder et calibrer les signaux selon la documentation du producteur. Calculer par seconde la tension efficace et le courant efficace. Gérer les fenêtres coupées entre fichiers et les périodes manquantes.

Pour un signal x de N échantillons :
RMS(x) = sqrt(sum(x_i^2) / N)

## Exactitude
Comparer les résultats Python et Spark par clé de fenêtre et par indicateur. Le sujet propose comme exemple de tolérance :
|a - b| <= 10^-8 + 10^-5 |a|
Cette tolérance doit être adaptée et justifiée selon les unités et l'amplitude.

Pour agréger des blocs, conserver les sommes et les effectifs. Ne pas calculer une moyenne globale comme moyenne non pondérée des moyennes de blocs.

## Performance
S(p) = T(1) / T(p)
E(p) = S(p) / p
D = V / T

Dans l'environnement mono-machine, interpréter p comme parallélisme local c uniquement si cela est explicitement indiqué ; ne pas prétendre mesurer des nœuds physiques.

## Sélection
Le guide du sujet définit C(p,P) = p × T(p,P) en nœuds-minutes pour un cluster homogène. Cette interprétation ne s'applique pas directement à une machine locale. Tout proxy local doit être nommé et justifié, par exemple c × T en threads-secondes, sans le confondre avec une consommation énergétique ou une facture cloud.

Sélectionner une configuration estimée respectant :
T_hat(c,P) + m <= L
puis minimiser le proxy de ressources choisi. Estimer m à partir d'erreurs sur des lots de validation. Évaluer sans nouveau réglage sur des lots de test.

## Mesures
- durée de bout en bout jusqu'à l'écriture effective ;
- mémoire maximale ou indicateur mémoire disponible ;
- octets shuffle ;
- spill disque ;
- échecs ;
- volume d'entrée, nombre d'échantillons et taille Parquet de sortie.

Distinguer coût de téléchargement/préparation et coût du traitement chronométré.
