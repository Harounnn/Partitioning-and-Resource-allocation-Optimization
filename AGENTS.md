# Instructions du dépôt — Projet Big Data M2 DSIR

## Rôle de Codex
Tu es l'assistant de développement de ce projet universitaire. Travaille de manière incrémentale, vérifiable et adaptée à un ordinateur Windows 10 avec 16 Go de RAM et un seul ordinateur physique.

## Langue
- Les échanges de travail avec l'étudiant peuvent être en anglais.
- Tous les livrables destinés à l'évaluation doivent être rédigés en français : `README.md`, rapport, commentaires/docstrings destinés à être présentés, titres de figures, tableaux, légendes et présentation.
- Le code, les noms de variables, les noms de fonctions et les identifiants techniques restent en anglais.
- Ne produis pas de prose de rapport en anglais.

## Objectif scientifique
Étudier le traitement batch de données réelles de capteurs et déterminer dans quelles conditions Python local ou PySpark local est pertinent. Comparer les performances sans dégrader l'exactitude des indicateurs calculés.

Le projet comporte deux jeux de données traités séparément :
1. MetroPT-3 : chaîne de validation, agrégations par fenêtres de 5 minutes.
2. UK-DALE : étude de signaux électriques haute fréquence, après décodage et calibration documentés, calcul RMS par seconde.

Il ne s'agit pas de construire un modèle de prédiction de panne.

## Contraintes matérielles et interprétation
- Système cible : Windows 10, 16 Go RAM, CPU uniquement, un seul ordinateur physique.
- Ne suppose jamais que le modèle exact du CPU ou l'espace disque sont connus. Demande ou propose une commande locale pour les relever.
- Commence par des lots modestes et un pilote. Ne télécharge pas l'archive complète UK-DALE.
- Ne charge pas inutilement les données complètes en mémoire. Privilégie le traitement par blocs et les sorties Parquet.
- Ne crée jamais de données dupliquées pour simuler une augmentation du volume réel.
- Un seul ordinateur en mode `local[n]` n'est pas un cluster multi-nœuds. Utilise `c` pour le nombre de cœurs/threads d'exécution locaux et `P` pour les partitions de shuffle. N'appelle pas `c` un nombre de nœuds.
- Toute conclusion sur le réseau ou le passage à l'échelle multi-nœuds est hors de portée sans plusieurs machines physiques.

## Méthode
- Implémente d'abord une référence Python correcte, puis une implémentation PySpark équivalente.
- Vérifie les résultats Python/Spark sur un petit échantillon en comparant les clés, les valeurs et une tolérance numérique justifiée.
- Pour MetroPT-3, calcule par fenêtre de 5 minutes : nombre de mesures, moyenne, écart-type (convention population, diviseur N), maximum de température, pression moyenne et variation de température par rapport à la fenêtre précédente.
- Vérifie la cadence réelle dans les horodatages. Ne traite pas les incidents documentés comme des étiquettes de panne par ligne.
- Pour UK-DALE, suis la documentation de calibration du jeu de données. Calcule les RMS tension/courant par seconde. Traite les fenêtres coupées entre fichiers et les périodes manquantes explicitement.
- Conserve un manifeste des fichiers avec chemin logique, date/période, taille et SHA-256 lorsque les fichiers sont disponibles localement.
- Stocke les données préparées en Parquet.
- Sépare les lots de calibration/réglage des lots de test avant toute optimisation.
- Chronomètre le traitement jusqu'à l'écriture effective de la sortie. Mesure séparément téléchargement et préparation.
- Ne rapatrie jamais des données massives avec Spark `collect()`.

## Benchmark local
Commence par un pilote pour déterminer les configurations réalistes. Candidats initiaux possibles :
- Python optimisé (B0)
- Spark `local[1]` (B1)
- Spark `local[2]`
- Spark `local[4]` si le CPU le permet
- `spark.sql.shuffle.partitions` : 8, 16, 32, 64, à réduire/adapter après pilote.

Ne lance pas aveuglément toute la grille. Sur 16 Go RAM, surveille la mémoire et réduis les tailles de lots si nécessaire. Garde constants les données, le stockage, le calcul, les ressources disponibles et les paramètres de cache au sein d'une comparaison.

Enregistre au minimum : durée, configuration, volume d'entrée, nombre d'échantillons, taille de sortie, mémoire maximale si mesurable, octets shuffle, spill disque et échecs. Répète les essais et rapporte médiane et dispersion, pas uniquement le meilleur temps. Mélange l'ordre des essais lorsque cela est raisonnable.

## Sélecteur de configuration
La référence du sujet définit le coût `C = p * T` pour des nœuds physiques homogènes. Dans cette étude mono-machine, ne présente pas `c * T` comme des nœuds-minutes. Si un indicateur local `c * T` est utilisé, nomme-le explicitement « coût proxy en threads-secondes » ou équivalent et explique ses limites.
Définis une échéance `L`, estime `T`, calibre une marge `m` sur des lots de validation et sélectionne une configuration respectant `T_hat + m <= L`. Évalue ensuite sur des lots de test jamais utilisés pour le réglage. Inclue le coût de calibration/sélection dans la discussion.

## Qualité et sécurité
- N'invente jamais de mesures, de résultats, de tailles de données, de références ou de configurations exécutées.
- Distingue clairement résultats mesurés, valeurs illustratives et hypothèses.
- Ne supprime pas les données brutes et ne modifie pas les fichiers de données source.
- Ne télécharge pas de gros fichiers et ne lance pas de benchmark coûteux sans demander confirmation à l'utilisateur.
- Ne modifie pas les dépendances ou versions sans expliquer la raison.
- N'ajoute pas de tests, de CI ou d'outils complexes sans nécessité pour le périmètre.
- Avant toute modification importante, résume brièvement le plan. Après modification, indique les fichiers touchés, les commandes exécutées et leurs résultats.
- Si une information du sujet est ambiguë, signale-la au lieu de l'inventer.

## Style de développement
- Préfère des fonctions courtes, typées lorsque cela améliore la lisibilité, et des noms explicites.
- Sépare ingestion, validation, transformation, écriture et benchmark.
- Ajoute des tests unitaires ciblés pour les calculs statistiques et les fenêtres temporelles.
- Rends les scripts exécutables depuis la racine du dépôt.
- Utilise `pathlib` pour les chemins.
- Évite les chemins absolus propres à une machine.
- Mets les paramètres expérimentaux dans `configs/`, pas en dur dans les scripts.
- N'écris pas de fichiers de données volumineux dans Git.

## Workflow Codex
1. Lis `AGENTS.md`, `PROJECT_PLAN.md` et `docs/PROJECT_SPEC.md` avant de commencer une tâche.
2. Travaille sur une seule étape à la fois.
3. Inspecte le dépôt avant de créer ou modifier des fichiers.
4. N'anticipe pas les étapes futures sans demande.
5. Exécute les vérifications adaptées à la modification.
6. Mets à jour la documentation française lorsque le comportement du projet change.
