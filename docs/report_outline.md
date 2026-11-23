# Plan du rapport de recherche

> Document de travail. Rédiger les sections avec les résultats effectivement obtenus ; ne pas inventer de mesures.

## 1. Contexte et problématique
- Contexte de l'analyse de données temporelles de capteurs.
- Problématique et motivation.
- Questions de recherche.
- Périmètre et limites de l'étude mono-machine.

## 2. Travaux liés et connexes
- Traitement batch de données temporelles.
- Python optimisé et calcul distribué avec Apache Spark.
- Partitionnement, shuffle et parallélisme.
- Références bibliographiques vérifiées.

## 3. Données et méthodes
### 3.1 MetroPT-3
- Origine, licence, variables, volume réellement utilisé.
- Contrôles qualité et cadence.
- Agrégations par fenêtres de cinq minutes.

### 3.2 UK-DALE
- Origine, licence, signaux et lots téléchargés.
- Décodage et calibration.
- Calcul RMS par seconde et gestion des frontières.

### 3.3 Implémentations
- Référence Python.
- Implémentation PySpark.
- Vérification d'équivalence et tolérance.

### 3.4 Sélection de configuration
- Variables, profils ou modèle d'estimation.
- Échéance L et marge m.
- Séparation calibration/test.
- Définition honnête du coût proxy local.

## 4. Protocole expérimental
- Matériel et logiciels.
- Volumes et lots.
- Configurations testées.
- Répétitions, ordre et paramètres constants.
- Métriques et méthode de mesure.

## 5. Résultats et discussion
- Python vs Spark selon le volume.
- Durée selon le parallélisme local.
- Durée et ressources selon les partitions.
- Comparaison des stratégies fixes et sélection proposée.
- Médianes, dispersion, échecs et limites.
- Validité externe et absence de cluster multi-nœuds.

## 6. Conclusion et perspectives
- Réponses aux questions de recherche.
- Conditions dans lesquelles Spark est pertinent.
- Configuration recommandée dans l'environnement testé.
- Limites et perspectives (cluster réel, volumes plus importants, autres périodes).
