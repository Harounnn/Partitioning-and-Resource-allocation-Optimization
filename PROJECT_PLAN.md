# Plan de travail du projet

## Objectif
Réaliser en quatre jours une étude expérimentale reproductible, adaptée à un ordinateur Windows 10 avec 16 Go RAM et un seul ordinateur physique.

## Décisions de cadrage
- Étude batch, CPU uniquement.
- MetroPT-3 : validation de la chaîne.
- UK-DALE : étude de signaux plus volumineux, téléchargement progressif.
- Comparaison Python local / PySpark local.
- `c` désigne le parallélisme local Spark (`local[c]`), pas le nombre de nœuds.
- `P` désigne `spark.sql.shuffle.partitions`.
- Aucune affirmation de passage à l'échelle réseau multi-nœuds.
- Les volumes et résultats sont toujours réels et mesurés.

## Jour 1 — Environnement et MetroPT-3
1. Relever CPU, espace disque, versions Python/Java.
2. Créer et activer un environnement virtuel.
3. Installer les dépendances minimales.
4. Obtenir MetroPT-3 depuis sa source officielle et conserver sa référence/licence.
5. Inspecter colonnes, horodatages, cadence, valeurs manquantes et doublons.
6. Développer le traitement Python par blocs.
7. Calculer les indicateurs sur fenêtres de cinq minutes.
8. Écrire en Parquet et créer un manifeste avec taille et SHA-256.
9. Vérifier les résultats sur un petit échantillon.

**Critère de sortie :** pipeline Python reproductible et sortie Parquet contrôlée.

## Jour 2 — PySpark et UK-DALE
1. Développer les transformations PySpark équivalentes.
2. Comparer clés et valeurs Python/Spark avec une tolérance justifiée.
3. Télécharger un petit signal UK-DALE et sa calibration.
4. Décoder/calibrer selon la documentation.
5. Calculer les RMS tension/courant par seconde.
6. Traiter les frontières de fichiers et périodes absentes.
7. Écrire les sorties Parquet.

**Critère de sortie :** deux pipelines opérationnels et vérification d'équivalence sur le traitement commun.

## Jour 3 — Benchmarks
1. Faire un pilote pour mesurer lecture, calcul et écriture.
2. Définir des lots réels croissants compatibles avec la machine.
3. Séparer calibration et test.
4. Comparer B0 Python, B1 Spark `local[1]`, puis configurations locales `local[2]` et éventuellement `local[4]`.
5. Tester une grille réduite de partitions, par exemple P ∈ {8, 16, 32, 64}.
6. Réaliser au moins trois répétitions exploratoires et cinq répétitions finales si le temps le permet.
7. Enregistrer durée, mémoire disponible/maximale si mesurable, shuffle, spill, erreurs et taille des sorties.
8. Générer les premières figures.

**Critère de sortie :** mesures brutes traçables et graphiques descriptifs.

## Jour 4 — Sélection et livrables
1. Construire une table de profils ou une régression simple pour estimer la durée.
2. Fixer l'échéance L et calibrer une marge m sur les lots de validation.
3. Figer la règle de sélection.
4. Évaluer sur les lots de test réservés.
5. Calculer gain et respect de l'échéance, avec une définition de coût honnête pour une seule machine.
6. Finaliser les figures et le rapport en français.
7. Finaliser le README et les instructions de reproduction.
8. Préparer la présentation en français.

## Règle de priorité
Si le temps ou le matériel manque, préserver dans cet ordre :
1. exactitude du pipeline ;
2. comparaison Python/Spark équivalente ;
3. mesures reproductibles ;
4. séparation calibration/test ;
5. méthode de sélection simple ;
6. extension des volumes et répétitions.

Ne jamais remplacer des résultats manquants par des valeurs inventées.
