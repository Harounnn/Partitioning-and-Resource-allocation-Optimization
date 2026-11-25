# Validation de l'équivalence MetroPT-3 Python / PySpark

## Méthode

L'implémentation Python par blocs (`src/metropt.py`) sert de référence. L'implémentation Spark SQL/DataFrame (`src/metropt_spark.py`) parse explicitement l'horodatage en `TIMESTAMP_NTZ`, dans le fuseau de session UTC, puis agrège sur les fenêtres `[début, fin)` de cinq minutes. Les horodatages source ne contiennent pas de fuseau : ils restent des temps muraux et ne sont pas convertis.

Chaque ligne source compte une fois, y compris en présence d'un horodatage dupliqué. Les mesures numériques invalides ou manquantes sont nulles : le nombre de mesures est le nombre de lignes, tandis que les agrégats numériques ignorent les valeurs nulles comme Pandas. La grille complète conserve les fenêtres vides. La variation est la différence entre les moyennes de température des fenêtres consécutives ; elle est nulle si la fenêtre immédiatement précédente est vide. Cette convention correspond à la référence Python du Jour 1.

La validation compare les clés de fenêtre, le nombre de lignes et chacun des indicateurs (moyenne, écart-type population, maximum, les cinq pressions moyennes et variation). Pour les valeurs numériques, la règle est `|a-b| <= 10⁻⁸ + 10⁻⁵ × |a|`, telle que proposée par la spécification. Les unités et amplitudes observées rendent cette tolérance suffisamment stricte ; les écarts constatés sont proches des erreurs d'arrondi en virgule flottante.

## Résultats mesurés

Les rapports JSON issus des commandes de validation sont les sources détaillées : `reports/metropt_spark_validation_local1.json` et `reports/metropt_spark_validation_local2.json`. Chaque mode a comparé 61 392 fenêtres et 1 516 948 mesures, dont 10 610 fenêtres vides.

| Mode Spark | Clés différentes | Valeurs hors tolérance | Équivalence |
|---|---:|---:|---|
| `local[1]` | 0 | 0 | Oui |
| `local[2]` | 0 | 0 | Oui |

L'écart absolu maximal observé parmi les métriques est de l'ordre de `7,1 × 10⁻¹⁴` pour `local[1]` et `7,1 × 10⁻¹⁴` pour `local[2]`. Les détails par indicateur se trouvent dans les rapports JSON. Ce sont des résultats de vérification fonctionnelle ; aucune conclusion comparative de performance n'en est tirée.

Les sorties Parquet sont `data/processed/metropt_5min_spark_local1.parquet` et `data/processed/metropt_5min_spark_local2.parquet`. Les manifestes associés enregistrent la taille, l'empreinte SHA-256, les paramètres Spark et les volumes traités.

## Particularité Windows

L'écriture Spark Parquet native a échoué sur cette installation avec `UnsatisfiedLinkError` dans `NativeIO.Windows.access0`, en raison du décalage entre Hadoop natif Windows installé (3.2.4) et Hadoop fourni avec Spark (3.4.2). Aucun composant système ni paquet n'a été installé ou remplacé. Le résultat agrégé, de taille modeste, est parcouru via `toLocalIterator(prefetchPartitions=False)` et sérialisé en lots plafonnés avec PyArrow. Le tableau complet n'est jamais collecté en mémoire. Cette solution préserve le calcul Spark, mais le coût de sérialisation Parquet inclut le transfert borné vers le processus Python ; il faudra en tenir compte si l'environnement Hadoop est corrigé ultérieurement.

Le calcul de `lag` ordonne toutes les fenêtres sur une seule partition Spark, ce qui est nécessaire pour la variation temporelle globale. Le pilote n'a pas mesuré les performances et ne justifie aucune campagne de benchmark.

## Reproduction

```powershell
.\.venv\Scripts\python.exe scripts\run_metropt_spark.py --master "local[1]"
.\.venv\Scripts\python.exe scripts\validate_metropt_spark.py --master "local[1]"
.\.venv\Scripts\python.exe scripts\run_metropt_spark.py --master "local[2]"
.\.venv\Scripts\python.exe scripts\validate_metropt_spark.py --master "local[2]"
.\.venv\Scripts\python.exe -m pytest -q tests\test_metropt_spark.py
```
