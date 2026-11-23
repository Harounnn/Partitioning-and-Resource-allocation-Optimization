# Exécution du pipeline MetroPT-3

## Environnement

L'environnement exécuté pour le Jour 1 utilise Python 3.12.1, Java 21.0.7, PySpark 4.1.1, pandas 2.2.3 et PyArrow 18.1.0. Les dépendances directes sont dans `requirements.txt` ; `requirements-lock.txt` fige aussi les dépendances transitives de l'environnement installé.

Le modèle détecté est Intel Core i7-8550U (4 cœurs physiques et 8 threads selon le fabricant). L'API mémoire de Windows indique 19,91 Gio au total et 9,82 Gio disponibles lors de l'audit ; cette capacité diffère des 16 Go annoncés pour la cible du projet. Les choix de traitement restent prudents et compatibles avec la cible annoncée.

## Commandes

Depuis la racine du dépôt, dans PowerShell :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-lock.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\run_metropt.py
```

Les paramètres sont définis dans `configs/metropt.toml`. Le script lit le CSV par blocs, écrit `data/processed/metropt_5min.parquet`, crée ou actualise le manifeste `data/manifests/metropt.csv` et rédige `reports/day1_validation.md`. Les données brutes et les fichiers Parquet sont exclus de Git.

Le mode Spark local `local[1]` a été démarré avec succès sur Java 21. Spark a signalé l'absence de `winutils.exe` et de sa bibliothèque Hadoop native ; le test élémentaire a néanmoins réussi. Cette configuration reste à vérifier pour les opérations Spark qui seront développées au Jour 2.
