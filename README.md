# Projet Big Data - Analyse distribuée de données de capteurs

**Niveau :** M2 DSIR  
**Sujet :** Optimisation des ressources de calcul et du partitionnement pour l'analyse batch de données réelles de capteurs.

## 1. Contexte

Ce projet étudie la préparation et l'agrégation de données temporelles issues de capteurs. Il compare une implémentation Python locale optimisée à une implémentation PySpark, puis examine l'influence du parallélisme local et du partitionnement sur les performances.

L'objectif est de déterminer dans quelles conditions le calcul distribué/localement parallèle est pertinent, tout en préservant l'exactitude des indicateurs.

## 2. Questions de recherche

- **Q1.** À partir de quel volume PySpark devient-il préférable à un traitement Python local correctement optimisé, dans l'environnement étudié ?
- **Q2.** À volume fixé, comment le parallélisme local et le nombre de partitions influencent-ils la durée, la mémoire et les échanges de données ?
- **Q3.** Une règle de sélection de configuration peut-elle réduire un indicateur de ressources tout en respectant une échéance sur des lots non utilisés pour son réglage ?

> **Limite expérimentale :** les expériences sont réalisées sur un seul ordinateur physique Windows 10 doté de 16 Go de RAM. Les modes Spark `local[n]` représentent des threads/cœurs d'exécution locaux, et non des nœuds physiques. Aucune conclusion sur le réseau d'un cluster multi-nœuds ne sera formulée.

## 3. Jeux de données

### MetroPT-3

Jeu industriel de mesures provenant du compresseur d'un train de métro. Il sert à construire et vérifier la chaîne de traitement. Les mesures sont agrégées par fenêtres de cinq minutes : nombre de mesures, moyenne, écart-type (convention population), maximum de température, pression moyenne et variation de température depuis la fenêtre précédente.

### UK-DALE

Jeu de mesures électriques domestiques haute fréquence. Il sert à étudier le traitement de signaux plus volumineux. Les signaux sont décodés et calibrés selon la documentation officielle, puis utilisés pour calculer la tension efficace et le courant efficace par seconde. Les téléchargements sont progressifs et limités aux lots compatibles avec le matériel disponible.

Les deux jeux de données sont traités séparément : leurs durées ne doivent pas être comparées comme si les traitements étaient identiques.

## 4. Technologies envisagées

- Python
- NumPy, Pandas, PyArrow
- PySpark
- SoundFile pour les fichiers audio/FLAC UK-DALE, si adapté au format téléchargé
- Parquet pour les données préparées
- Matplotlib pour les figures

Les versions exactes seront figées après vérification de l'environnement et des compatibilités Windows.

## 5. Organisation du dépôt

```text
.
├── AGENTS.md
├── PROJECT_PLAN.md
├── README.md
├── configs/
├── data/
│   ├── raw/          # Données brutes, non versionnées
│   ├── processed/    # Données préparées, non versionnées
│   └── manifests/    # Inventaires et empreintes
├── docs/
│   ├── PROJECT_SPEC.md
│   ├── DAY1_CHECKLIST.md
│   └── report_outline.md
├── notebooks/
├── reports/
│   ├── figures/
│   └── presentation/
├── scripts/
├── src/
└── tests/
```

## 6. Reproductibilité

Les expériences doivent enregistrer la configuration, le volume réellement traité, le nombre d'échantillons, les versions logicielles, les durées et les métriques disponibles. Les lots de réglage et de test sont séparés avant l'optimisation. Les résultats sont rapportés avec médiane et dispersion lorsque plusieurs répétitions sont réalisées.

Aucune donnée synthétique dupliquée ne sera présentée comme un volume réel. Les archives volumineuses ne sont pas déposées dans Git.

## 7. Exécution

Depuis la racine du dépôt, dans PowerShell :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-lock.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\run_metropt.py
```

Les paramètres du pipeline se trouvent dans `configs/metropt.toml`. Le script lit les données par blocs, écrit `data/processed/metropt_5min.parquet`, actualise `data/manifests/metropt.csv` et produit `reports/day1_validation.md`.

## 8. Livrables

1. Rapport de recherche rédigé en français.
2. Dépôt de code exécutable avec README, environnement figé, scripts Python/PySpark, configurations, manifeste des données, mesures brutes et scripts de figures.
3. Présentation orale en français de 10 à 15 minutes, suivie de questions.

## 9. État d'avancement

- [x] Environnement vérifié (RAM Windows mesurée à 19,91 Gio, contre 16 Go annoncés pour la cible)
- [x] Pipeline Python MetroPT-3
- [ ] Pipeline PySpark MetroPT-3
- [ ] Vérification d'équivalence
- [ ] Traitement UK-DALE
- [ ] Campagne de benchmark
- [ ] Sélecteur de configuration
- [ ] Rapport final
- [ ] Présentation finale
