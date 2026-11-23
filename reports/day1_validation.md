# Contrôles du pipeline MetroPT-3 — Jour 1

## Provenance

Jeu MetroPT-3 téléchargé depuis le dépôt officiel UCI, DOI [10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), licence CC BY 4.0. Les empreintes SHA-256 et tailles sont dans `data/manifests/metropt.csv`.

## Contrôles observés

- Mesures lues : 1,516,948.
- Période horodatée : 2020-02-01T00:00:00 à 2020-09-01T03:59:50.
- Horodatages monotones : True.
- Horodatages consécutifs identiques : 0.
- Intervalles observés (secondes, arrondis) : 10 s : 1337521, 9 s : 128277, 12 s : 38321, 13 s : 7988, 11 s : 4471.
- Valeurs manquantes : `Oil_temperature` : 0, `TP2` : 0, `TP3` : 0, `H1` : 0, `DV_pressure` : 0, `Reservoirs` : 0.
- Fenêtres de cinq minutes produites : 61,392, dont 10,610 vides.
- Taille Parquet : 3,264,953 octets.
- Durée de traitement incluant l’écriture Parquet : 43.503 s.

Les timestamps de la source ne précisent pas de fuseau horaire ; aucune conversion n’est appliquée. Une fenêtre est définie comme [début, fin). Les fenêtres vides sont conservées avec un compte nul et des indicateurs nuls. La variation de température est nulle quand la fenêtre précédente est vide ou absente.

La température retenue est `Oil_temperature` (°C). La source contient cinq pressions distinctes, exprimées en bar ; leurs moyennes sont produites séparément (`TP2`, `TP3`, `H1`, `DV_pressure`, `Reservoirs`) afin d’éviter de les mélanger.

## Configuration et versions

- Taille de bloc : 50000 lignes.
- Python : 3.12.1 ; pandas : 2.2.3 ; PyArrow : 18.1.0 ; PySpark : 4.1.1.

Ces contrôles décrivent le fichier réellement traité. Les performances correspondent à une exécution et ne constituent pas un benchmark comparatif.
