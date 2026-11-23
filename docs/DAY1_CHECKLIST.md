# Jour 1 — Liste de contrôle

## A. Diagnostic matériel
Dans PowerShell, depuis le dépôt :

```powershell
Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors
Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory
Get-PSDrive -PSProvider FileSystem
python --version
java -version
git --version
```

Noter le modèle CPU, les cœurs/logical processors, la RAM réellement détectée et l'espace libre du disque destiné aux données.

## B. Environnement Python
Après avoir choisi une version Python compatible avec la version PySpark retenue :

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

Si PowerShell bloque l'activation, utiliser temporairement :
```powershell
.\.venv\Scripts\python.exe -m pip --version
```

Ne pas installer une pile complète avant d'avoir vérifié les versions Java/Python/PySpark compatibles.

## C. Structure initiale
Créer les dossiers `src`, `tests`, `configs`, `data/raw`, `data/processed`, `data/manifests`, `reports/figures`, `reports/presentation` et `scripts`.

## D. MetroPT-3
- Télécharger uniquement depuis la source officielle indiquée dans le sujet ou le dépôt officiel du producteur.
- Conserver l'URL source, la licence, le nom du fichier, la date de téléchargement, la taille et le SHA-256.
- Inspecter les colonnes et le format des dates avant d'écrire la transformation.
- Vérifier le fuseau horaire et la cadence réelle.
- Vérifier valeurs manquantes, doublons et unités.
- Écrire un petit test sur un échantillon avant le traitement complet.

## E. Transformation attendue
Fenêtres temporelles de cinq minutes :
- count des mesures ;
- moyenne, écart-type population et maximum de température ;
- moyenne de pression ;
- variation de température par rapport à la fenêtre précédente.

Préciser la convention des fenêtres (début inclus, fin exclue) et le traitement des fenêtres vides.

## F. Validation
- Vérifier les clés temporelles.
- Vérifier les unités.
- Vérifier les valeurs sur un exemple calculable manuellement.
- Exporter en Parquet.
- Conserver un journal d'exécution.

## Critère de fin du jour
Un script Python exécutable depuis la racine, une sortie Parquet, un manifeste de données et une courte note en français décrivant les contrôles réalisés.
