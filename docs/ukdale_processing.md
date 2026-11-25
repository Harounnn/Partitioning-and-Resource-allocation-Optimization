# État de préparation UK-DALE

## Inventaire local

L'audit du dépôt n'a trouvé aucun fichier UK-DALE brut, aucun fichier de calibration et aucun manifeste UK-DALE. Il n'est donc pas possible de confirmer localement le nom exact des fichiers horaires, le contenu des coefficients de calibration, l'ordre des canaux, l'alignement des signaux ou de calculer des RMS réels. Aucun résultat UK-DALE n'est revendiqué.

## Format et calibration documentés par la source

La documentation du créateur décrit les fichiers haute fréquence comme des FLAC stéréo 16 kHz nommés `vi-<horodatage Unix de début>.flac`, découpés en fichiers horaires. Un canal correspond au courant et l'autre à la tension. Les coefficients `amps_per_adc_step` et `volts_per_adc_step` du fichier de calibration de la maison convertissent les valeurs normalisées en unités physiques ; les facteurs ADC documentés diffèrent selon la maison (maisons 1 et 2 : `2³¹`, maison 5 : `2¹⁵`). La page du créateur indique que le déphasage peut être ignoré pour les calculs ciblés ici. Ces informations ne permettent pas, à elles seules, de supposer l'ordre courant/tension des deux canaux pour un fichier donné.

Sources officielles : [page UK-DALE du créateur, formats et instructions de calibration](https://jack-kelly.com/data/) ; [archive de données haute fréquence UKERC EDC, DOI 10.5286/UKERC.EDC.000003](https://doi.org/10.5286/UKERC.EDC.000003). La source décrit aussi une archive CSV désagrégée séparée d'environ 3,5 Go ; elle n'est pas requise pour le pilote de signaux haute fréquence et ne doit pas être confondue avec ce dernier.

## Fichiers à obtenir pour un pilote vérifiable

Après sélection d'une maison et d'une période, le lot minimal doit inclure :

1. `house_<N>/.../calibration.cfg` de la maison choisie (nom/chemin exact à confirmer dans l'arborescence publiée) ;
2. deux FLAC consécutifs `vi-<timestamp1>.flac` et `vi-<timestamp2>.flac`, afin de vérifier une frontière de fichiers et de regrouper les échantillons dans les secondes correspondantes.

La source donne environ 200 Mo par FLAC horaire pour les maisons 1, 2 et 5 ; prévoir environ 400 Mo pour deux fichiers, auxquels s'ajoute le petit fichier texte de calibration et une marge pour les sorties temporaires. Les noms exacts et tailles de ces éléments ne peuvent être indiqués avant de consulter le répertoire officiel pour une période concrète. L'archive haute fréquence complète fait plusieurs téraoctets et n'est pas nécessaire.

Le traitement devra décoder sans altérer les originaux, vérifier avec `ffprobe` le codec, la fréquence d'échantillonnage et le nombre de canaux, interpréter les horodatages d'Unix, calibrer avec les valeurs effectivement lues dans le fichier de la maison, et enregistrer par seconde le RMS ainsi que le nombre d'échantillons de chaque signal. Les secondes manquantes devront être représentées par un compte nul et une valeur RMS nulle/non renseignée ; les échantillons de part et d'autre d'une frontière seront agrégés sous la même clé temporelle. Le mapping des canaux et leur synchronisation devront être établis à partir de données et métadonnées réelles avant de valider les résultats.

## État de l'implémentation

`ffmpeg` et `ffprobe` sont disponibles dans l'environnement, mais le paquet Python SoundFile et les données UK-DALE ne le sont pas. Aucune dépendance n'a été installée. Le décodeur et la calibration restent à implémenter après inspection d'un vrai FLAC et de son `calibration.cfg` : coder ces étapes avant d'avoir vérifié le format concret et l'association des canaux ferait courir un risque de résultats physiquement erronés. La validation future devra inclure un petit segment réel et un calcul RMS indépendant, ainsi qu'un cas contrôlé de frontière et de période manquante.

Le manifeste UK-DALE sera créé lors du premier traitement réel et consignera le chemin logique, la période, la taille, le SHA-256 et les paramètres de calibration/mapping effectivement appliqués.
