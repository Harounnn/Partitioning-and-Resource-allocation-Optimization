"""Exécute le pipeline Python MetroPT-3 depuis la racine du dépôt."""

from __future__ import annotations

import csv
import json
import platform
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pandas
import pyarrow
import pyspark

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.metropt import process_csv  # noqa: E402


def main() -> None:
    config_path = ROOT / "configs" / "metropt.toml"
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    input_path = ROOT / config["input"]["path"]
    output_path = ROOT / config["output"]["path"]
    manifest_path = ROOT / config["output"]["manifest_path"]
    report_path = ROOT / config["output"]["validation_report_path"]
    started = time.perf_counter()
    result = process_csv(
        input_path=input_path,
        output_path=output_path,
        chunk_size=int(config["input"]["chunk_size"]),
        timestamp_column=config["aggregation"]["timestamp_column"],
        temperature_column=config["aggregation"]["temperature_column"],
        pressure_columns=tuple(config["aggregation"]["pressure_columns"]),
        window=config["aggregation"]["window"],
    )
    result["processing_seconds_including_parquet_write"] = round(time.perf_counter() - started, 3)
    result["run_utc"] = datetime.now(timezone.utc).isoformat()
    result["python_version"] = platform.python_version()
    result["pandas_version"] = pandas.__version__
    result["pyarrow_version"] = pyarrow.__version__
    result["pyspark_version"] = pyspark.__version__

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as manifest:
        writer = csv.DictWriter(manifest, fieldnames=["logical_path", "role", "period", "downloaded_at_utc", "size_bytes", "sha256", "source_url", "license"])
        writer.writeheader()
        archive_path = ROOT / "data/raw/metropt_3_dataset.zip"
        downloaded_at = datetime.fromtimestamp(archive_path.stat().st_mtime, timezone.utc).isoformat()
        writer.writerow({"logical_path": "data/raw/MetroPT3(AirCompressor).csv", "role": "données brutes extraites", "period": f"{result['first_timestamp']} à {result['last_timestamp']}", "downloaded_at_utc": downloaded_at, "size_bytes": result["input_bytes"], "sha256": result["input_sha256"], "source_url": "https://archive.ics.uci.edu/static/public/791/metropt+3+dataset.zip", "license": "CC BY 4.0 — attribution UCI/Davari et al., DOI 10.24432/C5VW3R"})
        writer.writerow({"logical_path": "data/raw/metropt_3_dataset.zip", "role": "archive source UCI", "period": "sans objet", "downloaded_at_utc": downloaded_at, "size_bytes": archive_path.stat().st_size, "sha256": _sha256(archive_path), "source_url": "https://archive.ics.uci.edu/static/public/791/metropt+3+dataset.zip", "license": "CC BY 4.0 — attribution UCI/Davari et al., DOI 10.24432/C5VW3R"})
        description_path = ROOT / "data/raw/Data Description_Metro.pdf"
        writer.writerow({"logical_path": "data/raw/Data Description_Metro.pdf", "role": "documentation source UCI", "period": "sans objet", "downloaded_at_utc": downloaded_at, "size_bytes": description_path.stat().st_size, "sha256": _sha256(description_path), "source_url": "https://archive.ics.uci.edu/static/public/791/metropt+3+dataset.zip", "license": "documentation incluse dans l’archive UCI MetroPT-3"})
        writer.writerow({"logical_path": config["output"]["path"], "role": "agrégats préparés", "period": f"{result['first_timestamp']} à {result['last_timestamp']}", "downloaded_at_utc": "", "size_bytes": result["output_bytes"], "sha256": result["output_sha256"], "source_url": "", "license": "dérivé du jeu UCI CC BY 4.0"})

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(result), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def render_report(result: dict) -> str:
    cadence = ", ".join(f"{seconds} s : {count}" for seconds, count in result["cadence_seconds"].items())
    missing = ", ".join(f"`{name}` : {count}" for name, count in result["missing_values"].items())
    return f"""# Contrôles du pipeline MetroPT-3 — Jour 1

## Provenance

Jeu MetroPT-3 téléchargé depuis le dépôt officiel UCI, DOI [10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), licence CC BY 4.0. Les empreintes SHA-256 et tailles sont dans `data/manifests/metropt.csv`.

## Contrôles observés

- Mesures lues : {result['row_count']:,}.
- Période horodatée : {result['first_timestamp']} à {result['last_timestamp']}.
- Horodatages monotones : {result['timestamp_monotonic']}.
- Horodatages consécutifs identiques : {result['timestamp_duplicate_count']}.
- Intervalles observés (secondes, arrondis) : {cadence}.
- Valeurs manquantes : {missing}.
- Fenêtres de cinq minutes produites : {result['window_count']:,}, dont {result['empty_window_count']:,} vides.
- Taille Parquet : {result['output_bytes']:,} octets.
- Durée de traitement incluant l’écriture Parquet : {result['processing_seconds_including_parquet_write']:.3f} s.

Les timestamps de la source ne précisent pas de fuseau horaire ; aucune conversion n’est appliquée. Une fenêtre est définie comme [début, fin). Les fenêtres vides sont conservées avec un compte nul et des indicateurs nuls. La variation de température est nulle quand la fenêtre précédente est vide ou absente.

La température retenue est `Oil_temperature` (°C). La source contient cinq pressions distinctes, exprimées en bar ; leurs moyennes sont produites séparément (`TP2`, `TP3`, `H1`, `DV_pressure`, `Reservoirs`) afin d’éviter de les mélanger.

## Configuration et versions

- Taille de bloc : {result.get('chunk_size', '50 000')} lignes.
- Python : {result['python_version']} ; pandas : {result['pandas_version']} ; PyArrow : {result['pyarrow_version']} ; PySpark : {result['pyspark_version']}.

Ces contrôles décrivent le fichier réellement traité. Les performances correspondent à une exécution et ne constituent pas un benchmark comparatif.
"""


if __name__ == "__main__":
    main()
