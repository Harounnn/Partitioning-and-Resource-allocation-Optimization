"""Traitement MetroPT-3 avec Spark SQL et DataFrames."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def create_spark_session(
    master: str,
    shuffle_partitions: int,
    session_timezone: str = "UTC",
    app_name: str = "MetroPT3Spark",
) -> SparkSession:
    """Crée Spark avec l'interpréteur Python actif et les paramètres fournis."""
    if os.name == "nt":
        _normalize_hadoop_home()
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", session_timezone)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def _normalize_hadoop_home() -> None:
    """Corrige HADOOP_HOME lorsqu'il désigne bin au lieu de sa racine."""
    configured_home = os.environ.get("HADOOP_HOME")
    if not configured_home:
        return
    home = Path(configured_home)
    if not (home / "bin" / "winutils.exe").is_file() and (home.parent / "bin" / "winutils.exe").is_file():
        os.environ["HADOOP_HOME"] = str(home.parent)


def read_metropt_csv(
    spark: SparkSession,
    input_path: Path,
    timestamp_column: str,
    temperature_column: str,
    pressure_columns: tuple[str, ...],
    timestamp_format: str,
) -> DataFrame:
    """Lit les colonnes utiles puis parse explicitement temps et mesures."""
    source_columns = (timestamp_column, temperature_column, *pressure_columns)
    raw = spark.read.option("header", True).option("inferSchema", False).option("mode", "PERMISSIVE").csv(str(input_path))
    selected = raw.select(*source_columns)
    parsed = selected.withColumn("timestamp", F.to_timestamp_ntz(F.col(timestamp_column), F.lit(timestamp_format)))
    invalid_timestamps = parsed.filter(F.col("timestamp").isNull()).limit(1).count()
    if invalid_timestamps:
        raise ValueError(f"Horodatage invalide détecté dans {input_path}")

    result = parsed.select(
        "timestamp",
        *[
            F.when(F.isnan(F.col(name).cast("double")), F.lit(None).cast("double"))
            .otherwise(F.col(name).cast("double"))
            .alias(name)
            for name in (temperature_column, *pressure_columns)
        ],
    )
    return result


def aggregate_metropt(
    measurements: DataFrame,
    temperature_column: str,
    pressure_columns: tuple[str, ...],
    window_duration: str = "5 minutes",
) -> DataFrame:
    """Produit les fenêtres [début, fin), y compris les fenêtres sans mesures."""
    bucketed = measurements.withColumn(
        "window_start", F.window(F.col("timestamp"), window_duration).getField("start")
    )
    aggregations = [
        F.count(F.lit(1)).cast("long").alias("sample_count"),
        F.avg(F.col(temperature_column)).alias("temperature_mean"),
        F.stddev_pop(F.col(temperature_column)).alias("temperature_std_population"),
        F.max(F.col(temperature_column)).alias("temperature_max"),
        *[F.avg(F.col(name)).alias(f"{name}_mean") for name in pressure_columns],
    ]
    observed = bucketed.groupBy("window_start").agg(*aggregations)

    bounds = observed.agg(
        F.min("window_start").alias("first_window"),
        F.max("window_start").alias("last_window"),
    )
    grid = bounds.select(
        F.explode(
            F.sequence(
                F.col("first_window"),
                F.col("last_window"),
                F.expr(f"INTERVAL {window_duration.upper()}"),
            )
        ).alias("window_start")
    )
    complete = grid.join(observed, on="window_start", how="left").withColumn(
        "sample_count", F.coalesce(F.col("sample_count"), F.lit(0).cast("long"))
    )
    ordered_windows = Window.orderBy("window_start")
    previous_count = F.lag("sample_count").over(ordered_windows)
    previous_mean = F.lag("temperature_mean").over(ordered_windows)
    return (
        complete.withColumn(
            "temperature_change_from_previous_window",
            F.when(previous_count > 0, F.col("temperature_mean") - previous_mean),
        )
        .select(
            "window_start",
            "sample_count",
            "temperature_mean",
            "temperature_std_population",
            "temperature_max",
            *[f"{name}_mean" for name in pressure_columns],
            "temperature_change_from_previous_window",
        )
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _logical_path(path: Path) -> str:
    """Retourne un chemin relatif au répertoire de lancement pour le manifeste."""
    try:
        relative = os.path.relpath(path.resolve(), Path.cwd().resolve())
    except ValueError:  # Volumes Windows différents, par exemple un répertoire temporaire.
        return path.name
    return Path(relative).as_posix()


def write_metropt_parquet(
    result: DataFrame,
    output_path: Path,
    manifest_path: Path,
    input_path: Path,
    master: str,
    shuffle_partitions: int,
    window_duration: str,
    parquet_batch_rows: int = 8192,
) -> dict[str, Any]:
    """Sérialise les lignes agrégées par lots sans collecter le résultat complet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if output_path.resolve() == input_path.resolve():
        raise ValueError("Le chemin de sortie ne peut pas remplacer le CSV source")
    if output_path.exists() and output_path.is_dir():
        raise ValueError(f"Le chemin de sortie existe comme dossier : {output_path}")
    started = time.perf_counter()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f"{output_path.name}.{uuid.uuid4().hex}.tmp")
    metric_columns = [name for name in result.columns if name not in {"window_start", "sample_count"}]
    schema = pa.schema(
        [
            pa.field("window_start", pa.timestamp("us")),
            pa.field("sample_count", pa.int64()),
            *[pa.field(name, pa.float64()) for name in metric_columns],
        ]
    )
    window_count = 0
    sample_count = 0
    empty_window_count = 0
    batch: list[dict[str, Any]] = []
    try:
        with pq.ParquetWriter(temporary_path, schema=schema, compression="snappy") as writer:
            for row in result.toLocalIterator(prefetchPartitions=False):
                item = row.asDict(recursive=True)
                window_count += 1
                sample_count += int(item["sample_count"])
                empty_window_count += int(item["sample_count"] == 0)
                batch.append(item)
                if len(batch) >= parquet_batch_rows:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        temporary_path.replace(output_path)
    except Exception:
        if temporary_path.exists() and temporary_path.is_file():
            temporary_path.unlink()
        raise
    duration = time.perf_counter() - started
    parts = [{"logical_path": _logical_path(output_path), "size_bytes": output_path.stat().st_size, "sha256": _sha256(output_path)}]
    manifest = {
        "input_path": _logical_path(input_path),
        "input_size_bytes": input_path.stat().st_size,
        "input_sha256": _sha256(input_path),
        "output_path": _logical_path(output_path),
        "spark_version": result.sparkSession.version,
        "master": master,
        "shuffle_partitions": shuffle_partitions,
        "session_timezone": "UTC; les timestamps source restent sans fuseau via TIMESTAMP_NTZ",
        "window_duration": window_duration,
        "parquet_batch_rows": parquet_batch_rows,
        "window_convention": "début inclus, fin exclue; fenêtres vides conservées",
        "duplicates": "aucune déduplication; chaque ligne source contribue au compte",
        "window_count": window_count,
        "sample_count": sample_count,
        "empty_window_count": empty_window_count,
        "write_seconds": round(duration, 3),
        "output_parts": parts,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
