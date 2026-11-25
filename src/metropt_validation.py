"""Comparaison Spark compacte entre les sorties Python et PySpark."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


FLOAT_METRICS = (
    "temperature_mean",
    "temperature_std_population",
    "temperature_max",
    "TP2_mean",
    "TP3_mean",
    "H1_mean",
    "DV_pressure_mean",
    "Reservoirs_mean",
    "temperature_change_from_previous_window",
)


def compare_outputs(
    python_path: Path,
    spark_path: Path,
    absolute_tolerance: float = 1e-8,
    relative_tolerance: float = 1e-5,
) -> dict[str, Any]:
    """Compare les tables d'agrégats compactes après lecture de leurs Parquet."""
    python = pd.read_parquet(python_path)
    spark_result = pd.read_parquet(spark_path)
    joined = python.merge(spark_result, on="window_start", how="outer", suffixes=("_python", "_spark"), indicator=True)
    key_mismatch_count = int((joined["_merge"] != "both").sum())
    count_python = joined["sample_count_python"]
    count_spark = joined["sample_count_spark"]
    count_mismatch_count = int((count_python != count_spark).sum())
    summary: dict[str, Any] = {
        "joined_window_rows": len(joined),
        "key_mismatch_count": key_mismatch_count,
        "sample_count_mismatch_count": count_mismatch_count,
    }
    for metric in FLOAT_METRICS:
        python_value = joined[f"{metric}_python"]
        spark_value = joined[f"{metric}_spark"]
        both_null = python_value.isna() & spark_value.isna()
        one_null = python_value.isna() ^ spark_value.isna()
        difference = (python_value - spark_value).abs()
        allowed = absolute_tolerance + relative_tolerance * python_value.abs()
        failures = (~both_null) & (one_null | (difference > allowed))
        comparable = (~python_value.isna()) & (~spark_value.isna())
        max_difference = difference[comparable].max()
        summary[f"{metric}_mismatch_count"] = int(failures.sum())
        summary[f"{metric}_max_abs_difference"] = float(max_difference) if pd.notna(max_difference) else None
    python_windows = len(python)
    spark_windows = len(spark_result)
    summary.update(
        {
            "python_window_count": python_windows,
            "spark_window_count": spark_windows,
            "absolute_tolerance": absolute_tolerance,
            "relative_tolerance": relative_tolerance,
            "tolerance_formula": "|a-b| <= 1e-8 + 1e-5 * |a|",
        }
    )
    mismatch_fields = ["key_mismatch_count", "sample_count_mismatch_count"] + [
        f"{metric}_mismatch_count" for metric in FLOAT_METRICS
    ]
    summary["equivalent"] = all((summary.get(field) or 0) == 0 for field in mismatch_fields)
    return summary
