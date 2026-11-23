"""Agrégation mémoire-efficiente des mesures MetroPT-3."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class WindowStats:
    """Statistiques fusionnables d'une fenêtre temporelle."""

    count: int
    mean: float
    m2: float
    maximum: float
    pressure_sums: dict[str, float]
    pressure_counts: dict[str, int]
    sample_count: int


def _merge_window_stats(current: WindowStats | None, incoming: WindowStats) -> WindowStats:
    if current is None:
        return incoming
    total = current.count + incoming.count
    if total:
        delta = incoming.mean - current.mean
        mean = current.mean + delta * incoming.count / total
        m2 = current.m2 + incoming.m2 + delta * delta * current.count * incoming.count / total
    else:
        mean = 0.0
        m2 = 0.0
    pressure_sums = {
        name: current.pressure_sums.get(name, 0.0) + incoming.pressure_sums.get(name, 0.0)
        for name in current.pressure_sums.keys() | incoming.pressure_sums.keys()
    }
    pressure_counts = {
        name: current.pressure_counts.get(name, 0) + incoming.pressure_counts.get(name, 0)
        for name in current.pressure_counts.keys() | incoming.pressure_counts.keys()
    }
    return WindowStats(
        count=total,
        mean=mean,
        m2=m2,
        maximum=max(current.maximum, incoming.maximum),
        pressure_sums=pressure_sums,
        pressure_counts=pressure_counts,
        sample_count=current.sample_count + incoming.sample_count,
    )


def aggregate_frame(
    frame: pd.DataFrame,
    timestamp_column: str = "timestamp",
    temperature_column: str = "Oil_temperature",
    pressure_columns: tuple[str, ...] = ("TP2", "TP3", "H1", "DV_pressure", "Reservoirs"),
    window: str = "5min",
) -> pd.DataFrame:
    """Agrège un bloc en fenêtres gauches fermées et droites ouvertes."""
    required = {timestamp_column, temperature_column, *pressure_columns}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Colonnes absentes : {sorted(missing)}")
    work = frame[[timestamp_column, temperature_column, *pressure_columns]].copy()
    work[timestamp_column] = pd.to_datetime(work[timestamp_column], errors="raise")
    work["window_start"] = work[timestamp_column].dt.floor(window)
    grouped = work.groupby("window_start", sort=True)
    temp = grouped[temperature_column].agg(["count", "mean", "max"])
    temp["m2"] = grouped[temperature_column].var(ddof=0).fillna(0.0) * temp["count"]
    pressure_sum = grouped[list(pressure_columns)].sum(min_count=1)
    pressure_count = grouped[list(pressure_columns)].count()
    result = pd.DataFrame(index=temp.index)
    result["sample_count"] = grouped.size().astype("int64")
    result["temperature_count"] = temp["count"]
    result["temperature_mean"] = temp["mean"]
    result["temperature_m2"] = temp["m2"]
    result["temperature_max"] = temp["max"]
    for name in pressure_columns:
        result[f"{name}_sum"] = pressure_sum[name]
        result[f"{name}_count"] = pressure_count[name]
    return result


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def process_csv(
    input_path: Path,
    output_path: Path,
    chunk_size: int = 50000,
    timestamp_column: str = "timestamp",
    temperature_column: str = "Oil_temperature",
    pressure_columns: tuple[str, ...] = ("TP2", "TP3", "H1", "DV_pressure", "Reservoirs"),
    window: str = "5min",
) -> dict[str, Any]:
    """Lit le CSV par blocs, fusionne les statistiques et écrit un Parquet."""
    pressure_columns = tuple(pressure_columns)
    all_columns = [timestamp_column, temperature_column, *pressure_columns]
    accumulators: dict[pd.Timestamp, WindowStats] = {}
    missing_values: Counter[str] = Counter()
    cadence_seconds: Counter[int] = Counter()
    previous_timestamp: pd.Timestamp | None = None
    monotonic = True
    timestamp_duplicates = 0
    row_count = 0
    first_timestamp: pd.Timestamp | None = None
    last_timestamp: pd.Timestamp | None = None

    for chunk in pd.read_csv(input_path, usecols=all_columns, chunksize=chunk_size):
        parsed = pd.to_datetime(chunk[timestamp_column], errors="coerce")
        invalid = int(parsed.isna().sum())
        if invalid:
            raise ValueError(f"{invalid} horodatage(s) invalide(s) détecté(s)")
        chunk[timestamp_column] = parsed
        if previous_timestamp is not None and parsed.iloc[0] < previous_timestamp:
            monotonic = False
        ordered = parsed.diff().dropna()
        if previous_timestamp is not None:
            ordered = pd.concat([pd.Series([parsed.iloc[0] - previous_timestamp]), ordered], ignore_index=True)
        for delta in ordered:
            seconds = int(round(delta.total_seconds()))
            cadence_seconds[seconds] += 1
            if seconds == 0:
                timestamp_duplicates += 1
            if seconds < 0:
                monotonic = False
        previous_timestamp = parsed.iloc[-1]
        first_timestamp = first_timestamp or parsed.iloc[0]
        last_timestamp = parsed.iloc[-1]
        row_count += len(chunk)
        for column in [temperature_column, *pressure_columns]:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
            missing_values[column] += int(chunk[column].isna().sum())

        partial = aggregate_frame(
            chunk,
            timestamp_column=timestamp_column,
            temperature_column=temperature_column,
            pressure_columns=pressure_columns,
            window=window,
        )
        for start, row in partial.iterrows():
            pressure_sums = {name: float(row[f"{name}_sum"]) if pd.notna(row[f"{name}_sum"]) else 0.0 for name in pressure_columns}
            pressure_counts = {name: int(row[f"{name}_count"]) for name in pressure_columns}
            incoming = WindowStats(
                count=int(row["temperature_count"]),
                mean=float(row["temperature_mean"]) if pd.notna(row["temperature_mean"]) else 0.0,
                m2=float(row["temperature_m2"]),
                maximum=float(row["temperature_max"]) if pd.notna(row["temperature_max"]) else float("-inf"),
                pressure_sums=pressure_sums,
                pressure_counts=pressure_counts,
                sample_count=int(row["sample_count"]),
            )
            accumulators[start] = _merge_window_stats(accumulators.get(start), incoming)

    if not row_count or first_timestamp is None or last_timestamp is None:
        raise ValueError("Le fichier ne contient aucune mesure")

    starts = pd.date_range(first_timestamp.floor(window), last_timestamp.floor(window), freq=window, name="window_start")
    output = pd.DataFrame(index=starts)
    output["sample_count"] = 0
    for name in ["temperature_mean", "temperature_std_population", "temperature_max", *[f"{p}_mean" for p in pressure_columns]]:
        output[name] = float("nan")
    for start, stats in accumulators.items():
        output.at[start, "sample_count"] = stats.sample_count
        if stats.count:
            output.at[start, "temperature_mean"] = stats.mean
            output.at[start, "temperature_std_population"] = (max(stats.m2 / stats.count, 0.0)) ** 0.5
            output.at[start, "temperature_max"] = stats.maximum
        for name in pressure_columns:
            count = stats.pressure_counts[name]
            output.at[start, f"{name}_mean"] = stats.pressure_sums[name] / count if count else float("nan")
    output["sample_count"] = output["sample_count"].astype("int64")
    output["temperature_change_from_previous_window"] = output["temperature_mean"].diff().where(output["sample_count"].shift(1).fillna(0) > 0)
    output = output.reset_index()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(output_path, index=False, engine="pyarrow")
    return {
        "input_path": str(input_path),
        "input_bytes": input_path.stat().st_size,
        "input_sha256": _sha256(input_path),
        "output_path": str(output_path),
        "output_bytes": output_path.stat().st_size,
        "output_sha256": _sha256(output_path),
        "row_count": row_count,
        "window_count": len(output),
        "empty_window_count": int((output["sample_count"] == 0).sum()),
        "first_timestamp": first_timestamp.isoformat(),
        "last_timestamp": last_timestamp.isoformat(),
        "timestamp_monotonic": monotonic,
        "timestamp_duplicate_count": timestamp_duplicates,
        "missing_values": dict(missing_values),
        "cadence_seconds": dict(sorted(cadence_seconds.items(), key=lambda item: (-item[1], item[0]))[:5]),
        "cadence_interval_min_seconds": min(cadence_seconds) if cadence_seconds else None,
        "cadence_interval_max_seconds": max(cadence_seconds) if cadence_seconds else None,
        "cadence_intervals_over_60_seconds": sum(count for seconds, count in cadence_seconds.items() if seconds > 60),
        "window": window,
        "chunk_size": chunk_size,
        "window_convention": "début inclus, fin exclue ; fenêtres vides conservées avec count=0 et indicateurs nuls",
        "timezone": "non spécifié dans la source ; horodatages conservés sans conversion",
        "pressure_columns": list(pressure_columns),
    }
