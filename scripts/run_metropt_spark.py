"""Exécute MetroPT-3 avec PySpark local depuis la racine du dépôt."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.metropt_spark import (  # noqa: E402
    aggregate_metropt,
    create_spark_session,
    read_metropt_csv,
    write_metropt_parquet,
)


def main() -> None:
    config = tomllib.loads((ROOT / "configs/metropt.toml").read_text(encoding="utf-8"))
    parser = argparse.ArgumentParser(description="Agrégation PySpark locale MetroPT-3")
    parser.add_argument("--master", default=config["spark"]["master"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    master = args.master
    label = master.replace("local[", "local").replace("]", "")
    output_value = args.output or ROOT / config["spark"][f"{label}_output_path"]
    output_path = output_value if output_value.is_absolute() else ROOT / output_value
    manifest_path = ROOT / f"data/manifests/metropt_spark_{label}.json"
    aggregation = config["aggregation"]
    spark_config = config["spark"]
    spark = create_spark_session(
        master,
        int(spark_config["shuffle_partitions"]),
        spark_config["session_timezone"],
    )
    try:
        measurements = read_metropt_csv(
            spark,
            ROOT / config["input"]["path"],
            aggregation["timestamp_column"],
            aggregation["temperature_column"],
            tuple(aggregation["pressure_columns"]),
            spark_config["timestamp_format"],
        )
        output = aggregate_metropt(
            measurements,
            aggregation["temperature_column"],
            tuple(aggregation["pressure_columns"]),
            spark_config["window_duration"],
        )
        manifest = write_metropt_parquet(
            output,
            output_path,
            manifest_path,
            ROOT / config["input"]["path"],
            master,
            int(spark_config["shuffle_partitions"]),
            spark_config["window_duration"],
            int(spark_config["parquet_batch_rows"]),
        )
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
