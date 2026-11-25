"""Valide les sorties PySpark contre la référence Python par fenêtre."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.metropt_validation import compare_outputs  # noqa: E402


def main() -> None:
    config = tomllib.loads((ROOT / "configs/metropt.toml").read_text(encoding="utf-8"))
    parser = argparse.ArgumentParser(description="Compare Python et PySpark MetroPT-3")
    parser.add_argument("--master", default=config["spark"]["master"])
    parser.add_argument("--spark-output", type=Path)
    args = parser.parse_args()
    label = args.master.replace("local[", "local").replace("]", "")
    configured_path = ROOT / config["spark"][f"{label}_output_path"]
    spark_path = args.spark_output or configured_path
    if not spark_path.is_absolute():
        spark_path = ROOT / spark_path
    summary = compare_outputs(
        ROOT / config["output"]["path"],
        spark_path,
        float(config["validation"]["absolute_tolerance"]),
        float(config["validation"]["relative_tolerance"]),
    )
    report_path = ROOT / f"reports/metropt_spark_validation_{label}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["equivalent"]:
        raise SystemExit("Les sorties Python et PySpark divergent au-delà des contrôles configurés.")


if __name__ == "__main__":
    main()
