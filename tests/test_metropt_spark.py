from __future__ import annotations

from pathlib import Path
import tomllib

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.metropt import process_csv
from src.metropt_spark import aggregate_metropt, create_spark_session, read_metropt_csv, write_metropt_parquet
from src.metropt_validation import compare_outputs


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session("local[1]", 2, app_name="MetroPT3UnitTests")
    yield session
    session.stop()


def _sample_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "timestamp": [
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:00",
                "2020-01-01 00:04:59",
                "2020-01-01 00:05:00",
                "2020-01-01 00:15:00",
            ],
            "Oil_temperature": [10.0, 10.0, 14.0, 20.0, None],
            "TP2": [2.0, 2.0, None, 8.0, 10.0],
            "TP3": [4.0, 4.0, 6.0, 8.0, 10.0],
            "H1": [3.0, 3.0, 5.0, 7.0, 9.0],
            "DV_pressure": [1.0, 1.0, 3.0, 5.0, 7.0],
            "Reservoirs": [5.0, 5.0, 7.0, 9.0, 11.0],
        }
    ).to_csv(path, index=False)


def test_spark_windows_population_std_nulls_and_empty_window(spark, tmp_path) -> None:
    config = tomllib.loads(Path("configs/metropt.toml").read_text(encoding="utf-8"))
    source = tmp_path / "sample.csv"
    _sample_csv(source)
    aggregation = config["aggregation"]
    parsed = read_metropt_csv(
        spark,
        source,
        aggregation["timestamp_column"],
        aggregation["temperature_column"],
        tuple(aggregation["pressure_columns"]),
        config["spark"]["timestamp_format"],
    )
    rows = aggregate_metropt(
        parsed,
        aggregation["temperature_column"],
        tuple(aggregation["pressure_columns"]),
        config["spark"]["window_duration"],
    ).orderBy("window_start").collect()
    assert [row.sample_count for row in rows] == [3, 1, 0, 1]
    assert rows[0].temperature_mean == pytest.approx(34 / 3)
    assert rows[0].temperature_std_population == pytest.approx((32 / 9) ** 0.5)
    assert rows[0].TP2_mean == 2.0
    assert rows[1].temperature_change_from_previous_window == pytest.approx(20 - 34 / 3)
    assert rows[2].temperature_mean is None
    assert rows[2].temperature_change_from_previous_window is None
    assert rows[3].temperature_change_from_previous_window is None


def test_spark_output_matches_python_reference_on_edge_sample(spark, tmp_path) -> None:
    source = tmp_path / "sample.csv"
    python_output = tmp_path / "python.parquet"
    spark_output = tmp_path / "spark.parquet"
    _sample_csv(source)
    process_csv(source, python_output, chunk_size=2)
    config = tomllib.loads(Path("configs/metropt.toml").read_text(encoding="utf-8"))
    aggregation = config["aggregation"]
    parsed = read_metropt_csv(
        spark,
        source,
        aggregation["timestamp_column"],
        aggregation["temperature_column"],
        tuple(aggregation["pressure_columns"]),
        config["spark"]["timestamp_format"],
    )
    output = aggregate_metropt(
        parsed,
        aggregation["temperature_column"],
        tuple(aggregation["pressure_columns"]),
        config["spark"]["window_duration"],
    )
    write_metropt_parquet(output, spark_output, tmp_path / "manifest.json", source, "local[1]", 2, "5 minutes", 2)
    summary = compare_outputs(python_output, spark_output)
    assert summary["equivalent"]
    assert summary["key_mismatch_count"] == 0
    assert summary["sample_count_mismatch_count"] == 0
