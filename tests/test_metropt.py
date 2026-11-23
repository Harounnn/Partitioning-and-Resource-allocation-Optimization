from __future__ import annotations

import pandas as pd

from src.metropt import aggregate_frame, process_csv


def test_five_minute_window_statistics_and_population_std() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": ["2020-01-01 00:00:00", "2020-01-01 00:04:59", "2020-01-01 00:05:00"],
            "Oil_temperature": [10.0, 14.0, 20.0],
            "TP2": [2.0, 4.0, 8.0],
        }
    )
    result = aggregate_frame(frame, pressure_columns=("TP2",))
    first = result.loc[pd.Timestamp("2020-01-01 00:00:00")]
    second = result.loc[pd.Timestamp("2020-01-01 00:05:00")]
    assert first["sample_count"] == 2
    assert first["temperature_mean"] == 12.0
    assert first["temperature_m2"] / first["sample_count"] == 4.0
    assert first["temperature_max"] == 14.0
    assert first["TP2_sum"] / first["TP2_count"] == 3.0
    assert second["sample_count"] == 1
    assert second["temperature_mean"] == 20.0


def test_streaming_merge_and_explicit_empty_window(tmp_path) -> None:
    input_path = tmp_path / "sample.csv"
    output_path = tmp_path / "sample.parquet"
    pd.DataFrame(
        {
            "timestamp": ["2020-01-01 00:00:00", "2020-01-01 00:04:59", "2020-01-01 00:10:00"],
            "Oil_temperature": [10.0, 14.0, 20.0],
            "TP2": [2.0, 4.0, 8.0],
        }
    ).to_csv(input_path, index=False)
    summary = process_csv(input_path, output_path, chunk_size=1, pressure_columns=("TP2",))
    result = pd.read_parquet(output_path).set_index("window_start")
    first = result.loc[pd.Timestamp("2020-01-01 00:00:00")]
    empty = result.loc[pd.Timestamp("2020-01-01 00:05:00")]
    last = result.loc[pd.Timestamp("2020-01-01 00:10:00")]
    assert summary["row_count"] == 3
    assert first["sample_count"] == 2
    assert first["temperature_mean"] == 12.0
    assert first["temperature_std_population"] == 2.0
    assert first["TP2_mean"] == 3.0
    assert empty["sample_count"] == 0
    assert pd.isna(empty["temperature_mean"])
    assert pd.isna(last["temperature_change_from_previous_window"])
