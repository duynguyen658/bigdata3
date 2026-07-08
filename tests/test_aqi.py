from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.aqi import PM10_BREAKPOINTS, PM25_BREAKPOINTS, _truncate, aqi_category, combined_aqi, pollutant_aqi


# --- PM2.5 boundary correctness -------------------------------------------------

def test_pm25_upper_boundary_of_first_bucket():
    assert pollutant_aqi("pm25", 9.0) == 50


def test_pm25_lower_boundary_of_second_bucket():
    assert pollutant_aqi("pm25", 9.1) == 51


def test_pm25_gap_value_905_does_not_return_500():
    # 9.05 sits in the old breakpoint "gap" between 9.0 and 9.1. EPA truncation
    # (to 1 decimal) must resolve it to 9.0, not fall through to AQI 500.
    result = pollutant_aqi("pm25", 9.05)
    assert result != 500
    assert result == pollutant_aqi("pm25", 9.0) == 50


def test_pm25_truncation_never_rounds_up_into_next_bucket():
    # 35.45 truncated to 1 decimal is 35.4 (still bucket 2), not 35.5 (bucket 3).
    assert pollutant_aqi("pm25", 35.45) == pollutant_aqi("pm25", 35.4) == 100


def test_pm25_float_representation_artifact_905():
    # A literal 9.05 float is actually stored as 9.04999999999999982...; the
    # Decimal(str(value)) truncation path must still resolve it to 9.0.
    value = 9.05
    assert value != 9.0  # sanity: this is a genuinely distinct float
    assert pollutant_aqi("pm25", value) == 50


# --- PM10 boundary correctness --------------------------------------------------

def test_pm10_upper_boundary_of_first_bucket():
    assert pollutant_aqi("pm10", 54) == 50


def test_pm10_lower_boundary_of_second_bucket():
    assert pollutant_aqi("pm10", 55) == 51


def test_pm10_gap_value_549_does_not_return_500():
    # 54.9 sits in the gap between 54 and 55. EPA truncation (to integer)
    # must resolve it to 54, not fall through to AQI 500.
    result = pollutant_aqi("pm10", 54.9)
    assert result != 500
    assert result == pollutant_aqi("pm10", 54) == 50


def test_pm10_truncation_never_rounds_up_into_next_bucket():
    assert pollutant_aqi("pm10", 154.9) == pollutant_aqi("pm10", 154) == 100


# --- Unsupported pollutants must be rejected, not defaulted to PM10 ------------

def test_unsupported_pollutant_returns_none_not_pm10():
    assert pollutant_aqi("o3", 40.0) is None
    assert pollutant_aqi("co", 5.0) is None
    assert pollutant_aqi("no2", 20.0) is None
    assert pollutant_aqi("bogus-parameter", 999) is None


def test_pm25_and_pm10_key_variants_still_supported():
    assert pollutant_aqi("PM2.5", 9.0) == 50
    assert pollutant_aqi("pm_25", 9.0) == 50
    assert pollutant_aqi("PM10", 54) == 50
    assert pollutant_aqi("pm_10", 54) == 50


# --- Edge inputs -----------------------------------------------------------------

def test_none_concentration_returns_none():
    assert pollutant_aqi("pm25", None) is None


def test_negative_concentration_clamped_to_zero():
    assert pollutant_aqi("pm25", -5.0) == 0
    assert pollutant_aqi("pm10", -1.0) == 0


def _matches_a_bucket(truncated_value: float, breakpoints) -> bool:
    return any(bp.concentration_low <= truncated_value <= bp.concentration_high for bp in breakpoints)


def test_no_gap_in_breakpoint_tables_after_truncation():
    # Sweep across the full PM2.5/PM10 range at fine granularity and assert the
    # truncated value always lands inside a breakpoint bucket -- i.e. the
    # fallback path (`return 500` for values matching no bucket) is never
    # reached anywhere within the table's own range. (Near the very top of
    # the top bucket, the *computed* AQI can legitimately round to 500 too,
    # so this test checks bucket membership directly instead of the
    # returned AQI value, which would otherwise be ambiguous.)
    # PM2.5 table's top breakpoint tops out at 325.4; PM10's at 604.
    for i in range(0, 3255):
        concentration = i / 10.0
        truncated = _truncate(concentration, 1)
        assert _matches_a_bucket(truncated, PM25_BREAKPOINTS), concentration
        assert pollutant_aqi("pm25", concentration) is not None
    for i in range(0, 605):
        truncated = _truncate(float(i), 0)
        assert _matches_a_bucket(truncated, PM10_BREAKPOINTS), i
        assert pollutant_aqi("pm10", float(i)) is not None


# --- aqi_category / combined_aqi (unchanged behavior, still verified) ----------

def test_aqi_category_thresholds():
    assert aqi_category(None) == "unknown"
    assert aqi_category(50) == "good"
    assert aqi_category(100) == "moderate"
    assert aqi_category(150) == "unhealthy_sensitive"
    assert aqi_category(200) == "unhealthy"
    assert aqi_category(300) == "very_unhealthy"
    assert aqi_category(301) == "hazardous"


def test_combined_aqi_takes_max_of_pollutants():
    assert combined_aqi({"pm25": 9.05, "pm10": 54.9}) == 50
    assert combined_aqi({"pm25": 35.4, "pm10": 10}) == 100


def test_combined_aqi_ignores_unsupported_pollutants():
    assert combined_aqi({"pm25": 9.0, "o3": 999}) == 50


def test_combined_aqi_empty_returns_none():
    assert combined_aqi({}) is None
    assert combined_aqi({"o3": 999}) is None


def test_breakpoint_tables_are_contiguous_definitions():
    # Documents the intended contiguity: consecutive buckets' concentration
    # bounds differ by the smallest representable step at that precision.
    for low, high in zip(PM25_BREAKPOINTS, PM25_BREAKPOINTS[1:]):
        assert round(high.concentration_low - low.concentration_high, 1) == 0.1
    for low, high in zip(PM10_BREAKPOINTS, PM10_BREAKPOINTS[1:]):
        assert high.concentration_low - low.concentration_high == 1
