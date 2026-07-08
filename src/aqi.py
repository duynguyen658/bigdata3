from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal


@dataclass(frozen=True)
class Breakpoint:
    concentration_low: float
    concentration_high: float
    index_low: int
    index_high: int


# U.S. EPA Air Quality Index (AQI) breakpoints.
# PM2.5 breakpoints reflect the EPA's 2024 NAAQS revision (effective 2024-05-06):
#   Good 0.0-9.0, Moderate 9.1-35.4, USG 35.5-55.4, Unhealthy 55.5-125.4,
#   Very Unhealthy 125.5-225.4, Hazardous 225.5-325.4(+).
PM25_BREAKPOINTS = [
    Breakpoint(0.0, 9.0, 0, 50),
    Breakpoint(9.1, 35.4, 51, 100),
    Breakpoint(35.5, 55.4, 101, 150),
    Breakpoint(55.5, 125.4, 151, 200),
    Breakpoint(125.5, 225.4, 201, 300),
    Breakpoint(225.5, 325.4, 301, 500),
]

# PM10 breakpoints are the EPA's original (unrevised) AQI table:
#   Good 0-54, Moderate 55-154, USG 155-254, Unhealthy 255-354,
#   Very Unhealthy 355-424, Hazardous 425-604(+).
PM10_BREAKPOINTS = [
    Breakpoint(0, 54, 0, 50),
    Breakpoint(55, 154, 51, 100),
    Breakpoint(155, 254, 101, 150),
    Breakpoint(255, 354, 151, 200),
    Breakpoint(355, 424, 201, 300),
    Breakpoint(425, 604, 301, 500),
]


def _truncate(value: float, decimals: int) -> float:
    """Truncate `value` toward zero at `decimals` precision, per EPA AQI guidance.

    EPA's AQI technical guidance requires truncating (never rounding) the raw
    concentration before matching a breakpoint bucket -- PM2.5 to 1 decimal
    place, PM10 to the nearest whole number -- which is what guarantees every
    truncated value lands inside a bucket instead of a breakpoint gap.
    Decimal(str(value)) is used (rather than float multiplication) so that
    binary floating-point artifacts, e.g. 9.05 actually stored as
    9.04999999999999982..., don't cause an incorrect truncation. Python's
    built-in round() is unsuitable here: it rounds half-to-even ("banker's
    rounding") rather than truncating, which is not the EPA rule.
    """
    quantum = Decimal(1).scaleb(-decimals)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_DOWN))


def pollutant_aqi(parameter: str, concentration: float | None) -> int | None:
    if concentration is None:
        return None
    parameter_key = parameter.lower().replace(".", "").replace("_", "")
    if parameter_key in {"pm25", "pm25um"}:
        breakpoints = PM25_BREAKPOINTS
        value = _truncate(max(float(concentration), 0.0), 1)
    elif parameter_key in {"pm10", "pm10um"}:
        breakpoints = PM10_BREAKPOINTS
        value = _truncate(max(float(concentration), 0.0), 0)
    else:
        # Unsupported pollutant: do not silently fall back to PM10 breakpoints.
        return None

    for bp in breakpoints:
        if bp.concentration_low <= value <= bp.concentration_high:
            # The final AQI index (not the concentration) is rounded to the
            # nearest whole number per EPA convention -- a different rule
            # from the concentration truncation above.
            aqi = (
                (bp.index_high - bp.index_low)
                / (bp.concentration_high - bp.concentration_low)
                * (value - bp.concentration_low)
                + bp.index_low
            )
            return round(aqi)
    return 500


def aqi_category(aqi: int | None) -> str:
    if aqi is None:
        return "unknown"
    if aqi <= 50:
        return "good"
    if aqi <= 100:
        return "moderate"
    if aqi <= 150:
        return "unhealthy_sensitive"
    if aqi <= 200:
        return "unhealthy"
    if aqi <= 300:
        return "very_unhealthy"
    return "hazardous"


def combined_aqi(values_by_parameter: dict[str, float]) -> int | None:
    scores = [pollutant_aqi(parameter, value) for parameter, value in values_by_parameter.items()]
    scores = [score for score in scores if score is not None]
    return max(scores) if scores else None
