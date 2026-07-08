from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Breakpoint:
    concentration_low: float
    concentration_high: float
    index_low: int
    index_high: int


PM25_BREAKPOINTS = [
    Breakpoint(0.0, 9.0, 0, 50),
    Breakpoint(9.1, 35.4, 51, 100),
    Breakpoint(35.5, 55.4, 101, 150),
    Breakpoint(55.5, 125.4, 151, 200),
    Breakpoint(125.5, 225.4, 201, 300),
    Breakpoint(225.5, 325.4, 301, 500),
]

PM10_BREAKPOINTS = [
    Breakpoint(0, 54, 0, 50),
    Breakpoint(55, 154, 51, 100),
    Breakpoint(155, 254, 101, 150),
    Breakpoint(255, 354, 151, 200),
    Breakpoint(355, 424, 201, 300),
    Breakpoint(425, 604, 301, 500),
]


def pollutant_aqi(parameter: str, concentration: float | None) -> int | None:
    if concentration is None:
        return None
    parameter_key = parameter.lower().replace(".", "").replace("_", "")
    breakpoints = PM25_BREAKPOINTS if parameter_key in {"pm25", "pm25um"} else PM10_BREAKPOINTS
    value = max(float(concentration), 0.0)

    for bp in breakpoints:
        if bp.concentration_low <= value <= bp.concentration_high:
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
