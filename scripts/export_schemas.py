#!/usr/bin/env python
"""Export JSON Schema for every frozen contract model (decision T032).

There is no schema registry in this project (§10.2) — these exported files are the
honest substitute, committed alongside the code so a reviewer can see the frozen shape
without running Python. Re-running this script must produce no diff; `test_contracts.py`
(T033) asserts the committed files match what the models generate right now, so a
contract change is a deliberate regenerate-and-commit, not an accident.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from voltstream.contracts.events import MeterReading
from voltstream.contracts.reference import TariffRecord, WeatherForecast

SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "src" / "voltstream" / "contracts" / "schemas"

MODELS: dict[str, type[BaseModel]] = {
    "meter_reading.json": MeterReading,
    "tariff_record.json": TariffRecord,
    "weather_forecast.json": WeatherForecast,
}


def export_schemas() -> None:
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    for filename, model in MODELS.items():
        schema = model.model_json_schema()
        text = json.dumps(schema, indent=2, sort_keys=True) + "\n"
        (SCHEMAS_DIR / filename).write_text(text, encoding="utf-8")


if __name__ == "__main__":
    export_schemas()
    print(f"Wrote {len(MODELS)} schema(s) to {SCHEMAS_DIR}")
