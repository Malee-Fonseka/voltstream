"""Contract drift guard (T033).

Regenerates each contract's JSON Schema in memory and asserts it matches the committed
file under `contracts/schemas/`. Changing a contract then becomes a deliberate act
(regenerate with `scripts/export_schemas.py` and commit) rather than an accident — which
is what "frozen at the end of Phase 1" (§6.2) has to mean mechanically, not just by
convention.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel
from scripts.export_schemas import MODELS, SCHEMAS_DIR

from voltstream.contracts.events import MeterReading


@pytest.mark.parametrize("filename,model", list(MODELS.items()))
def test_committed_schema_matches_the_model(
    filename: str, model: type[BaseModel]
) -> None:
    committed_path = SCHEMAS_DIR / filename
    assert committed_path.exists(), (
        f"{filename} is missing — run scripts/export_schemas.py and commit the result"
    )
    committed = json.loads(committed_path.read_text(encoding="utf-8"))
    current = model.model_json_schema()

    assert current == committed, (
        f"{filename} is stale relative to {model.__name__} — "
        "run scripts/export_schemas.py and commit the diff"
    )


def test_mutating_a_contract_field_breaks_the_guard() -> None:
    """Proves the guard actually bites: a field added at runtime changes the generated
    schema without touching the committed file, so the comparison above must fail for it."""
    live_schema = MeterReading.model_json_schema()
    committed_schema = json.loads(
        (SCHEMAS_DIR / "meter_reading.json").read_text(encoding="utf-8")
    )
    assert live_schema == committed_schema  # sanity: guard is not already broken

    mutated = json.loads(json.dumps(live_schema))
    mutated["properties"]["new_field"] = {"type": "string"}
    assert mutated != committed_schema
