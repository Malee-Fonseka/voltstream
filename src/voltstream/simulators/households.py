"""The deterministic household roster (T042's distribution rule, restated here as code).

`docker/init/postgres/03_seed_households.sql` seeds the database from exactly this rule.
`simulators/meter_producer.py` and `simulators/reference_dropper.py` both need the same
household metadata (zone, tier, solar) before Postgres necessarily has anything in it —
Phase 5 (simulators) is built and runnable before Phase 8 (the storage layer) exists — so
both derive it from this one function instead of querying the database. The seeded table
is populated from the identical formula, so whichever a later reader uses, the answer for
a given household is the same.
"""

from __future__ import annotations

from typing import Literal, NamedTuple

BillingTier = Literal["TIER_1", "TIER_2", "TIER_3"]
_TIERS: tuple[BillingTier, ...] = ("TIER_1", "TIER_2", "TIER_3")


class Household(NamedTuple):
    household_id: str
    meter_id: str
    grid_zone: str
    billing_tier: BillingTier
    subsidy_flag: bool
    has_solar: bool


def household_roster(count: int, zones: list[str]) -> list[Household]:
    """Households `HH-0001..HH-{count:04d}`, zone/tier/subsidy/solar assigned by T042's
    rule: zone and tier cycle through the configured lists; every 4th household is
    subsidised; every 3rd has solar."""
    return [
        Household(
            household_id=f"HH-{n:04d}",
            meter_id=f"MTR-{n:04d}",
            grid_zone=zones[(n - 1) % len(zones)],
            billing_tier=_TIERS[(n - 1) % len(_TIERS)],
            subsidy_flag=(n % 4 == 0),
            has_solar=(n % 3 == 0),
        )
        for n in range(1, count + 1)
    ]
