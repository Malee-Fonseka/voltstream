"""Write the daily report to a file (T131, §9 Phase 3).

The API serves the same figures as JSON; this writes them as Markdown to an output volume
so the day's report exists as an artefact rather than only as a request someone has to
know to make. That is what the brief means by a consolidated report: something produced,
not something available.

Markdown rather than CSV because the report is not one table — it has per-zone rows, a
billing summary, a run history and reject counts, and flattening those into a single CSV
would either lose structure or need four files. A Markdown file renders in a browser, in
the repository, and in the report appendix without conversion.

Run at the end of the billing DAG, after the rollup, so every section it can fill is
filled.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from voltstream.logging_setup import get_logger
from voltstream.storage import repositories

log = get_logger("report-generator")

_DEFAULT_OUTPUT_DIR = Path(os.environ.get("VOLTSTREAM_REPORT_DIR", "/var/lib/voltstream/reports"))


def _fmt(value: object, dp: int = 2) -> str:
    """Numbers to a fixed number of places; an absent value as an em dash.

    `-` rather than `0` for a missing figure: a zero and an unwritten section look the
    same in a table otherwise, and the difference is the whole point of reporting on a
    day that is still open.
    """
    if value is None:
        return "—"
    if isinstance(value, int):
        return f"{value:,}"
    try:
        return f"{float(value):,.{dp}f}"
    except (TypeError, ValueError):
        return str(value)


def render(sim_date: date) -> str:
    """Build the Markdown document for one simulated day."""
    finalised = repositories.is_day_finalised(sim_date)
    zones = repositories.get_zone_daily(sim_date)
    billing = repositories.get_billing_summary(sim_date)
    runs = repositories.get_run_summary(sim_date)
    rejected = repositories.get_rejected_for_day(sim_date)
    reconciliation = repositories.get_reconciliation_summary(sim_date)

    status = "FINAL" if finalised else "PROVISIONAL"
    lines = [
        f"# voltstream daily report — {sim_date.isoformat()}",
        "",
        f"**Status: {status}**",
        "",
    ]

    if not finalised:
        lines += [
            "> This day has not been closed by a successful billing run. Figures below",
            "> come from the speed layer where the batch layer has not yet written, and",
            "> are estimates computed against the previous day's tariff.",
            "",
        ]

    lines += ["## Grid operations by zone", ""]
    if zones:
        lines += [
            "| Zone | Consumption kWh | Solar kWh | Self-consumed | Exported "
            "| Renewable | Peak kWh | Meters |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for z in zones:
            lines.append(
                f"| {z.grid_zone} | {_fmt(z.total_consumption_kwh, 4)} | "
                f"{_fmt(z.total_solar_kwh, 4)} | {_fmt(z.self_consumed_kwh, 4)} | "
                f"{_fmt(z.export_kwh, 4)} | {_fmt(float(z.renewable_ratio) * 100, 1)}% | "
                f"{_fmt(z.peak_consumption_kwh, 4)} | {z.active_meters} |"
            )
    else:
        lines.append("_No zone rollup for this day yet._")
    lines.append("")

    lines += ["## Billing", ""]
    if billing:
        lines += [
            f"- Households billed: **{billing.households}**",
            f"- Total consumption: **{_fmt(billing.total_kwh, 4)} kWh**",
            f"- Total billed: **{_fmt(billing.total_billed)}**",
            f"- Readings processed: {_fmt(billing.readings_count)}",
            f"- Duplicates removed: {_fmt(billing.duplicates_removed)}",
        ]
    else:
        lines.append("_No finalised bills for this day yet._")
    lines.append("")

    lines += ["## Data quality", ""]
    if rejected:
        lines += ["| Reason | Records |", "|---|---:|"]
        lines += [f"| `{r.reason}` | {_fmt(r.total)} |" for r in rejected]
    else:
        lines.append("_No records rejected for this simulated day._")
    lines.append("")

    lines += ["## Speed versus batch", ""]
    if reconciliation:
        count, mean_abs, mean_pct = reconciliation
        lines += [
            f"- Households reconciled: **{count}**",
            f"- Mean absolute divergence: **{_fmt(mean_abs)}**",
            f"- Mean divergence: **{_fmt(mean_pct, 3)}%**",
        ]
    else:
        lines.append("_Reconciliation has not run for this day._")
    lines.append("")

    lines += ["## Pipeline runs", ""]
    if runs:
        lines += [
            "| Status | Rows in | Rows out | Finished | Orchestrator run |",
            "|---|---:|---:|---|---|",
        ]
        for r in runs:
            finished = r.finished_at.isoformat(timespec="seconds") if r.finished_at else "—"
            lines.append(
                f"| {r.status} | {_fmt(r.rows_in)} | {_fmt(r.rows_out)} | {finished} | "
                f"`{r.orchestrator_run_id or '—'}` |"
            )
        # Superseded rows are shown rather than filtered: a restated day has a history,
        # and a report that hides it is claiming the first answer never existed.
        if any(r.status == "superseded" for r in runs):
            lines += [
                "",
                "> This day was **restated**. Superseded runs are listed above; the figures",
                "> in this report come from the most recent successful run.",
            ]
    else:
        lines.append("_No billing runs recorded for this day._")
    lines.append("")

    return "\n".join(lines)


def write_report(sim_date: date, output_dir: Path | None = None) -> Path:
    directory = output_dir or _DEFAULT_OUTPUT_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"report_{sim_date.isoformat()}.md"
    path.write_text(render(sim_date), encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Write the daily report for one simulated day.")
    parser.add_argument("--sim-date", required=True, help="Simulated date, YYYY-MM-DD.")
    parser.add_argument("--out", default=None, help="Output directory.")
    args = parser.parse_args()

    # No pool: this runs as a one-shot task in the Spark image, where psycopg_pool is not
    # installed. repositories' transaction() opens a direct connection when no pool exists.
    try:
        sim_date = date.fromisoformat(args.sim_date)
        path = write_report(sim_date, Path(args.out) if args.out else None)
        log.info(
            "daily report written",
            extra={"stage": "report", "sim_date": sim_date.isoformat(), "path": str(path)},
        )
        print(str(path))
    except Exception as exc:  # noqa: BLE001 - exit code is the DAG's signal
        print(f"generate_report failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
