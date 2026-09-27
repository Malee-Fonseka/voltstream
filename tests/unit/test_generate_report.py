"""Unit tests for where the daily report goes (T131, R08).

`render` itself reads Postgres, so it is replaced with a fixed document here; what is under
test is that the document ends up in object storage, under the archive layout, rather than
on the disk of a container the DAG removes as soon as it exits.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from scripts import generate_report

from voltstream.config import get_config
from voltstream.storage import objectstore

_DAY = date(2026, 1, 2)
# Non-ASCII on purpose: the real report uses em dashes, and they must survive as UTF-8.
_DOCUMENT = "# voltstream daily report — 2026-01-02\n"


@pytest.fixture(autouse=True)
def _clear_config_cache() -> Iterator[None]:
    get_config.cache_clear()
    yield
    get_config.cache_clear()


@pytest.fixture
def rendered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_report, "render", lambda sim_date: _DOCUMENT)


@pytest.fixture
def stored(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, bytes, str]]:
    """Every put_object_bytes call, instead of a real object store."""
    calls: list[tuple[str, str, bytes, str]] = []

    def put(bucket: str, key: str, data: bytes, content_type: str) -> None:
        calls.append((bucket, key, data, content_type))

    monkeypatch.setattr(objectstore, "put_object_bytes", put)
    return calls


def _printed_location(capsys: pytest.CaptureFixture[str]) -> str:
    """The script's last stdout line; the JSON log line before it goes to stdout too."""
    return capsys.readouterr().out.strip().splitlines()[-1]


def test_the_report_key_sits_under_reports_in_the_archive_layout() -> None:
    assert objectstore.report_key(_DAY) == "reports/report_2026-01-02.md"


@pytest.mark.usefixtures("rendered")
def test_publishing_puts_the_rendered_markdown_in_the_archive_bucket(
    stored: list[tuple[str, str, bytes, str]],
) -> None:
    location = generate_report.publish_report(_DAY)

    bucket = get_config().minio.bucket_archive
    assert stored == [
        (
            bucket,
            "reports/report_2026-01-02.md",
            _DOCUMENT.encode("utf-8"),
            "text/markdown; charset=utf-8",
        )
    ]
    assert location == f"{bucket}/reports/report_2026-01-02.md"


def test_put_object_bytes_passes_the_content_type_to_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class _Client:
        def put_object(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(objectstore, "get_client", lambda: _Client())
    objectstore.put_object_bytes("bucket", "key.md", b"body", "text/markdown; charset=utf-8")

    assert calls == [
        {
            "Bucket": "bucket",
            "Key": "key.md",
            "Body": b"body",
            "ContentType": "text/markdown; charset=utf-8",
        }
    ]


@pytest.mark.usefixtures("rendered")
def test_out_writes_a_local_file_and_stores_nothing(
    tmp_path: Path,
    stored: list[tuple[str, str, bytes, str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sys.argv", ["generate_report.py", "--sim-date", "2026-01-02", "--out", str(tmp_path)]
    )
    generate_report.main()

    path = tmp_path / "report_2026-01-02.md"
    assert path.read_text(encoding="utf-8") == _DOCUMENT
    assert stored == []
    assert _printed_location(capsys) == str(path)


@pytest.mark.usefixtures("rendered")
def test_without_out_main_publishes(
    stored: list[tuple[str, str, bytes, str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("sys.argv", ["generate_report.py", "--sim-date", "2026-01-02"])
    generate_report.main()

    assert len(stored) == 1
    assert _printed_location(capsys).endswith("/reports/report_2026-01-02.md")


def test_a_failure_exits_non_zero_for_the_dag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def unreachable(sim_date: date) -> str:
        raise ConnectionError("object store unreachable")

    monkeypatch.setattr(generate_report, "publish_report", unreachable)
    monkeypatch.setattr("sys.argv", ["generate_report.py", "--sim-date", "2026-01-02"])

    with pytest.raises(SystemExit) as exited:
        generate_report.main()

    assert exited.value.code == 1
    assert "object store unreachable" in capsys.readouterr().err
