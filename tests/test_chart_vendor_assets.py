"""Security checks for self-hosted dashboard chart libraries."""

from hashlib import sha256
from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[1]
REMOTE_SCRIPT_PATTERN = re.compile(
    r"""<script\b[^>]*\bsrc\s*=\s*["']https?://[^"']+["']""",
    re.IGNORECASE,
)

CHART_ASSETS = {
    "/static/vendor/chart.js/4.4.0/chart.umd.js": {
        "marker": b"Chart.js v4.4.0",
        "sha256": "321e3a3fa98da4aaa957d10be57cbb514de0989eed8f9d726b5d05902cd01904",
    },
    "/static/vendor/chart.js/4.5.1/chart.umd.min.js": {
        "marker": b"Chart.js v4.5.1",
        "sha256": "48444a82d4edcb5bec0f1965faacdde18d9c17db3063d042abada2f705c9f54a",
    },
    (
        "/static/vendor/lightweight-charts/4.1.0/"
        "lightweight-charts.standalone.production.js"
    ): {
        "marker": b"TradingView Lightweight Charts",
        "sha256": "78d2bcbd79556d4f67ae3e3f7776f74e3b46a499466615b1f99397c53cb4056f",
    },
}

CHART_PAGES = {
    "/positions": "/static/vendor/chart.js/4.4.0/chart.umd.js",
    "/sensex_positions": "/static/vendor/chart.js/4.4.0/chart.umd.js",
    "/trade-journal": "/static/vendor/chart.js/4.4.0/chart.umd.js",
    "/nifty_contributors": "/static/vendor/chart.js/4.5.1/chart.umd.min.js",
    "/cas_tracker/contributors": "/static/vendor/chart.js/4.5.1/chart.umd.min.js",
    (
        "/expiry_trade"
    ): (
        "/static/vendor/lightweight-charts/4.1.0/"
        "lightweight-charts.standalone.production.js"
    ),
}


@pytest.mark.parametrize(("page_url", "asset_url"), CHART_PAGES.items())
def test_chart_pages_load_only_local_executable_scripts(
    client, monkeypatch, page_url: str, asset_url: str
) -> None:
    monkeypatch.setattr("flask_app._setup_config_complete", lambda: True)
    response = client.get(page_url)

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert f'src="{asset_url}"' in html
    assert REMOTE_SCRIPT_PATTERN.search(html) is None


@pytest.mark.parametrize(("asset_url", "expected"), CHART_ASSETS.items())
def test_vendored_chart_asset_identity(client, asset_url: str, expected: dict) -> None:
    response = client.get(asset_url)

    assert response.status_code == 200
    assert expected["marker"] in response.data[:500]
    assert sha256(response.data).hexdigest() == expected["sha256"]


def test_source_has_no_remote_chart_runtime_references() -> None:
    source_files = [
        path
        for suffix in (
            "*.html",
            "*.htm",
            "*.jinja",
            "*.jinja2",
            "*.js",
            "*.mjs",
            "*.cjs",
            "*.ts",
            "*.tsx",
            "*.py",
            "*.md",
        )
        for path in ROOT.rglob(suffix)
        if not any(
            part in {".git", ".venv", "venv", "tests", "vendor"}
            for part in path.parts
        )
    ]

    violations = []
    for path in source_files:
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in REMOTE_SCRIPT_PATTERN.finditer(text):
            tag = match.group(0)
            if "chart.js" in tag.lower() or "lightweight-charts" in tag.lower():
                violations.append(f"{path.relative_to(ROOT)}: {tag}")

    assert violations == []
