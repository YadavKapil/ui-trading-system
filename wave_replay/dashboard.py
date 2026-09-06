"""Self-contained HTML dashboard generation for continuous Wave replay reports."""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .normalize import canonical_json

DEFAULT_MAX_POINTS = 8_000


def generate_continuous_dashboard(
    *,
    report_path: str | Path,
    trace_path: str | Path,
    output_path: str | Path,
    chart_asset_path: str | Path,
    simulator_commit: str,
    title: str = "Wave Continuous Replay — Provisional 135 Sessions",
    max_points: int = DEFAULT_MAX_POINTS,
) -> dict[str, Any]:
    if max_points < 4:
        raise ValueError("max_points must be at least 4")
    report_path = Path(report_path)
    trace_path = Path(trace_path)
    output_path = Path(output_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    marker_times = _marker_timestamps(report)
    trace_data = _collect_trace_data(
        trace_path,
        max_points=max_points,
        required_times=marker_times,
    )
    payload = _build_payload(report, trace_data, max_points=max_points)
    chart_source = Path(chart_asset_path).read_text(encoding="utf-8")
    rendered = _render_html(
        title=title,
        report=report,
        payload=payload,
        chart_source=chart_source,
        simulator_commit=simulator_commit,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    return {
        "output": str(output_path.resolve()),
        "size_bytes": output_path.stat().st_size,
        "underlying_points": len(payload["underlying"]),
        "option_points": len(payload["option"]),
        "equity_points": len(payload["equity"]),
        "fill_markers": len(payload["markers"]["fills"]),
        "selection_markers": len(payload["markers"]["selections"]),
        "lifecycle_markers": len(payload["markers"]["lifecycle"]),
        "session_boundaries": len(payload["markers"]["boundaries"]),
        "missing_markers": sum(
            len(payload["markers"][name])
            for name in ("missing_option", "missing_underlying", "unanchored")
        ),
    }


def generate_provisional_trace_dashboard(
    *,
    trace_path: str | Path,
    collection_manifest_path: str | Path,
    output_path: str | Path,
    chart_asset_path: str | Path,
    simulator_commit: str,
    title: str,
    max_points: int = DEFAULT_MAX_POINTS,
) -> dict[str, Any]:
    if max_points < 4:
        raise ValueError("max_points must be at least 4")
    trace_path = Path(trace_path)
    manifest = json.loads(Path(collection_manifest_path).read_text(encoding="utf-8"))
    events = _collect_provisional_events(trace_path)
    required_times = {
        marker["time"]
        for name in ("fills", "selections", "lifecycle", "missing", "boundaries")
        for marker in events["markers"][name]
    }
    trace_data = _sample_trace_bars(
        trace_path,
        counts=events["bar_counts"],
        max_points=max_points,
        required_times=required_times,
    )
    option = list(trace_data["option"])
    existing = {point["time"] for point in option}
    option.extend(
        {"time": timestamp}
        for timestamp in sorted(required_times - existing)
    )
    payload = {
        "underlying": trace_data["underlying"],
        "option": _consolidate(option),
        "inventory": _consolidate(_inventory_points(events["fills"])),
        "markers": events["markers"],
    }
    chart_source = Path(chart_asset_path).read_text(encoding="utf-8")
    rendered = _render_provisional_html(
        title=title,
        manifest=manifest,
        payload=payload,
        simulator_commit=simulator_commit,
        trace_sha256=events["trace_sha256"],
        sessions=events["sessions"],
    )
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered.replace("/*__CHART_SOURCE__*/", chart_source), encoding="utf-8")
    return {
        "output": str(output_path.resolve()),
        "size_bytes": output_path.stat().st_size,
        "trace_sha256": events["trace_sha256"],
        "sessions": len(events["sessions"]),
        "underlying_points": len(payload["underlying"]),
        "option_points": len(payload["option"]),
        "fill_markers": len(events["markers"]["fills"]),
        "selection_markers": len(events["markers"]["selections"]),
        "lifecycle_markers": len(events["markers"]["lifecycle"]),
        "missing_markers": len(events["markers"]["missing"]),
        "session_boundaries": len(events["markers"]["boundaries"]),
    }


def _collect_provisional_events(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    order_symbols: dict[tuple[str, str], str] = {}
    fills = []
    selections = []
    lifecycle = []
    missing = []
    boundaries = []
    sessions: set[str] = set()
    bar_counts = {"underlying": 0, "option": 0}
    with path.open("rb") as handle:
        for raw in handle:
            digest.update(raw)
            event = json.loads(raw)
            session = event.get("continuous_session_date")
            if session:
                sessions.add(session)
            kind = event.get("kind")
            details = event.get("details", {})
            if kind == "BAR_AVAILABLE":
                bar_counts[
                    "underlying" if details.get("lean_symbol") == "NIFTY" else "option"
                ] += 1
            elif kind == "BROKER_ORDER_TRANSITION":
                order_symbols[(session, str(details.get("order_id")))] = str(
                    details.get("tradingsymbol", "")
                )
            elif kind == "BROKER_FILL":
                fills.append({
                    "time": _epoch(event["at"]),
                    "at": event["at"],
                    "session": session,
                    "order_id": str(details["order_id"]),
                    "side": details["side"],
                    "quantity": int(details["quantity"]),
                    "price": details["price"],
                })
            elif kind == "TRACE_HEADER":
                contract = details.get("contract", {})
                selections.append({
                    "time": _epoch(event["at"]),
                    "position": "aboveBar",
                    "color": "#22c55e",
                    "shape": "circle",
                    "text": f"START · {contract.get('wave_trading_symbol', '')}",
                })
            elif kind == "SIMULATOR_ROLL_LIQUIDATION":
                lifecycle.append({
                    "time": _epoch(event["at"]),
                    "position": "aboveBar",
                    "color": "#e879f9",
                    "shape": "square",
                    "text": f"SWITCH/ROLL · {event.get('actor', '')}",
                })
            elif kind == "MISSING_QUOTE_POLICY":
                missing.append({
                    "time": _epoch(event["at"]),
                    "position": "belowBar",
                    "color": "#ef4444",
                    "shape": "circle",
                    "text": "Missing · skip",
                })
            elif kind == "EXCHANGE_SESSION_CLOSE":
                boundaries.append({
                    "time": _epoch(event["at"]),
                    "position": "belowBar",
                    "color": "#475569",
                    "shape": "square",
                    "text": session,
                })
    fill_markers = []
    for fill in fills:
        symbol = order_symbols.get((fill["session"], fill["order_id"]), "Wave option")
        fill["symbol"] = symbol
        fill_markers.append({
            "time": fill["time"],
            "position": "belowBar" if fill["side"] == "BUY" else "aboveBar",
            "color": "#4da3ff" if fill["side"] == "BUY" else "#f59e0b",
            "shape": "arrowUp" if fill["side"] == "BUY" else "arrowDown",
            "text": f"{fill['side'][0]} {fill['quantity']} · {symbol}",
        })
    return {
        "trace_sha256": digest.hexdigest(),
        "sessions": sorted(sessions),
        "bar_counts": bar_counts,
        "fills": fills,
        "markers": {
            "fills": fill_markers,
            "selections": selections,
            "lifecycle": lifecycle,
            "missing": missing,
            "boundaries": boundaries,
        },
    }


def _collect_trace_data(
    path: Path,
    *,
    max_points: int = DEFAULT_MAX_POINTS,
    required_times: set[int] | None = None,
) -> dict[str, Any]:
    required_times = required_times or set()
    counts = {"underlying": 0, "option": 0}
    boundaries = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            kind = event.get("kind")
            if kind == "BAR_AVAILABLE":
                details = event["details"]
                if details.get("lean_symbol") == "NIFTY":
                    counts["underlying"] += 1
                else:
                    counts["option"] += 1
            elif kind == "EXCHANGE_SESSION_CLOSE":
                boundaries.append({
                    "time": _epoch(event["at"]),
                    "session": event["continuous_session_date"],
                })
    required_times = required_times | {item["time"] for item in boundaries}
    sampled = _sample_trace_bars(
        path,
        counts=counts,
        max_points=max_points,
        required_times=required_times,
    )
    return {
        **sampled,
        "boundaries": boundaries,
    }


def _sample_trace_bars(
    path: Path,
    *,
    counts: dict[str, int],
    max_points: int,
    required_times: set[int],
) -> dict[str, list[dict[str, Any]]]:
    samplers = {
        name: _StreamingMinMaxSampler(
            total=count,
            limit=max_points,
            required_times=required_times,
        )
        for name, count in counts.items()
    }
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            if event.get("kind") != "BAR_AVAILABLE":
                continue
            details = event["details"]
            name = "underlying" if details.get("lean_symbol") == "NIFTY" else "option"
            samplers[name].add({
                "time": _epoch(event["at"]),
                "value": float(details["close"]),
                "session": event["continuous_session_date"],
                "symbol": details.get("wave_trading_symbol") or details.get("lean_symbol"),
            })
    return {
        "underlying": samplers["underlying"].finish(),
        "option": samplers["option"].finish(),
    }


class _StreamingMinMaxSampler:
    def __init__(
        self,
        *,
        total: int,
        limit: int,
        required_times: set[int],
    ) -> None:
        if limit < 4:
            raise ValueError("limit must be at least 4")
        if len(required_times) >= limit:
            raise ValueError(
                f"limit={limit} cannot retain {len(required_times)} required marker timestamps"
            )
        remaining = limit - len(required_times)
        if remaining < 4:
            raise ValueError(
                f"limit={limit} leaves fewer than four slots after required markers"
            )
        target_buckets = remaining // 4
        self.bucket_size = max(1, (total + target_buckets - 1) // target_buckets)
        self.required_times = required_times
        self.bucket: list[dict[str, Any]] = []
        self.selected: list[dict[str, Any]] = []

    def add(self, point: dict[str, Any]) -> None:
        self.bucket.append(point)
        if len(self.bucket) >= self.bucket_size:
            self._flush()

    def finish(self) -> list[dict[str, Any]]:
        self._flush()
        return _consolidate(self.selected)

    def _flush(self) -> None:
        if not self.bucket:
            return
        choices = [
            self.bucket[0],
            min(self.bucket, key=lambda item: item["value"]),
            max(self.bucket, key=lambda item: item["value"]),
            self.bucket[-1],
            *(
                point
                for point in self.bucket
                if point["time"] in self.required_times
            ),
        ]
        self.selected.extend(choices)
        self.bucket = []


def _marker_timestamps(report: dict[str, Any]) -> set[int]:
    values = [
        fill["at"]
        for fill in report["activity"]["fill_events"]
    ]
    values.extend(
        session["contract_decision"]["at"]
        for session in report["sessions"]
        if session.get("contract_decision", {}).get("at")
    )
    values.extend(
        event["at"]
        for collection in (report["switches"], report["rolls"])
        for event in collection
    )
    values.extend(
        item["timestamp"]
        for item in report["missing_data"]
        if item.get("timestamp")
    )
    return {_epoch(value) for value in values}


def _build_payload(
    report: dict[str, Any],
    trace_data: dict[str, Any],
    *,
    max_points: int,
) -> dict[str, Any]:
    equity_source = [
        {
            "time": _epoch(point["at"]),
            "value": None if point["equity"] is None else float(point["equity"]),
            "realized": float(point["realized_pnl"]),
            "unrealized": (
                None if point["unrealized_pnl"] is None else float(point["unrealized_pnl"])
            ),
            "fees": float(point["fees"]),
        }
        for point in report["equity_curve"]
    ]
    drawdown_source = _drawdown_points(equity_source)
    inventory = _inventory_points(report["activity"]["fill_events"])
    missing_markers = [
        {
            "time": _epoch(item["timestamp"]),
            "position": "belowBar",
            "color": "#ef4444",
            "shape": "circle",
            "text": "Missing · skip",
        }
        for item in report["missing_data"]
        if item.get("timestamp")
    ]
    fill_markers = [
            {
                "time": _epoch(fill["at"]),
                "position": "belowBar" if fill["side"] == "BUY" else "aboveBar",
                "color": "#4da3ff" if fill["side"] == "BUY" else "#f59e0b",
                "shape": "arrowUp" if fill["side"] == "BUY" else "arrowDown",
                "text": f"{fill['side'][0]} {fill['quantity']} · {fill['symbol']}",
            }
            for fill in report["activity"]["fill_events"]
        ]
    selection_markers = [
            {
                "time": _epoch(session["contract_decision"]["at"]),
                "position": "aboveBar",
                "color": "#22c55e",
                "shape": "circle",
                "text": (
                    f"{session['contract_decision']['action'].upper()} · "
                    f"{session['contract']['wave_trading_symbol']}"
                ),
            }
            for session in report["sessions"]
            if session.get("contract")
            and session.get("contract_decision", {}).get("at")
            and session["contract_decision"].get("action") in {"select", "switch"}
        ]
    lifecycle_markers = [
            {
                "time": _epoch(event["at"]),
                "position": "aboveBar",
                "color": "#e879f9" if kind == "switch" else "#f97316",
                "shape": "square",
                "text": (
                    f"{kind.upper()} · "
                    f"{event['old_contract']['wave_trading_symbol']}"
                ),
            }
            for kind, values in (("switch", report["switches"]), ("roll", report["rolls"]))
            for event in values
        ]
    option_marker_times = {
        marker["time"]
        for marker in [
            *fill_markers,
            *selection_markers,
            *lifecycle_markers,
            *missing_markers,
        ]
    }
    option_series = list(trace_data["option"])
    existing_option_times = {point["time"] for point in option_series}
    option_series.extend(
        {"time": timestamp}
        for timestamp in sorted(option_marker_times - existing_option_times)
    )
    markers = {
        "fills": fill_markers,
        "selections": selection_markers,
        "lifecycle": lifecycle_markers,
        "boundaries": [
            {
                "time": item["time"],
                "position": "belowBar",
                "color": "#475569",
                "shape": "square",
                "text": item["session"],
            }
            for item in trace_data["boundaries"]
        ],
        "missing_option": missing_markers,
        "missing_underlying": [],
        "unanchored": [],
    }
    return {
        "underlying": trace_data["underlying"],
        "option": _consolidate(option_series),
        "equity": _downsample_nullable(equity_source, max_points),
        "realized": _metric_series(equity_source, "realized", max_points),
        "unrealized": _metric_series(equity_source, "unrealized", max_points),
        "fees": _metric_series(equity_source, "fees", max_points),
        "drawdown": _downsample_nullable(drawdown_source, max_points),
        "inventory": _consolidate(inventory),
        "event_anchors": [],
        "markers": markers,
    }


def _downsample(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit < 4:
        raise ValueError("limit must be at least 4")
    if limit < 4:
        raise ValueError("limit must be at least 4")
    points = _consolidate(points)
    if len(points) <= limit:
        return points
    bucket_size = max(1, (len(points) + limit // 4 - 1) // (limit // 4))
    selected: list[dict[str, Any]] = []
    for start in range(0, len(points), bucket_size):
        bucket = points[start : start + bucket_size]
        choices = [bucket[0], min(bucket, key=lambda item: item["value"]), max(bucket, key=lambda item: item["value"]), bucket[-1]]
        for point in sorted({item["time"]: item for item in choices}.values(), key=lambda item: item["time"]):
            if not selected or selected[-1]["time"] != point["time"]:
                selected.append(point)
    return selected


def _consolidate(points: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_time = {point["time"]: point for point in points}
    return [by_time[timestamp] for timestamp in sorted(by_time)]


def _downsample_nullable(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    known = [{"time": item["time"], "value": item["value"]} for item in points if item["value"] is not None]
    return _downsample(known, limit)


def _metric_series(
    points: list[dict[str, Any]],
    field: str,
    limit: int,
) -> list[dict[str, Any]]:
    return _downsample_nullable(
        [{"time": item["time"], "value": item[field]} for item in points],
        limit,
    )


def _drawdown_points(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak: float | None = None
    result = []
    for point in points:
        value = point["value"]
        if value is None:
            continue
        peak = value if peak is None else max(peak, value)
        result.append({"time": point["time"], "value": value - peak})
    return result


def _inventory_points(fills: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    inventory: dict[str, int] = {}
    result = []
    for fill in sorted(fills, key=lambda item: (item["at"], item["order_id"])):
        signed = int(fill["quantity"]) if fill["side"] == "BUY" else -int(fill["quantity"])
        symbol = fill["symbol"]
        inventory[symbol] = inventory.get(symbol, 0) + signed
        result.append({
            "time": _epoch(fill["at"]),
            "value": sum(abs(quantity) for quantity in inventory.values()),
        })
    return result


def _epoch(value: str) -> int:
    return int(dt.datetime.fromisoformat(value).timestamp())


def _render_html(
    *,
    title: str,
    report: dict[str, Any],
    payload: dict[str, Any],
    chart_source: str,
    simulator_commit: str,
) -> str:
    collection = report["collection"]
    account = report["account"]
    drawdown = report["drawdown"]
    activity = report["activity"]
    metadata = {
        "collection_id": collection["collection_id"],
        "collection_sha256": collection["collection_sha256"],
        "simulator_commit": simulator_commit,
        "trace_digest": report["trace_digest"],
        "report_hash": report["report_hash"],
        "range": report["requested_range"],
    }
    session_count = len(report["sessions"])
    data_json = canonical_json({"charts": payload}).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title>
<style>
:root{{--bg:#0b0e13;--panel:#121720;--panel2:#171d27;--line:#2a3340;--text:#e7edf5;--muted:#8b98a8;--blue:#4da3ff;--amber:#f59e0b;--green:#22c55e;--red:#ef4444;--purple:#e879f9}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font:13px/1.45 Inter,Segoe UI,Arial,sans-serif}} .wrap{{max-width:1540px;margin:auto;padding:18px}}
h1{{font-size:22px;margin:0}} h2{{font-size:15px;margin:0 0 10px}} .sub,.meta,.note{{color:var(--muted)}} .top{{display:flex;justify-content:space-between;gap:20px;align-items:flex-start;margin-bottom:14px}}
.badge{{border:1px solid #7c5a16;color:#fbbf24;background:#211b0f;padding:5px 9px;border-radius:6px;font-weight:700}} .grid{{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:8px;margin:12px 0}}
.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:9px;box-shadow:0 5px 18px rgba(0,0,0,.22)}} .card{{padding:10px 12px}} .label{{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em}} .value{{font-size:20px;font-weight:700;margin-top:2px}}
.charts{{display:grid;grid-template-columns:1fr 1fr;gap:10px}} .panel{{padding:12px;margin-bottom:10px}} .wide{{grid-column:1/-1}} .chart{{height:350px}} .small-chart{{height:250px}}
.legend{{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin-bottom:8px}} .dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px}}
.controls{{display:flex;gap:12px;flex-wrap:wrap;margin:8px 0}} label{{color:var(--muted)}} table{{width:100%;border-collapse:collapse}} th,td{{text-align:right;padding:6px 8px;border-bottom:1px solid var(--line)}} th:first-child,td:first-child{{text-align:left}} th{{color:var(--muted);font-size:10px;text-transform:uppercase}}
.tables{{display:grid;grid-template-columns:1fr 2fr;gap:10px}} code{{color:#b7c3d4;word-break:break-all}} @media(max-width:900px){{.grid{{grid-template-columns:repeat(2,1fr)}}.charts,.tables{{grid-template-columns:1fr}}.wide{{grid-column:auto}}.chart{{height:300px}}}}
</style>
<script>{chart_source}</script>
</head>
<body><main class="wrap">
<div class="top"><div><h1>{html.escape(title)}</h1><div class="sub">Deterministic close-only Wave replay · IST · offline result</div></div><div class="badge">PROVISIONAL · {session_count} SESSIONS</div></div>
<section class="grid">
{_metric_card("Boundary equity", account["ending_equity"], "INR")}
{_metric_card("Boundary P&L", account["pnl"], "INR")}
{_metric_card("Realized P&L", account["realized_pnl"], "INR")}
{_metric_card("Unrealized P&L", account["unrealized_pnl"], "INR")}
{_metric_card("Fees", account["fees"], "INR")}
{_metric_card("Max drawdown", drawdown["max_drawdown_pct"], "ratio")}
{_metric_card("Orders / fills", f"{activity['orders']} / {activity['fills']}", "")}
{_metric_card("Switches / rolls", f"{len(report['switches'])} / {len(report['rolls'])}", "")}
{_metric_card("No-trade days", len(report["no_trade_days"]), "")}
{_metric_card("Skipped minutes", report["missing_data_summary"]["skipped_minutes"], "")}
{_metric_card("Max inventory", report["inventory"]["max_total_absolute_quantity"], "")}
{_metric_card("Ending position", sum(abs(int(v["quantity"])) for v in account["positions"].values()), "")}
</section>
<section class="panel wide"><h2>NIFTY and selected option path</h2>
<div class="legend"><span><i class="dot" style="background:var(--blue)"></i>NIFTY</span><span><i class="dot" style="background:var(--amber)"></i>Selected option</span><span><i class="dot" style="background:#4da3ff"></i>BUY</span><span><i class="dot" style="background:#f59e0b"></i>SELL</span><span><i class="dot" style="background:var(--green)"></i>Selection</span><span><i class="dot" style="background:var(--purple)"></i>Switch/roll</span></div>
<div class="controls"><label><input type="checkbox" id="fills" checked> fills</label><label><input type="checkbox" id="selections" checked> selections</label><label><input type="checkbox" id="lifecycle" checked> switches/rolls</label><label><input type="checkbox" id="missing" checked> missing</label><label><input type="checkbox" id="boundaries"> session boundaries</label></div>
<div id="market-chart" class="chart"></div></section>
<div class="charts"><section class="panel"><h2>Equity and drawdown</h2><div id="equity-chart" class="chart"></div></section>
<section class="panel"><h2>Realized / unrealized P&L and fees</h2><div id="pnl-chart" class="chart"></div></section>
<section class="panel"><h2>Absolute inventory after fills</h2><div id="inventory-chart" class="small-chart"></div></section>
<section class="panel"><h2>Boundary MTM vs forced-close sensitivity</h2>{_comparison_table(report)}</section></div>
<div class="tables"><section class="panel"><h2>Annual summary</h2>{_period_table(report["summaries"]["annual"])}</section><section class="panel"><h2>Monthly summary</h2>{_period_table(report["summaries"]["monthly"])}</section></div>
<section class="panel meta"><h2>Reproducibility</h2><div>Collection: <code>{html.escape(str(metadata["collection_id"]))}</code></div><div>Collection SHA: <code>{html.escape(str(metadata["collection_sha256"]))}</code></div><div>Simulator: <code>{html.escape(str(metadata["simulator_commit"]))}</code></div><div>Trace digest: <code>{html.escape(str(metadata["trace_digest"]))}</code></div><div>Report hash: <code>{html.escape(str(metadata["report_hash"]))}</code></div><div>Range: {html.escape(str(metadata["range"]["start"]))} → {html.escape(str(metadata["range"]["end"]))}</div><p class="note">Offline self-contained report. Continuous lines are deterministically min/max downsampled; every fill, selection, switch, roll, missing marker, and session boundary is retained. Prices and timestamps use completed-minute availability in IST. This provisional prefix is not the final 464-session result.</p></section>
</main>
<script>
const DATA={data_json};
const LC=LightweightCharts;
const ist=t=>new Intl.DateTimeFormat('en-IN',{{timeZone:'Asia/Kolkata',dateStyle:'medium',timeStyle:'short'}}).format(new Date(t*1000));
const base={{layout:{{background:{{type:'solid',color:'#121720'}},textColor:'#9aa7b8'}},grid:{{vertLines:{{color:'#1e2631'}},horzLines:{{color:'#1e2631'}}}},crosshair:{{mode:LC.CrosshairMode.Normal}},timeScale:{{timeVisible:true,secondsVisible:false,borderColor:'#334155',tickMarkFormatter:ist}},rightPriceScale:{{borderColor:'#334155'}},handleScroll:{{mouseWheel:true,pressedMouseMove:true}},handleScale:{{mouseWheel:true,pinch:true}},localization:{{timeFormatter:ist}}}};
function chart(id,height){{const c=LC.createChart(document.getElementById(id),{{...base,height}});new ResizeObserver(()=>c.applyOptions({{width:document.getElementById(id).clientWidth}})).observe(document.getElementById(id));return c}}
const market=chart('market-chart',350); const nifty=market.addLineSeries({{color:'#4da3ff',lineWidth:2,title:'NIFTY'}}); nifty.setData(DATA.charts.underlying); const option=market.addLineSeries({{color:'#f59e0b',lineWidth:2,title:'Selected option',priceScaleId:'left'}}); option.setData(DATA.charts.option);
const anchors=market.addLineSeries({{lineVisible:false,lastValueVisible:false,priceLineVisible:false,priceScaleId:'event-anchor'}}); anchors.setData(DATA.charts.event_anchors); anchors.priceScale().applyOptions({{visible:false}});
const markerGroups=DATA.charts.markers; function refreshMarkers(){{option.setMarkers(['fills','selections','lifecycle'].flatMap(k=>document.getElementById(k).checked?markerGroups[k]:[]).concat(document.getElementById('missing').checked?markerGroups.missing_option:[]).sort((a,b)=>a.time-b.time)); nifty.setMarkers((document.getElementById('boundaries').checked?markerGroups.boundaries:[]).concat(document.getElementById('missing').checked?markerGroups.missing_underlying:[]).sort((a,b)=>a.time-b.time)); anchors.setMarkers(document.getElementById('missing').checked?markerGroups.unanchored:[])}} document.querySelectorAll('.controls input').forEach(x=>x.addEventListener('change',refreshMarkers)); refreshMarkers(); market.timeScale().fitContent();
const eq=chart('equity-chart',350); const eqs=eq.addLineSeries({{color:'#22c55e',lineWidth:2,title:'Equity'}}); eqs.setData(DATA.charts.equity); const dds=eq.addHistogramSeries({{color:'#7f1d1d',priceScaleId:'left',title:'Drawdown'}}); dds.setData(DATA.charts.drawdown); eq.timeScale().fitContent();
const pnl=chart('pnl-chart',350); pnl.addLineSeries({{color:'#4da3ff',title:'Realized'}}).setData(DATA.charts.realized); pnl.addLineSeries({{color:'#f59e0b',title:'Unrealized'}}).setData(DATA.charts.unrealized); pnl.addLineSeries({{color:'#a78bfa',title:'Fees'}}).setData(DATA.charts.fees); pnl.timeScale().fitContent();
const inv=chart('inventory-chart',250); inv.addLineSeries({{color:'#e879f9',lineWidth:2,title:'Absolute quantity'}}).setData(DATA.charts.inventory); inv.timeScale().fitContent();
</script></body></html>"""


def _render_provisional_html(
    *,
    title: str,
    manifest: dict[str, Any],
    payload: dict[str, Any],
    simulator_commit: str,
    trace_sha256: str,
    sessions: list[str],
) -> str:
    data_json = canonical_json(payload).replace("</", "<\\/")
    marker_counts = {
        name: len(payload["markers"][name])
        for name in ("fills", "selections", "lifecycle", "missing", "boundaries")
    }
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title><style>
:root{{--bg:#0b0e13;--panel:#121720;--line:#2a3340;--text:#e7edf5;--muted:#8b98a8}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:13px/1.45 Segoe UI,Arial,sans-serif}}main{{max-width:1540px;margin:auto;padding:18px}}h1{{font-size:22px;margin:0}}h2{{font-size:15px;margin:0 0 8px}}.top{{display:flex;justify-content:space-between;gap:16px}}.badge{{background:#2b160b;border:1px solid #9a4e16;color:#fb923c;border-radius:6px;padding:6px 10px;font-weight:700}}.muted{{color:var(--muted)}}.grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin:14px 0}}.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:11px}}.label{{font-size:10px;color:var(--muted);text-transform:uppercase}}.value{{font-size:19px;font-weight:700}}.chart{{height:430px}}.small{{height:250px}}.panel{{margin-bottom:10px}}.legend,.controls{{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin:7px 0}}code{{color:#bdc9d8;word-break:break-all}}.warning{{border-left:3px solid #f97316;padding-left:10px}}@media(max-width:850px){{.grid{{grid-template-columns:repeat(2,1fr)}}.chart{{height:320px}}}}
</style><script>/*__CHART_SOURCE__*/</script></head><body><main>
<div class="top"><div><h1>{html.escape(title)}</h1><div class="muted">Trace-only full-period view · deterministic IST · economics pending pass 2</div></div><div class="badge">PROVISIONAL — PASS 2 VERIFICATION PENDING</div></div>
<div class="grid">{_metric_card("Sessions", len(sessions), "")}{_metric_card("Fills", marker_counts["fills"], "")}{_metric_card("Selections", marker_counts["selections"], "")}{_metric_card("Switches / rolls", marker_counts["lifecycle"], "")}{_metric_card("Missing markers", marker_counts["missing"], "")}{_metric_card("Boundaries", marker_counts["boundaries"], "")}</div>
<section class="panel"><h2>NIFTY and selected option path</h2><div class="legend">Blue: NIFTY · Amber: option · Blue arrows: BUY · Amber arrows: SELL · Green: worker/selection · Purple: switch/roll · Red: missing</div><div class="controls"><label><input id="fills" type="checkbox" checked> fills</label><label><input id="selections" type="checkbox" checked> selections</label><label><input id="lifecycle" type="checkbox" checked> switch/roll</label><label><input id="missing" type="checkbox" checked> missing</label><label><input id="boundaries" type="checkbox"> boundaries</label></div><div id="market" class="chart"></div></section>
<section class="panel"><h2>Absolute inventory after fills</h2><div id="inventory" class="small"></div></section>
<section class="panel warning"><h2>Pass-1 disclosure</h2><p>Generated from pass 1 while pass 2 remains active. Equity, drawdown, realized/unrealized P&amp;L, fees, monthly/yearly summaries, final boundary MTM, and forced-close sensitivity are intentionally withheld until the final deterministic report exists.</p>
<div>Collection: <code>{html.escape(str(manifest["collection_id"]))}</code></div><div>Collection SHA: <code>{html.escape(str(manifest["collection_sha256"]))}</code></div><div>Simulator: <code>{html.escape(simulator_commit)}</code></div><div>Pass-1 trace SHA256: <code>{trace_sha256}</code></div><div>Range: {html.escape(str(manifest["start_date"]))} → {html.escape(str(manifest["end_date"]))}</div><p class="muted">Offline, self-contained, no broker/network calls. Non-event price series are bounded min/max samples; all pass-1 fill, selection, lifecycle, missing, and boundary markers are retained.</p></section>
</main><script>
const DATA={data_json},LC=LightweightCharts,ist=t=>new Intl.DateTimeFormat('en-IN',{{timeZone:'Asia/Kolkata',dateStyle:'medium',timeStyle:'short'}}).format(new Date(t*1000));
const base={{layout:{{background:{{type:'solid',color:'#121720'}},textColor:'#9aa7b8'}},grid:{{vertLines:{{color:'#1e2631'}},horzLines:{{color:'#1e2631'}}}},crosshair:{{mode:LC.CrosshairMode.Normal}},timeScale:{{timeVisible:true,tickMarkFormatter:ist}},handleScroll:{{mouseWheel:true,pressedMouseMove:true}},handleScale:{{mouseWheel:true,pinch:true}},localization:{{timeFormatter:ist}}}};
function chart(id,h){{const c=LC.createChart(document.getElementById(id),{{...base,height:h}});new ResizeObserver(()=>c.applyOptions({{width:document.getElementById(id).clientWidth}})).observe(document.getElementById(id));return c}}
const c=chart('market',430),n=c.addLineSeries({{color:'#4da3ff',lineWidth:2,title:'NIFTY'}}),o=c.addLineSeries({{color:'#f59e0b',lineWidth:2,title:'Selected option',priceScaleId:'left'}});n.setData(DATA.underlying);o.setData(DATA.option);
function marks(){{o.setMarkers(['fills','selections','lifecycle','missing'].flatMap(k=>document.getElementById(k).checked?DATA.markers[k]:[]).sort((a,b)=>a.time-b.time));n.setMarkers(document.getElementById('boundaries').checked?DATA.markers.boundaries:[])}}document.querySelectorAll('.controls input').forEach(x=>x.addEventListener('change',marks));marks();c.timeScale().fitContent();
const i=chart('inventory',250);i.addLineSeries({{color:'#e879f9',lineWidth:2,title:'Absolute quantity'}}).setData(DATA.inventory);i.timeScale().fitContent();
</script></body></html>"""


def _metric_card(label: str, value: Any, kind: str) -> str:
    if value is None:
        rendered = "Unknown"
    elif kind == "INR":
        amount = Decimal(str(value))
        rendered = f"{'-' if amount < 0 else ''}₹{abs(amount):,.2f}"
    elif kind == "ratio":
        rendered = f"{Decimal(str(value)) * 100:.2f}%"
    else:
        rendered = str(value)
    return f'<div class="card"><div class="label">{html.escape(label)}</div><div class="value">{html.escape(rendered)}</div></div>'


def _period_table(rows: list[dict[str, Any]]) -> str:
    body = "".join(
        "<tr>"
        f"<td>{html.escape(str(row['period']))}</td>"
        f"<td>{html.escape(str(row.get('start_equity')))}</td>"
        f"<td>{html.escape(str(row.get('end_equity')))}</td>"
        f"<td>{html.escape(str(row.get('pnl')))}</td>"
        f"<td>{html.escape(str(row.get('fees')))}</td>"
        "</tr>"
        for row in rows
    )
    return f"<table><thead><tr><th>Period</th><th>Start equity</th><th>End equity</th><th>P&L</th><th>Fees</th></tr></thead><tbody>{body}</tbody></table>"


def _comparison_table(report: dict[str, Any]) -> str:
    boundary = report["final_boundary_mtm"]
    forced = report["forced_close_sensitivity"]
    rows = [
        ("Ending equity", boundary.get("ending_equity"), forced.get("ending_equity")),
        ("Realized P&L", boundary.get("realized_pnl"), forced.get("realized_pnl")),
        ("Unrealized P&L", boundary.get("unrealized_pnl"), forced.get("unrealized_pnl")),
        ("Fees", boundary.get("fees"), forced.get("total_fees")),
    ]
    body = "".join(
        f"<tr><td>{html.escape(label)}</td><td>{html.escape(_format_table_value(left))}</td><td>{html.escape(_format_table_value(right))}</td></tr>"
        for label, left, right in rows
    )
    return f"<table><thead><tr><th>Metric</th><th>Boundary MTM</th><th>Forced close</th></tr></thead><tbody>{body}</tbody></table>"


def _format_table_value(value: Any) -> str:
    if value is None:
        return "Unknown"
    amount = Decimal(str(value))
    return f"{'-' if amount < 0 else ''}₹{abs(amount):,.2f}"
