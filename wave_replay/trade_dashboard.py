"""Offline strategy-specific dashboard for normalized Wave inventory episodes."""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from .dashboard import _capital_section, _comparison_table, _metric_card
from .normalize import canonical_json
from .trade_episodes import load_trade_episode_model


DEFAULT_MAX_POINTS = 8_000


def generate_trade_episode_dashboard(
    *,
    report_path: str | Path,
    trace_path: str | Path,
    output_path: str | Path,
    chart_asset_path: str | Path,
    simulator_commit: str,
    title: str = "Wave Individual Trade Episodes",
    max_points: int = DEFAULT_MAX_POINTS,
) -> dict[str, Any]:
    if max_points < 32:
        raise ValueError("max_points must be at least 32")
    report, model = load_trade_episode_model(report_path, trace_path)
    payload = build_trade_dashboard_payload(
        report,
        model,
        max_points=max_points,
    )
    chart_source = Path(chart_asset_path).read_text(encoding="utf-8")
    rendered = _render_trade_dashboard(
        report=report,
        payload=payload,
        title=title,
        chart_source=chart_source,
        simulator_commit=simulator_commit,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    return {
        "output": str(output.resolve()),
        "size_bytes": output.stat().st_size,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        **payload["counts"],
    }


def build_trade_dashboard_payload(
    report: dict[str, Any],
    model: dict[str, Any],
    *,
    max_points: int = DEFAULT_MAX_POINTS,
) -> dict[str, Any]:
    required_by_symbol: dict[str, set[str]] = {}
    for episode in model["episodes"]:
        required = required_by_symbol.setdefault(episode["symbol"], set())
        required.update(fill["at"] for fill in episode["fills"])
        required.add(episode["start_at"])
        required.add(episode["end_at"])
        if episode["virtual_forced_close"]:
            required.add(episode["virtual_forced_close"]["at"])
    for order in model["orders"]:
        required = required_by_symbol.setdefault(order["symbol"], set())
        required.update(point["at"] for point in order["limit_points"])
    option_by_symbol = {
        symbol: [
            _price_point(point)
            for point in _downsample_preserving(
                values,
                max_points,
                required_by_symbol.get(symbol, set()),
                value_key="close",
            )
        ]
        for symbol, values in model["market"]["option_by_symbol"].items()
    }
    underlying_required = {
        item["at"]
        for name in ("selections", "lifecycle", "missing", "session_boundaries")
        for item in model["markers"][name]
    }
    underlying = [
        _price_point(point)
        for point in _downsample_preserving(
            model["market"]["underlying"],
            max_points,
            underlying_required,
            value_key="close",
        )
    ]
    episodes = [_episode_payload(episode) for episode in model["episodes"]]
    orders = [_order_payload(order) for order in model["orders"]]
    return {
        "schema": "wave-replay-trade-dashboard-payload-v1",
        "episodes": episodes,
        "orders": orders,
        "pairs": model["pairs"],
        "market": {
            "underlying": underlying,
            "option_by_symbol": option_by_symbol,
        },
        "markers": {
            name: [_timed_marker(item) for item in values]
            for name, values in model["markers"].items()
        },
        "counts": model["counts"],
        "reconciliation": {
            "episode_net_pnl": str(
                sum(
                    (
                        Decimal(episode["display_net_pnl"])
                        for episode in model["episodes"]
                        if episode["display_net_pnl"] is not None
                    ),
                    Decimal("0"),
                )
            ),
            "forced_close_report_pnl": report["forced_close_sensitivity"]["pnl"],
        },
    }


def _downsample_preserving(
    points: list[dict[str, Any]],
    limit: int,
    required_at: set[str],
    *,
    value_key: str,
) -> list[dict[str, Any]]:
    if len(points) <= limit:
        return list(points)
    required_epochs = {_epoch(value) for value in required_at}
    required = {
        index: point
        for index, point in enumerate(points)
        if _epoch(point["at"]) in required_epochs
    }
    if len(required) >= limit:
        raise ValueError(
            f"limit={limit} cannot retain {len(required)} required timestamps"
        )
    remaining = limit - len(required)
    bucket_size = max(1, (len(points) + max(1, remaining // 4) - 1) // max(1, remaining // 4))
    selected: dict[int, dict[str, Any]] = dict(required)
    for start in range(0, len(points), bucket_size):
        bucket = list(enumerate(points[start : start + bucket_size], start=start))
        choices = [
            bucket[0],
            min(bucket, key=lambda item: Decimal(str(item[1][value_key]))),
            max(bucket, key=lambda item: Decimal(str(item[1][value_key]))),
            bucket[-1],
        ]
        for index, point in choices:
            selected[index] = point
    if len(selected) > limit:
        required_indexes = set(required)
        optional = [
            index for index in sorted(selected) if index not in required_indexes
        ]
        keep_optional = max(0, limit - len(required_indexes))
        if keep_optional < len(optional):
            stride = len(optional) / max(1, keep_optional)
            optional = [
                optional[min(int(position * stride), len(optional) - 1)]
                for position in range(keep_optional)
            ]
        keep = required_indexes | set(optional)
        selected = {index: selected[index] for index in keep}
    return [selected[index] for index in sorted(selected)]


def _episode_payload(episode: dict[str, Any]) -> dict[str, Any]:
    value = copy_without(episode, {"pnl_curve", "inventory_curve", "fills"})
    value.update(
        {
            "start_time": _epoch(episode["start_at"]),
            "end_time": _epoch(episode["end_at"]),
            "is_profitable": (
                episode["display_net_pnl"] is not None
                and Decimal(episode["display_net_pnl"]) > 0
            ),
            "is_overnight": episode["overnight_count"] > 0,
            "is_virtual": episode["virtual_forced_close"] is not None,
            "is_lifecycle": ":" in episode["exit_reason"],
            "start_date": episode["start_at"][:10],
            "end_date": episode["end_at"][:10],
            "calendar_dte_at_start": (
                (
                    dt.date.fromisoformat(episode["contract"]["expiry"])
                    - dt.date.fromisoformat(episode["start_at"][:10])
                ).days
                if episode["contract"].get("expiry")
                else None
            ),
            "fills": [
                {
                    **fill,
                    "time": _epoch(fill["at"]),
                }
                for fill in episode["fills"]
            ],
            "inventory_events": [
                {
                    **point,
                    "time": _epoch(point["at"]),
                    "value": point["quantity"],
                }
                for point in episode["inventory_curve"]
            ],
            "inventory_curve": [
                {
                    "time": _epoch(point["at"]),
                    "at": point["at"],
                    "value": point["quantity"],
                    "source": point["source"],
                }
                for point in _last_point_by_timestamp(
                    [
                        point
                        for point in episode["pnl_curve"]
                        if point["source"] != "virtual"
                    ]
                )
            ],
            "pnl_curve": [
                {
                    **point,
                    "time": _epoch(point["at"]),
                    "realized": _float_or_none(point["realized_pnl"]),
                    "unrealized": _float_or_none(point["unrealized_pnl"]),
                    "net": _float_or_none(point["net_pnl"]),
                    "fees": _float_or_none(point["cumulative_fees"]),
                }
                for point in _last_point_by_timestamp(episode["pnl_curve"])
            ],
        }
    )
    if value["virtual_forced_close"]:
        value["virtual_forced_close"]["time"] = _epoch(
            value["virtual_forced_close"]["at"]
        )
    return value


def _order_payload(order: dict[str, Any]) -> dict[str, Any]:
    return {
        **order,
        "transitions": [
            {**item, "time": _epoch(item["at"])}
            for item in order["transitions"]
        ],
        "limit_points": [
            {
                **item,
                "time": _epoch(item["at"]),
                "value": float(item["price"]),
            }
            for item in order["limit_points"]
        ],
    }


def _price_point(point: dict[str, Any]) -> dict[str, Any]:
    return {
        "time": _epoch(point["at"]),
        "value": float(point["close"]),
        "at": point["at"],
        "source": point["source"],
    }


def _last_point_by_timestamp(
    points: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_time = {_epoch(point["at"]): point for point in points}
    return [by_time[value] for value in sorted(by_time)]


def _timed_marker(item: dict[str, Any]) -> dict[str, Any]:
    return {**item, "time": _epoch(item["at"])}


def _epoch(value: str) -> int:
    return int(dt.datetime.fromisoformat(value).timestamp())


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)


def copy_without(value: dict[str, Any], excluded: set[str]) -> dict[str, Any]:
    return {
        key: json.loads(json.dumps(item))
        for key, item in value.items()
        if key not in excluded
    }


def _render_trade_dashboard(
    *,
    report: dict[str, Any],
    payload: dict[str, Any],
    title: str,
    chart_source: str,
    simulator_commit: str,
) -> str:
    account = report["account"]
    activity = report["activity"]
    warning = (
        "IN-SAMPLE OPTIMIZATION RESULT — January 2026 was used to select these "
        "parameters. Adjacent-month validation lost money. Do not treat this as "
        "production-ready performance."
    )
    summary = (
        '<section class="metric-grid">'
        + _metric_card("Forced-close P&L", report["forced_close_sensitivity"]["pnl"], "INR")
        + _metric_card("Boundary P&L", account["pnl"], "INR")
        + _metric_card("All fees", report["forced_close_sensitivity"]["total_fees"], "INR")
        + _metric_card("Episodes", payload["counts"]["episodes"], "")
        + _metric_card("Orders / fills", f"{activity['orders']} / {activity['fills']}", "")
        + _metric_card("Max inventory", report["inventory"]["max_total_absolute_quantity"], "")
        + "</section>"
        + '<section class="summary-grid"><article class="panel">'
        + "<h2>Boundary MTM vs virtual forced close</h2>"
        + _comparison_table(report)
        + "</article><article class=\"panel\">"
        + "<h2>Capital-audited January result</h2>"
        + _capital_section(report)
        + "</article></section>"
    )
    metadata = canonical_json(
        {
            "collection_id": report["collection"]["collection_id"],
            "collection_sha256": report["collection"]["collection_sha256"],
            "trace_digest": report["trace_digest"],
            "report_hash": report["report_hash"],
            "simulator_commit": simulator_commit,
        }
    )
    rendered = _TEMPLATE
    replacements = {
        "__TITLE__": html.escape(title),
        "__WARNING__": html.escape(warning),
        "__SUMMARY__": summary,
        "__DATA__": canonical_json(payload).replace("</", "<\\/"),
        "__CHART_SOURCE__": chart_source,
        "__METADATA__": html.escape(metadata),
    }
    for key, value in replacements.items():
        rendered = rendered.replace(key, value)
    return rendered


_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<style>
:root{--bg:#0b0e13;--panel:#121720;--panel2:#171d27;--line:#2a3340;--text:#e7edf5;--muted:#8b98a8;--blue:#4da3ff;--amber:#f59e0b;--green:#22c55e;--red:#ef4444;--purple:#e879f9}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:13px/1.45 Inter,Segoe UI,Arial,sans-serif}.wrap{max-width:1720px;margin:auto;padding:16px}h1{font-size:22px;margin:0}h2{font-size:15px;margin:0 0 10px}.sub,.muted,.note{color:var(--muted)}.warning{margin:12px 0;padding:12px;border:1px solid #b45309;background:#2a1808;color:#fbbf24;border-radius:9px;font-weight:700}.metric-grid{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:8px;margin:12px 0}.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:9px;box-shadow:0 5px 18px rgba(0,0,0,.22)}.card{padding:10px 12px}.label{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em}.value{font-size:20px;font-weight:700;margin-top:2px}.panel{padding:12px;margin-bottom:10px}.controls{display:grid;grid-template-columns:2fr repeat(6,minmax(110px,1fr));gap:8px;align-items:end}.controls label,.layer-controls label{color:var(--muted);font-size:11px}.controls input,.controls select{width:100%;background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:5px;padding:7px}.layer-controls{display:flex;gap:12px;flex-wrap:wrap;margin:8px 0}.workspace{display:grid;grid-template-columns:minmax(620px,1.7fr) minmax(420px,1fr);gap:10px}.chart{height:300px}.small-chart{height:220px}.ledger-wrap{overflow:auto;max-height:460px;border:1px solid var(--line);border-radius:7px}table{width:100%;border-collapse:collapse}th,td{text-align:right;padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child,td:first-child{text-align:left}th{position:sticky;top:0;background:var(--panel2);color:var(--muted);font-size:10px;text-transform:uppercase;cursor:pointer;z-index:2}tr.episode-row{cursor:pointer}tr.episode-row:hover,tr.episode-row.selected{background:#1f2937}.positive{color:#4ade80}.negative{color:#f87171}.badge{display:inline-block;padding:2px 6px;border-radius:999px;font-size:10px;border:1px solid var(--line)}.observed{color:#93c5fd}.derived{color:#fbbf24}.virtual{color:#f0abfc}.episode-head{display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap}.nav button,.reset{background:#1d4ed8;color:white;border:0;border-radius:5px;padding:6px 10px;cursor:pointer}.nav button:disabled{opacity:.4}.pair-table{max-height:300px;overflow:auto}.summary-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.summary-grid .panel{overflow:auto}.meta code{word-break:break-all;color:#b7c3d4}.hidden{display:none!important}@media(max-width:1000px){.metric-grid{grid-template-columns:repeat(2,1fr)}.controls{grid-template-columns:repeat(2,1fr)}.workspace,.summary-grid{grid-template-columns:minmax(0,1fr)}.workspace>*,.panel{min-width:0}.chart{height:280px}}@media(max-width:520px){.wrap{padding:8px}.controls{grid-template-columns:1fr}.metric-grid{grid-template-columns:1fr 1fr}.value{font-size:16px}.ledger-wrap{max-height:340px}.chart{height:240px}}
</style>
<script>__CHART_SOURCE__</script>
</head>
<body><main class="wrap">
<header><h1>__TITLE__</h1><div class="sub">Wave exact-contract inventory episodes · deterministic close-only replay · IST · offline</div></header>
<div class="warning">__WARNING__</div>
__SUMMARY__
<section class="panel">
<div class="controls">
<label>Search<input id="search" placeholder="episode, symbol, reason"></label>
<label>Outcome<select id="outcome"><option value="">All</option><option value="profit">Profitable</option><option value="loss">Loss</option></select></label>
<label>Contract<select id="contract"><option value="">All</option></select></label>
<label>Start side<select id="direction"><option value="">All</option><option>LONG</option><option>SHORT</option></select></label>
<label>Exit<select id="exit"><option value="">All</option><option value="overnight">Overnight</option><option value="lifecycle">Switch / roll</option><option value="virtual">Boundary + virtual</option></select></label>
<label>Min max inventory<input id="min-inventory" type="number" min="0" value="0"></label>
<label>Start date<input id="date-from" type="date"></label>
<label>End date<input id="date-to" type="date"></label>
</div>
<div class="layer-controls">
<button class="reset" id="reset">Reset filters</button>
<label><input id="show-fills" type="checkbox" checked> fills</label>
<label><input id="show-limits" type="checkbox" checked> observed limits</label>
<label><input id="show-pairs" type="checkbox" checked> pair labels</label>
<label><input id="show-lifecycle" type="checkbox" checked> lifecycle</label>
<label><input id="show-missing" type="checkbox" checked> missing data</label>
<label><input id="show-virtual" type="checkbox" checked> virtual forced close</label>
</div>
<div class="episode-head"><div><strong id="visible-count"></strong><span class="muted"> · click an episode to synchronize every panel</span></div><div class="nav"><button id="previous">← Previous</button> <button id="next">Next →</button></div></div>
<div class="ledger-wrap"><table id="ledger"><thead><tr>
<th data-sort="episode_id">Episode</th><th data-sort="symbol">Contract</th><th data-sort="opening_direction">Start</th><th data-sort="start_at">Opened</th><th data-sort="duration_seconds">Duration</th><th data-sort="display_net_pnl">Net P&amp;L</th><th data-sort="historical_fees">Fees</th><th data-sort="close_mark_mfe">Close MFE</th><th data-sort="close_mark_mae">Close MAE</th><th data-sort="max_abs_inventory">Max inv</th><th data-sort="overnight_count">O/N</th><th data-sort="exit_reason">Exit</th>
</tr></thead><tbody></tbody></table></div>
</section>
<section class="panel"><div class="episode-head"><div><h2 id="episode-title">Select an episode</h2><div id="episode-subtitle" class="muted"></div></div><div><span class="badge observed">OBSERVED</span> <span class="badge derived">DERIVED</span> <span class="badge virtual">VIRTUAL</span></div></div></section>
<div class="workspace">
<div>
<section class="panel"><h2>Selected contract close and actual order limits</h2><div class="note">Completed-minute close only. Limit lines are observed order states; pair/cancel labels may be deterministically derived.</div><div id="option-chart" class="chart"></div></section>
<section class="panel"><h2>NIFTY close, strike and lifecycle</h2><div id="nifty-chart" class="small-chart"></div></section>
<section class="panel"><h2>Signed inventory</h2><div class="note">Risk-adding and risk-reducing annotations are derived from signed inventory changes.</div><div id="inventory-chart" class="small-chart"></div></section>
<section class="panel"><h2>Episode P&amp;L and cumulative fees</h2><div class="note">MFE/MAE use synchronized completed-minute closes, not intrabar OHLC.</div><div id="pnl-chart" class="chart"></div></section>
</div>
<aside>
<section class="panel"><h2>Episode metrics</h2><div id="episode-metrics"></div></section>
<section class="panel"><h2>Order-pair lifecycle</h2><div class="note">Linkage is observed where present and backfilled from peer bracket IDs. Unknown cancel causes remain unknown.</div><div class="pair-table"><table><thead><tr><th>Pair / order</th><th>Side</th><th>Limit</th><th>States</th><th>Terminal / cause</th></tr></thead><tbody id="pair-body"></tbody></table></div></section>
<section class="panel"><h2>Unavailable by design</h2><p class="note">Historical anchors, multiplier/cooldown bands, delta/velocity state and placement reasons are hidden because the current trace does not observe them. No values are fabricated.</p></section>
</aside>
</div>
<section class="panel meta"><h2>Reproducibility</h2><code>__METADATA__</code><p class="note">Episode accounting reconciles to forced-close report P&amp;L. Observed events come from trace/report artifacts; derived linkage and virtual forced-close sensitivity are labeled explicitly.</p></section>
</main>
<script>
const DATA=__DATA__;
const LC=LightweightCharts;
const money=v=>v==null?'N/A':new Intl.NumberFormat('en-IN',{style:'currency',currency:'INR',maximumFractionDigits:2}).format(Number(v));
const num=v=>v==null?'N/A':new Intl.NumberFormat('en-IN',{maximumFractionDigits:2}).format(Number(v));
const ist=t=>new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',dateStyle:'medium',timeStyle:'short'}).format(new Date(t*1000));
const duration=s=>{const d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);return [d&&d+'d',h&&h+'h',m+'m'].filter(Boolean).join(' ')};
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const base={layout:{background:{type:'solid',color:'#121720'},textColor:'#9aa7b8'},grid:{vertLines:{color:'#1e2631'},horzLines:{color:'#1e2631'}},crosshair:{mode:LC.CrosshairMode.Normal},timeScale:{timeVisible:true,secondsVisible:false,borderColor:'#334155',tickMarkFormatter:ist},rightPriceScale:{borderColor:'#334155'},handleScroll:{mouseWheel:true,pressedMouseMove:true},handleScale:{mouseWheel:true,pinch:true},localization:{timeFormatter:ist}};
function chart(id,height){const el=document.getElementById(id),c=LC.createChart(el,{...base,height});new ResizeObserver(()=>c.applyOptions({width:el.clientWidth})).observe(el);return c}
const optionChart=chart('option-chart',300), optionSeries=optionChart.addLineSeries({color:'#f59e0b',lineWidth:2,title:'Option close'});
const niftyChart=chart('nifty-chart',220), niftySeries=niftyChart.addLineSeries({color:'#4da3ff',lineWidth:2,title:'NIFTY close'});
const inventoryChart=chart('inventory-chart',220), inventorySeries=inventoryChart.addLineSeries({color:'#e879f9',lineWidth:2,lineType:LC.LineType.WithSteps,title:'Signed quantity'});
inventorySeries.createPriceLine({price:0,color:'#64748b',lineWidth:1,lineStyle:LC.LineStyle.Dashed,axisLabelVisible:true,title:'flat'});
const pnlChart=chart('pnl-chart',300), realizedSeries=pnlChart.addLineSeries({color:'#4da3ff',title:'Realized'}),unrealizedSeries=pnlChart.addLineSeries({color:'#f59e0b',title:'Unrealized'}),netSeries=pnlChart.addLineSeries({color:'#22c55e',lineWidth:2,title:'Net'}),feeSeries=pnlChart.addLineSeries({color:'#a78bfa',title:'Fees'});
let strikeLine=null,limitSeries=[],selectedId=null,visible=[],sortKey='start_at',sortDirection=1,syncing=false;
const ordersById=Object.fromEntries(DATA.orders.map(o=>[o.order_id,o]));
const pairsById=Object.fromEntries(DATA.pairs.map(p=>[p.pair_id,p]));
const inputs=['search','outcome','contract','direction','exit','min-inventory','date-from','date-to'];
function syncCharts(charts){charts.forEach(source=>source.timeScale().subscribeVisibleLogicalRangeChange(()=>{if(syncing)return;const range=source.timeScale().getVisibleRange();if(!range)return;syncing=true;charts.filter(c=>c!==source).forEach(c=>c.timeScale().setVisibleRange(range));syncing=false}))}
syncCharts([optionChart,niftyChart,inventoryChart,pnlChart]);
function populateContracts(){const select=document.getElementById('contract');[...new Set(DATA.episodes.map(e=>e.symbol))].sort().forEach(symbol=>{const o=document.createElement('option');o.value=o.textContent=symbol;select.appendChild(o)})}
function filteredEpisodes(){const q=document.getElementById('search').value.toLowerCase(),outcome=document.getElementById('outcome').value,contract=document.getElementById('contract').value,direction=document.getElementById('direction').value,exit=document.getElementById('exit').value,minInv=Number(document.getElementById('min-inventory').value||0),from=document.getElementById('date-from').value,to=document.getElementById('date-to').value;return DATA.episodes.filter(e=>(!q||[e.episode_id,e.symbol,e.exit_reason].join(' ').toLowerCase().includes(q))&&(!outcome||(outcome==='profit'?e.is_profitable:!e.is_profitable))&&(!contract||e.symbol===contract)&&(!direction||e.opening_direction===direction)&&(!exit||(exit==='overnight'?e.is_overnight:exit==='lifecycle'?e.is_lifecycle:e.is_virtual))&&e.max_abs_inventory>=minInv&&(!from||e.start_date>=from)&&(!to||e.start_date<=to)).sort((a,b)=>{const av=a[sortKey],bv=b[sortKey];return ((Number.isFinite(Number(av))&&Number.isFinite(Number(bv)))?Number(av)-Number(bv):String(av??'').localeCompare(String(bv??'')))*sortDirection})}
function renderLedger(){visible=filteredEpisodes();document.getElementById('visible-count').textContent=`${visible.length} of ${DATA.episodes.length} episodes`;document.querySelector('#ledger tbody').innerHTML=visible.map(e=>`<tr class="episode-row ${e.episode_id===selectedId?'selected':''}" data-id="${esc(e.episode_id)}"><td>${esc(e.episode_id)}</td><td>${esc(e.symbol)}</td><td>${esc(e.opening_direction)}</td><td>${esc(ist(e.start_time))}</td><td>${esc(duration(e.duration_seconds))}</td><td class="${Number(e.display_net_pnl)>=0?'positive':'negative'}">${esc(money(e.display_net_pnl))}</td><td>${esc(money(e.historical_fees))}</td><td>${esc(money(e.close_mark_mfe))}</td><td>${esc(money(e.close_mark_mae))}</td><td>${e.max_abs_inventory}</td><td>${e.overnight_count}</td><td>${esc(e.exit_reason)}</td></tr>`).join('');document.querySelectorAll('.episode-row').forEach(row=>row.addEventListener('click',()=>selectEpisode(row.dataset.id)));if(!visible.some(e=>e.episode_id===selectedId)&&visible.length)selectEpisode(visible[0].episode_id);updateNav()}
function clearLimits(){limitSeries.forEach(s=>optionChart.removeSeries(s));limitSeries=[]}
function episodeOrders(e){return e.order_ids.map(id=>ordersById[id]).filter(Boolean)}
function selectEpisode(id,updateHash=true){const e=DATA.episodes.find(x=>x.episode_id===id);if(!e)return;syncing=true;selectedId=id;if(updateHash)history.replaceState(null,'','#'+id);document.querySelectorAll('.episode-row').forEach(r=>r.classList.toggle('selected',r.dataset.id===id));document.getElementById('episode-title').textContent=`${e.episode_id} · ${e.symbol}`;document.getElementById('episode-subtitle').textContent=`${e.opening_direction} · ${ist(e.start_time)} → ${ist(e.end_time)} · ${e.exit_reason}`;optionSeries.setData(DATA.market.option_by_symbol[e.symbol]||[]);niftySeries.setData(DATA.market.underlying);if(strikeLine)niftySeries.removePriceLine(strikeLine);strikeLine=niftySeries.createPriceLine({price:Number(e.contract.strike),color:'#f97316',lineStyle:LC.LineStyle.Dashed,lineWidth:2,axisLabelVisible:true,title:`Strike ${e.contract.strike}`});inventorySeries.setData(e.inventory_curve.map(p=>({time:p.time,value:p.value})));realizedSeries.setData(e.pnl_curve.filter(p=>p.realized!=null).map(p=>({time:p.time,value:p.realized})));unrealizedSeries.setData(e.pnl_curve.filter(p=>p.unrealized!=null).map(p=>({time:p.time,value:p.unrealized})));netSeries.setData(e.pnl_curve.filter(p=>p.net!=null).map(p=>({time:p.time,value:p.net})));feeSeries.setData(e.pnl_curve.filter(p=>p.fees!=null).map(p=>({time:p.time,value:p.fees})));renderLimits(e);renderMarkers(e);renderMetrics(e);renderPairs(e);[optionChart,niftyChart,inventoryChart,pnlChart].forEach(c=>c.timeScale().setVisibleRange({from:e.start_time,to:e.end_time}));syncing=false;updateNav()}
function renderLimits(e){clearLimits();if(!document.getElementById('show-limits').checked)return;episodeOrders(e).forEach((o,index)=>{if(!o.limit_points.length)return;const color=o.side==='BUY'?'rgba(77,163,255,.55)':'rgba(245,158,11,.55)',series=optionChart.addLineSeries({color,lineWidth:1,lineType:LC.LineType.WithSteps,lastValueVisible:false,priceLineVisible:false,title:`${o.side} #${o.order_id}`});series.setData(o.limit_points.map(p=>({time:p.time,value:p.value})));limitSeries.push(series)})}
function renderMarkers(e){const fills=document.getElementById('show-fills').checked?e.fills.map(f=>({time:f.time,position:f.side==='BUY'?'belowBar':'aboveBar',color:f.side==='BUY'?'#4da3ff':'#f59e0b',shape:f.side==='BUY'?'arrowUp':'arrowDown',text:`${f.source==='observed'?'OBS':'DRV'} ${f.side[0]} ${f.quantity}${document.getElementById('show-pairs').checked&&f.pair_id?' · '+f.pair_id:''}`})):[];const showLifecycle=document.getElementById('show-lifecycle').checked;const lifecycle=showLifecycle?DATA.markers.lifecycle.filter(m=>m.symbol===e.symbol&&m.time>=e.start_time&&m.time<=e.end_time).map(m=>({time:m.time,position:'aboveBar',color:'#e879f9',shape:'square',text:`OBS ${m.kind.toUpperCase()}`})):[];const virtual=document.getElementById('show-virtual').checked&&e.virtual_forced_close?[{time:e.virtual_forced_close.time,position:'belowBar',color:'#f0abfc',shape:'circle',text:'VIRTUAL CLOSE'}]:[];optionSeries.setMarkers([...fills,...lifecycle,...virtual].sort((a,b)=>a.time-b.time));inventorySeries.setMarkers(e.inventory_events.filter(p=>p.fill_quantity>0).map(p=>({time:p.time,position:p.risk_effect==='risk_adding'?'aboveBar':'belowBar',color:p.risk_effect==='risk_adding'?'#ef4444':'#22c55e',shape:'circle',text:`DRV ${p.risk_effect==='risk_adding'?'ADD':'REDUCE'}`})));const selection=DATA.markers.selections.filter(m=>m.symbol===e.symbol).map(m=>({time:m.time,position:'aboveBar',color:'#22c55e',shape:'circle',text:`OBS ${String(m.action).toUpperCase()}`}));const life=showLifecycle?DATA.markers.lifecycle.filter(m=>m.symbol===e.symbol).map(m=>({time:m.time,position:'aboveBar',color:'#e879f9',shape:'square',text:`OBS ${String(m.kind).toUpperCase()}`})):[];const missing=document.getElementById('show-missing').checked?DATA.markers.missing.map(m=>({time:m.time,position:'belowBar',color:'#ef4444',shape:'circle',text:'OBS MISSING'})):[];niftySeries.setMarkers([...selection,...life,...missing].sort((a,b)=>a.time-b.time))}
function renderMetrics(e){const v=e.virtual_forced_close,basis=v?'virtual forced close':e.status==='closed_lifecycle_fallback'?'report-derived lifecycle fallback':'observed flat close';document.getElementById('episode-metrics').innerHTML=`<table><tbody><tr><td>Status</td><td>${esc(e.status)}</td></tr><tr><td>Net P&amp;L basis</td><td>${basis}</td></tr><tr><td>Net P&amp;L</td><td class="${Number(e.display_net_pnl)>=0?'positive':'negative'}">${esc(money(e.display_net_pnl))}</td></tr><tr><td>Historical fees</td><td>${esc(money(e.historical_fees))}</td></tr><tr><td>Close-only MFE / MAE</td><td>${esc(money(e.close_mark_mfe))} / ${esc(money(e.close_mark_mae))}</td></tr><tr><td>Max signed exposure</td><td>${e.max_abs_inventory}</td></tr><tr><td>Overnight boundaries</td><td>${e.overnight_count}</td></tr><tr><td>Strike / expiry / DTE at start</td><td>${esc(e.contract.strike)} / ${esc(e.contract.expiry)} / ${e.calendar_dte_at_start??'N/A'}</td></tr><tr><td>Peak capital incl-margin</td><td>${esc(money(e.peak_capital.capital_used_including_margin?.value))}</td></tr><tr><td>Virtual endpoint</td><td>${v?`${esc(v.side)} ${v.quantity} @ ${esc(v.price)} · ${esc(money(v.fee))}`:'None'}</td></tr></tbody></table>`}
function renderPairs(e){const rows=[];episodeOrders(e).forEach(o=>{const pair=o.pair_id?(pairsById[o.pair_id]||{linkage:o.pair_linkage}):null,states=o.transitions.map(t=>t.status).filter(v=>v!=='PLACE_CALL').filter((v,i,a)=>i===0||v!==a[i-1]).join(' → '),limit=o.limit_points.length?o.limit_points[o.limit_points.length-1].price:'N/A',cause=o.cancel_cause?`${o.cancel_cause} (${o.cancel_cause_source})`:o.terminal_status||'open';rows.push(`<tr><td>${esc(o.pair_id||'unpaired')}<br><span class="${pair&&pair.linkage.includes('backfill')?'derived':'observed'}">${esc(pair?.linkage||o.pair_linkage)}</span><br>#${esc(o.order_id)}</td><td>${esc(o.side)}</td><td>${esc(limit)}</td><td>${esc(states)}</td><td>${esc(cause)}</td></tr>`)});e.report_only_order_ids.forEach(id=>rows.push(`<tr><td>report-only<br><span class="derived">derived lifecycle fallback</span><br>#${esc(id)}</td><td>derived</td><td>N/A</td><td>REPORT CLOSE</td><td>${esc(e.exit_reason)}</td></tr>`));document.getElementById('pair-body').innerHTML=rows.join('')||'<tr><td colspan="5">No linked orders</td></tr>'}
function updateNav(){const index=visible.findIndex(e=>e.episode_id===selectedId);document.getElementById('previous').disabled=index<=0;document.getElementById('next').disabled=index<0||index>=visible.length-1}
document.getElementById('previous').addEventListener('click',()=>{const i=visible.findIndex(e=>e.episode_id===selectedId);if(i>0)selectEpisode(visible[i-1].episode_id)});
document.getElementById('next').addEventListener('click',()=>{const i=visible.findIndex(e=>e.episode_id===selectedId);if(i>=0&&i<visible.length-1)selectEpisode(visible[i+1].episode_id)});
inputs.forEach(id=>document.getElementById(id).addEventListener('input',renderLedger));
['show-fills','show-limits','show-pairs','show-lifecycle','show-missing','show-virtual'].forEach(id=>document.getElementById(id).addEventListener('change',()=>{const e=DATA.episodes.find(x=>x.episode_id===selectedId);if(e){if(id==='show-limits')renderLimits(e);renderMarkers(e)}}));
document.querySelectorAll('#ledger th[data-sort]').forEach(th=>th.addEventListener('click',()=>{if(sortKey===th.dataset.sort)sortDirection*=-1;else{sortKey=th.dataset.sort;sortDirection=1}renderLedger()}));
document.getElementById('reset').addEventListener('click',()=>{inputs.forEach(id=>{const e=document.getElementById(id);e.value=id==='min-inventory'?'0':''});renderLedger()});
window.addEventListener('hashchange',()=>selectEpisode(location.hash.slice(1),false));
populateContracts();selectedId=location.hash.slice(1)||DATA.episodes[0]?.episode_id;renderLedger();if(selectedId)selectEpisode(selectedId,!location.hash);
</script></body></html>
"""
