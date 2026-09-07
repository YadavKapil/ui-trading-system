"""Minimal offline candlestick and observed Wave order-line dashboard."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import html
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from .continuous import ReplayDatasetCollection
from .normalize import canonical_json, sha256_file
from .schemas import DatasetManifest, SchemaError
from .trade_episodes import load_trade_episode_model


DEFAULT_EPISODE_ID = "episode-2b11ea0b7d-001"
IST = ZoneInfo("Asia/Kolkata")


def generate_order_chart(
    *,
    report_path: str | Path,
    trace_path: str | Path,
    collection_manifest_path: str | Path,
    output_path: str | Path,
    chart_asset_path: str | Path,
    simulator_commit: str,
    title: str = "Wave Option Orders",
    default_episode_id: str = DEFAULT_EPISODE_ID,
) -> dict[str, Any]:
    report, model = load_trade_episode_model(report_path, trace_path)
    ohlc = load_pinned_episode_ohlc(
        report,
        model,
        collection_manifest_path,
    )
    payload = build_order_chart_payload(
        report,
        model,
        ohlc,
        default_episode_id=default_episode_id,
    )
    chart_source = Path(chart_asset_path).read_text(encoding="utf-8")
    rendered = _render_order_chart(
        title=title,
        payload=payload,
        chart_source=chart_source,
        simulator_commit=simulator_commit,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    default = next(
        item
        for item in payload["episodes"]
        if item["episode_id"] == payload["default_episode_id"]
    )
    return {
        "output": str(output.resolve()),
        "size_bytes": output.stat().st_size,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "episodes": len(payload["episodes"]),
        "default_episode_id": payload["default_episode_id"],
        "default_series_kind": default["series_kind"],
        "default_candles": len(default["candles"]),
        "default_order_lines": len(default["order_lines"]),
        "default_fills": len(default["fills"]),
    }


def load_pinned_episode_ohlc(
    report: dict[str, Any],
    model: dict[str, Any],
    collection_manifest_path: str | Path,
) -> dict[str, Any]:
    collection = ReplayDatasetCollection.from_inputs([collection_manifest_path])
    manifest = collection.to_manifest()
    reported = report["collection"]
    for key in ("collection_id", "collection_sha256"):
        if manifest.get(key) != reported.get(key):
            raise SchemaError(
                f"Order chart collection {key} mismatch: "
                f"actual={manifest.get(key)} report={reported.get(key)}"
            )
    order_by_id = {order["order_id"]: order for order in model["orders"]}
    bounds = {
        episode["episode_id"]: _episode_display_bounds(episode, order_by_id)
        for episode in model["episodes"]
    }
    instrument_to_episodes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for episode in model["episodes"]:
        instrument_id = episode["contract"].get("instrument_id")
        if instrument_id:
            instrument_to_episodes[str(instrument_id)].append(episode)
    candles: dict[str, list[dict[str, Any]]] = {
        episode["episode_id"]: [] for episode in model["episodes"]
    }
    pins: dict[str, list[dict[str, Any]]] = defaultdict(list)
    start = dt.date.fromisoformat(report["requested_range"]["start"])
    end = dt.date.fromisoformat(report["requested_range"]["end"])
    for partition in collection.partitions:
        if partition.session_date < start or partition.session_date > end:
            continue
        needed = {
            instrument_id
            for instrument_id, episodes in instrument_to_episodes.items()
            if any(
                lower.date() <= partition.session_date <= upper.date()
                for episode in episodes
                for lower, upper in [bounds[episode["episode_id"]]]
            )
        }
        if not needed:
            continue
        option_path = partition.path / "option_bars.parquet"
        dataset_manifest = DatasetManifest.load(partition.path)
        file_record = dataset_manifest.files.get("option_bars.parquet")
        if file_record is None:
            raise SchemaError(
                f"Partition {partition.session_date} has no option_bars pin"
            )
        if (
            option_path.stat().st_size != int(file_record["size_bytes"])
            or sha256_file(option_path) != str(file_record["sha256"])
        ):
            raise SchemaError(
                f"Pinned option_bars changed for {partition.session_date}"
            )
        table = pq.read_table(
            option_path,
            columns=[
                "instrument_id",
                "bar_end_utc",
                "bar_end_ist",
                "resolution_seconds",
                "open",
                "high",
                "low",
                "close",
                "missing_bar",
                "source_archive_sha256",
                "source_member_sha256",
                "source_row",
            ],
            filters=[("instrument_id", "in", sorted(needed))],
        )
        for row in table.to_pylist():
            if bool(row["missing_bar"]) or int(row["resolution_seconds"]) != 60:
                continue
            utc = _aware(row["bar_end_utc"])
            ist = _aware(row["bar_end_ist"])
            if utc.astimezone(IST) != ist.astimezone(IST):
                raise SchemaError(
                    f"UTC/IST bar timestamp mismatch for {row['instrument_id']}"
                )
            candle = {
                "time": int(utc.timestamp()),
                "at": ist.isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "source_archive_sha256": str(row["source_archive_sha256"]),
                "source_member_sha256": str(row["source_member_sha256"]),
                "source_row": int(row["source_row"]),
            }
            for episode in instrument_to_episodes[str(row["instrument_id"])]:
                lower, upper = bounds[episode["episode_id"]]
                if lower <= ist <= upper:
                    candles[episode["episode_id"]].append(copy.deepcopy(candle))
                    pin = {
                        "session_date": partition.session_date.isoformat(),
                        "dataset_id": partition.dataset_id,
                        "manifest_sha256": partition.manifest_sha256,
                        "option_bars_sha256": str(file_record["sha256"]),
                    }
                    if pin not in pins[episode["episode_id"]]:
                        pins[episode["episode_id"]].append(pin)
    for values in candles.values():
        values.sort(key=lambda item: item["time"])
        if len({item["time"] for item in values}) != len(values):
            raise SchemaError("Duplicate candle timestamps in pinned option data")
    return {
        "collection": {
            "collection_id": manifest["collection_id"],
            "collection_sha256": manifest["collection_sha256"],
        },
        "candles": candles,
        "pins": {key: value for key, value in sorted(pins.items())},
    }


def build_order_chart_payload(
    report: dict[str, Any],
    model: dict[str, Any],
    ohlc: dict[str, Any],
    *,
    default_episode_id: str = DEFAULT_EPISODE_ID,
) -> dict[str, Any]:
    orders = {order["order_id"]: order for order in model["orders"]}
    episodes = []
    for episode in model["episodes"]:
        lower, upper = _episode_display_bounds(episode, orders)
        candles = list(ohlc["candles"].get(episode["episode_id"], []))
        trace_closes = [
            {
                "time": _epoch(point["at"]),
                "value": float(point["close"]),
                "at": point["at"],
            }
            for point in model["market"]["option_by_symbol"].get(
                episode["symbol"],
                [],
            )
            if lower <= _aware(point["at"]).astimezone(IST) <= upper
        ]
        series_kind = "candlestick" if candles else "line"
        order_lines = [
            build_order_step_segment(orders[order_id])
            for order_id in episode["order_ids"]
            if order_id in orders
            and _order_intersects(orders[order_id], lower, upper)
        ]
        fills = [
            {
                **fill,
                "time": _epoch(fill["at"]),
                "provenance": (
                    "observed"
                    if fill["source"] == "observed"
                    else "derived"
                ),
            }
            for fill in episode["fills"]
        ]
        terminals = [
            marker
            for segment in order_lines
            for marker in segment["terminal_markers"]
        ]
        virtual = copy.deepcopy(episode["virtual_forced_close"])
        if virtual:
            virtual["time"] = _epoch(virtual["at"])
        right = _option_right(episode["contract"])
        episodes.append(
            {
                "episode_id": episode["episode_id"],
                "ordinal": episode["ordinal"],
                "symbol": episode["symbol"],
                "contract": episode["contract"],
                "right": right,
                "status": episode["status"],
                "start_at": episode["start_at"],
                "end_at": episode["end_at"],
                "display_start_at": lower.isoformat(),
                "display_end_at": upper.isoformat(),
                "display_start_time": int(lower.timestamp()),
                "display_end_time": int(upper.timestamp()),
                "net_pnl": episode["display_net_pnl"],
                "selector_label": (
                    f"{episode['symbol']} | #{episode['ordinal']} | "
                    f"{episode['start_at'][:10]} | "
                    f"P&L {episode['display_net_pnl']}"
                ),
                "series_kind": series_kind,
                "series_label": (
                    "Pinned 1-minute OHLC candles"
                    if candles
                    else "Trace close-only line; OHLC unavailable"
                ),
                "candles": candles,
                "line": trace_closes,
                "order_lines": order_lines,
                "fills": fills,
                "terminal_markers": terminals,
                "virtual_close": virtual,
                "pins": ohlc["pins"].get(episode["episode_id"], []),
            }
        )
    available = {episode["episode_id"] for episode in episodes}
    resolved_default = (
        default_episode_id
        if default_episode_id in available
        else episodes[0]["episode_id"]
    )
    return {
        "schema": "wave-replay-order-chart-v1",
        "default_episode_id": resolved_default,
        "episodes": episodes,
        "provenance": {
            "collection_id": ohlc["collection"]["collection_id"],
            "collection_sha256": ohlc["collection"]["collection_sha256"],
            "trace_digest": report["trace_digest"],
            "report_hash": report["report_hash"],
        },
    }


def build_order_step_segment(order: dict[str, Any]) -> dict[str, Any]:
    points = [
        {
            "time": _epoch(point["at"]),
            "value": float(point["price"]),
            "at": point["at"],
            "status": point["status"],
        }
        for point in order["limit_points"]
    ]
    points = _last_by_time(points)
    terminals = []
    for transition in order["transitions"]:
        if transition["status"] not in {"CANCELLED", "REJECTED"}:
            continue
        terminals.append(
            {
                "time": _epoch(transition["at"]),
                "at": transition["at"],
                "order_id": order["order_id"],
                "side": order["side"],
                "status": transition["status"],
                "reason": (
                    transition.get("rejection_reason")
                    or order.get("cancel_cause")
                    or "unknown"
                ),
                "reason_source": (
                    "observed"
                    if transition["status"] == "REJECTED"
                    and transition.get("rejection_reason")
                    else order.get("cancel_cause_source") or "unavailable"
                ),
            }
        )
    return {
        "order_id": order["order_id"],
        "pair_id": order["pair_id"],
        "pair_linkage": order["pair_linkage"],
        "side": order["side"],
        "symbol": order["symbol"],
        "tag": order["tag"],
        "points": points,
        "starts_at": None if not points else points[0]["at"],
        "ends_at": None if not points else points[-1]["at"],
        "terminal_status": order["terminal_status"],
        "terminal_at": order["terminal_at"],
        "terminal_markers": terminals,
        "source": "observed_order_transitions",
    }


def _episode_display_bounds(
    episode: dict[str, Any],
    orders: dict[str, dict[str, Any]],
) -> tuple[dt.datetime, dt.datetime]:
    timestamps = [_aware(episode["start_at"]).astimezone(IST)]
    terminal_candidates = [_aware(episode["end_at"]).astimezone(IST)]
    for order_id in episode["order_ids"]:
        order = orders.get(order_id)
        if order:
            transitions = [
                _aware(item["at"]).astimezone(IST)
                for item in order["transitions"]
            ]
            timestamps.extend(transitions)
            terminal_candidates.extend(transitions)
    lower = min(timestamps)
    upper = max(terminal_candidates)
    return lower, upper


def _order_intersects(
    order: dict[str, Any],
    lower: dt.datetime,
    upper: dt.datetime,
) -> bool:
    timestamps = [
        _aware(item["at"]).astimezone(IST)
        for item in order["transitions"]
    ]
    return bool(timestamps) and min(timestamps) <= upper and max(timestamps) >= lower


def _option_right(contract: dict[str, Any]) -> str:
    symbol = str(contract.get("wave_trading_symbol", "")).upper()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    instrument = str(contract.get("instrument_id", ""))
    return "CE" if "|C|" in instrument else "PE" if "|P|" in instrument else "UNKNOWN"


def _last_by_time(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_time = {int(point["time"]): point for point in points}
    return [by_time[key] for key in sorted(by_time)]


def _epoch(value: str | dt.datetime) -> int:
    return int(_aware(value).timestamp())


def _aware(value: str | dt.datetime) -> dt.datetime:
    parsed = (
        value
        if isinstance(value, dt.datetime)
        else dt.datetime.fromisoformat(str(value))
    )
    if parsed.tzinfo is None:
        raise SchemaError(f"Order chart timestamp is timezone-naive: {value}")
    return parsed


def _render_order_chart(
    *,
    title: str,
    payload: dict[str, Any],
    chart_source: str,
    simulator_commit: str,
) -> str:
    provenance = {
        **payload["provenance"],
        "simulator_commit": simulator_commit,
    }
    return (
        _TEMPLATE.replace("__TITLE__", html.escape(title))
        .replace("__CHART_SOURCE__", chart_source.replace("</script>", "<\\/script>"))
        .replace("__DATA__", canonical_json(payload).replace("</", "<\\/"))
        .replace("__PROVENANCE__", html.escape(canonical_json(provenance)))
    )


_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<style>
:root{--bg:#0b0e13;--panel:#121720;--line:#2a3340;--text:#e7edf5;--muted:#8b98a8;--green:#22c55e;--red:#ef4444;--blue:#4da3ff;--purple:#e879f9}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:13px/1.45 Inter,Segoe UI,Arial,sans-serif}.wrap{max-width:1450px;margin:auto;padding:16px}h1{font-size:21px;margin:0}.sub,.muted{color:var(--muted)}.toolbar{display:flex;gap:8px;align-items:end;flex-wrap:wrap;margin:12px 0}.toolbar label{color:var(--muted);font-size:11px}.toolbar select,.toolbar button{background:#171d27;color:var(--text);border:1px solid var(--line);border-radius:5px;padding:7px}.toolbar select{min-width:520px}.toolbar button{cursor:pointer}.panel{position:relative;background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:12px;box-shadow:0 5px 18px rgba(0,0,0,.22)}.contract{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;margin-bottom:8px}.symbol{font-size:18px;font-weight:700}.badge{border:1px solid var(--line);border-radius:999px;padding:3px 8px;color:#fbbf24}.chart{height:650px}.legend{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin:6px 0}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:4px}.tooltip{display:none;position:absolute;z-index:10;pointer-events:none;background:rgba(10,14,20,.95);border:1px solid #475569;border-radius:6px;padding:8px;min-width:260px;box-shadow:0 4px 18px rgba(0,0,0,.4)}details{margin-top:10px;color:var(--muted)}code{word-break:break-all}.fallback{color:#fbbf24;font-weight:700}@media(max-width:700px){.wrap{padding:8px}.toolbar{align-items:stretch}.toolbar label,.toolbar select,.toolbar button{width:100%;min-width:0}.chart{height:480px}.symbol{font-size:16px}}
</style>
<script>__CHART_SOURCE__</script>
</head>
<body><main class="wrap">
<h1>__TITLE__</h1>
<div class="sub">Observed Wave option orders over pinned one-minute option data · IST · offline</div>
<div class="toolbar">
<label>Contract / episode<br><select id="episode"></select></label>
<button id="previous">← Previous</button><button id="next">Next →</button>
<button id="fit">Fit to episode</button>
<label><input id="orders" type="checkbox" checked> order lines</label>
<label><input id="fills" type="checkbox" checked> fills / terminal markers</label>
</div>
<section class="panel">
<div class="contract"><div><div id="symbol" class="symbol"></div><div id="contract-meta" class="muted"></div></div><div><span id="status" class="badge"></span></div></div>
<div id="source-label" class="legend"></div>
<div class="legend"><span><i class="dot" style="background:var(--green)"></i>BUY limit</span><span><i class="dot" style="background:var(--red)"></i>SELL limit</span><span><i class="dot" style="background:var(--blue)"></i>BUY fill</span><span><i class="dot" style="background:#f59e0b"></i>SELL fill</span><span><i class="dot" style="background:var(--purple)"></i>virtual</span></div>
<div id="chart" class="chart"></div><div id="tooltip" class="tooltip"></div>
</section>
<details><summary>Provenance</summary><code>__PROVENANCE__</code><p>Pinned collection parquet is hash-validated. Report and trace hashes are verified by the shared episode normalizer. Order lines are observed transitions; pair linkage may be derived and is shown in tooltips. Virtual close is not a historical fill.</p></details>
</main>
<script>
const DATA=__DATA__,LC=LightweightCharts,IST_OFFSET_SECONDS=19800,IST_MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const esc=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const istDate=time=>typeof time==='number'?new Date((time+IST_OFFSET_SECONDS)*1000):null,istPad=v=>v<10?'0'+v:String(v);
function istTickMark(time,type){const d=istDate(time);if(!d)return'';if(type===0)return String(d.getUTCFullYear());if(type===1)return IST_MONTHS[d.getUTCMonth()];if(type===2)return String(d.getUTCDate());return istPad(d.getUTCHours())+':'+istPad(d.getUTCMinutes())}
function istTime(time){const d=istDate(time);return d?d.getUTCFullYear()+'-'+istPad(d.getUTCMonth()+1)+'-'+istPad(d.getUTCDate())+' '+istPad(d.getUTCHours())+':'+istPad(d.getUTCMinutes())+' IST':''}
const chart=LC.createChart(document.getElementById('chart'),{autoSize:true,layout:{background:{type:'solid',color:'#121720'},textColor:'#c9d1d9'},grid:{vertLines:{color:'#21262d'},horzLines:{color:'#21262d'}},crosshair:{mode:LC.CrosshairMode.Normal},timeScale:{timeVisible:true,secondsVisible:false,borderColor:'#30363d',tickMarkFormatter:istTickMark},localization:{timeFormatter:istTime},rightPriceScale:{borderColor:'#30363d'},handleScroll:{mouseWheel:true,pressedMouseMove:true},handleScale:{mouseWheel:true,pinch:true}});
let priceSeries=null,orderSeries=[],selected=null;
const selector=document.getElementById('episode'),tooltip=document.getElementById('tooltip');
DATA.episodes.forEach(e=>{const o=document.createElement('option');o.value=e.episode_id;o.textContent=e.selector_label;selector.appendChild(o)});
function clearSeries(){if(priceSeries)chart.removeSeries(priceSeries);orderSeries.forEach(s=>chart.removeSeries(s));orderSeries=[]}
function orderLines(e){return e.order_lines}
function activeLimits(e,time){const rows=[];orderLines(e).forEach(o=>{if(!o.points.length||time<o.points[0].time||time>o.points[o.points.length-1].time)return;let point=null;o.points.forEach(p=>{if(p.time<=time)point=p});if(point)rows.push(`${o.side} #${o.order_id} @ ${point.value}${o.pair_id?' · '+o.pair_id+' ('+o.pair_linkage+')':''}`)});return rows}
function markers(e){if(!document.getElementById('fills').checked)return[];const fills=e.fills.map(f=>({time:f.time,position:f.side==='BUY'?'belowBar':'aboveBar',color:f.side==='BUY'?'#4da3ff':'#f59e0b',shape:f.side==='BUY'?'arrowUp':'arrowDown',text:`${f.provenance==='observed'?'OBS':'DRV'} ${f.side[0]} ${f.quantity} #${f.order_id}`}));const terminal=e.terminal_markers.map(m=>({time:m.time,position:'aboveBar',color:m.status==='REJECTED'?'#ef4444':'#94a3b8',shape:'circle',text:`${m.status} #${m.order_id} · ${m.reason}`}));const virtual=e.virtual_close?[{time:e.virtual_close.time,position:'belowBar',color:'#e879f9',shape:'square',text:`VIRTUAL ${e.virtual_close.side} ${e.virtual_close.quantity}`}]:[];return [...fills,...terminal,...virtual].sort((a,b)=>a.time-b.time)}
function renderOrders(e){orderSeries.forEach(s=>chart.removeSeries(s));orderSeries=[];if(!document.getElementById('orders').checked)return;e.order_lines.forEach((o,index)=>{if(!o.points.length)return;const color=o.side==='BUY'?'#22c55e':'#ef4444',series=chart.addLineSeries({color,lineWidth:2,lineType:LC.LineType.WithSteps,lastValueVisible:false,priceLineVisible:false,title:`${o.side} #${o.order_id}`});series.setData(o.points.map(p=>({time:p.time,value:p.value})));orderSeries.push(series)})}
function fit(){if(selected)chart.timeScale().setVisibleRange({from:selected.display_start_time,to:selected.display_end_time})}
function render(id){selected=DATA.episodes.find(e=>e.episode_id===id)||DATA.episodes[0];selector.value=selected.episode_id;history.replaceState(null,'','#'+selected.episode_id);clearSeries();if(selected.series_kind==='candlestick'){priceSeries=chart.addCandlestickSeries({upColor:'#26a69a',downColor:'#ef5350',borderVisible:false,wickUpColor:'#26a69a',wickDownColor:'#ef5350'});priceSeries.setData(selected.candles)}else{priceSeries=chart.addLineSeries({color:'#f59e0b',lineWidth:2,title:'Option close'});priceSeries.setData(selected.line)}priceSeries.setMarkers(markers(selected));renderOrders(selected);document.getElementById('symbol').textContent=selected.symbol;document.getElementById('contract-meta').textContent=`Expiry ${selected.contract.expiry} · Strike ${selected.contract.strike} · ${selected.right} · ${selected.start_at} → ${selected.end_at}`;document.getElementById('status').textContent=selected.status;document.getElementById('source-label').innerHTML=`<span class="${selected.series_kind==='line'?'fallback':''}">${selected.series_label}</span> · ${selected.candles.length} candles · ${selected.order_lines.length} observed order lines · ${selected.fills.length} fills`;fit();updateNav()}
function updateNav(){const i=DATA.episodes.findIndex(e=>e.episode_id===selected.episode_id);document.getElementById('previous').disabled=i<=0;document.getElementById('next').disabled=i>=DATA.episodes.length-1}
selector.addEventListener('change',()=>render(selector.value));document.getElementById('fit').addEventListener('click',fit);document.getElementById('previous').addEventListener('click',()=>{const i=DATA.episodes.findIndex(e=>e.episode_id===selected.episode_id);if(i>0)render(DATA.episodes[i-1].episode_id)});document.getElementById('next').addEventListener('click',()=>{const i=DATA.episodes.findIndex(e=>e.episode_id===selected.episode_id);if(i<DATA.episodes.length-1)render(DATA.episodes[i+1].episode_id)});document.getElementById('orders').addEventListener('change',()=>renderOrders(selected));document.getElementById('fills').addEventListener('change',()=>priceSeries.setMarkers(markers(selected)));
chart.subscribeCrosshairMove(param=>{if(!param.time||!selected){tooltip.style.display='none';return}const point=param.seriesData.get(priceSeries),limits=activeLimits(selected,param.time),fills=selected.fills.filter(f=>f.time===param.time);let price='';if(point&&selected.series_kind==='candlestick')price=`O ${point.open} · H ${point.high} · L ${point.low} · C ${point.close}`;else if(point)price=`Close ${point.value}`;tooltip.innerHTML=`<strong>${esc(istTime(param.time))}</strong><br>${esc(price)}<br>${limits.map(esc).join('<br>')}${fills.length?'<br>'+fills.map(f=>esc(`${f.side} fill ${f.quantity} @ ${f.price} #${f.order_id}`)).join('<br>'):''}`;tooltip.style.display='block';tooltip.style.left=Math.min(param.point?.x+18||10,document.getElementById('chart').clientWidth-290)+'px';tooltip.style.top=Math.max(50,(param.point?.y||60)-20)+'px'});
const requested=location.hash.slice(1);render(DATA.episodes.some(e=>e.episode_id===requested)?requested:DATA.default_episode_id);
</script></body></html>
"""
