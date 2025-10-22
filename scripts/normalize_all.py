# scripts/normalize_all.py
# 把 data/raw/<provider>/latest.json 轉成統一逐日（date/text/tmin/tmax/src）
from __future__ import annotations
import json, pathlib, re
from typing import List, Dict, Any, Optional
from providers import PROVIDERS

RAW = pathlib.Path("data/raw")
OUT = pathlib.Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

# ---------- 工具 ----------
def _safe_get(d: Any, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def _clean_text(s: Optional[str]) -> Optional[str]:
    if not isinstance(s, str):
        return None
    s = s.replace("\u3000", " ").replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def _as_iso_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    m = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    # pubDate 這種格式：Wed, 22 Oct 2025 04:00:00 +0800
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})\s+(20\d{2})", s)
    if m:
        day, mon, yy = m.groups()
        mm = {
            "Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
            "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12",
        }.get(mon, "01")
        return f"{yy}-{mm}-{int(day):02d}"
    return s

def _append(out: List[Dict[str, Any]], date, text, tmin=None, tmax=None, src=""):
    if not date and not text:
        return
    out.append({
        "date": _as_iso_date(date),
        "text": _clean_text(text),
        "tmin": tmin if (isinstance(tmin, (int, float)) or tmin is None) else None,
        "tmax": tmax if (isinstance(tmax, (int, float)) or tmax is None) else None,
        "src": (src or "").upper()
    })

# ---------- HKO ----------
def _map_hko(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    wf = _safe_get(raw, "data", "weatherForecast", default=[])
    out: List[Dict[str, Any]] = []
    for d in wf:
        _append(
            out,
            d.get("forecastDate"),
            d.get("forecastWeather"),
            _safe_get(d, "forecastMintemp", "value"),
            _safe_get(d, "forecastMaxtemp", "value"),
            src="HKO"
        )
    return out

# ---------- JMA（bosai） ----------
def _map_jma(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = raw.get("data") if isinstance(raw.get("data"), list) else raw
    root = arr[0] if isinstance(arr, list) and arr else arr
    ts_list = _safe_get(root, "timeSeries", default=[]) or []
    out: List[Dict[str, Any]] = []
    for ts in ts_list:
        time_def = ts.get("timeDefines")
        areas = ts.get("areas")
        if isinstance(time_def, list) and isinstance(areas, list) and areas:
            weathers = areas[0].get("weathers") or areas[0].get("weatherCodes")
            if isinstance(weathers, list) and weathers:
                for i, t in enumerate(time_def):
                    text = weathers[i] if i < len(weathers) else None
                    _append(out, t, text, src="JMA")
                break
    # 只留每日一筆
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# ---------- MSS（新加坡 24h） ----------
def _map_mss(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    items = root.get("items") if isinstance(root, dict) else None
    out: List[Dict[str, Any]] = []
    if isinstance(items, list) and items:
        it = items[0]
        date = _safe_get(it, "valid_period", "start") or _safe_get(it, "timestamp")
        text = _safe_get(it, "general", "forecast") or _safe_get(it, "general", "summary")
        _append(out, date, text, src="MSS")
    return out

# ---------- MET Norway（locationforecast） ----------
_SYMBOL_MAP = {
    "clearsky": "Clear",
    "cloudy": "Cloudy",
    "fair": "Fair",
    "fog": "Fog",
    "heavyrain": "Heavy rain",
    "heavyrainshowers": "Heavy rain showers",
    "lightrain": "Light rain",
    "lightrainshowers": "Light rain showers",
    "partlycloudy": "Sunny intervals",
    "rain": "Rain",
    "rainshowers": "Rain showers",
    "thunderstorm": "Thunderstorm",
}
def _map_metno(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    series = _safe_get(root, "properties", "timeseries", default=[])
    out: List[Dict[str, Any]] = []
    if not isinstance(series, list) or not series:
        return out
    by_day: Dict[str, Dict[str, Any]] = {}
    for t in series:
        ts = t.get("time"); 
        if not ts: 
            continue
        date = _as_iso_date(ts) or str(ts)[:10]
        det = _safe_get(t, "data", "instant", "details", default={}) or {}
        temp = det.get("air_temperature")
        sym = _safe_get(t, "data", "next_6_hours", "summary", "symbol_code") or \
              _safe_get(t, "data", "next_12_hours", "summary", "symbol_code")
        if date not in by_day:
            by_day[date] = {"tmin": None, "tmax": None, "symbols": []}
        if isinstance(temp, (int, float)):
            if by_day[date]["tmin"] is None or temp < by_day[date]["tmin"]:
                by_day[date]["tmin"] = temp
            if by_day[date]["tmax"] is None or temp > by_day[date]["tmax"]:
                by_day[date]["tmax"] = temp
        if isinstance(sym, str):
            by_day[date]["symbols"].append(sym.split("_")[0])
    from collections import Counter
    for d in sorted(by_day.keys())[:10]:
        rec = by_day[d]
        text = None
        if rec["symbols"]:
            sym = Counter(rec["symbols"]).most_common(1)[0][0]
            text = _SYMBOL_MAP.get(sym, sym.replace("_", " ").title())
        _append(out, d, text, rec["tmin"], rec["tmax"], src="METNO")
    return out

# ---------- SMG（澳門 7 日預報 RSS / 文字） ----------
def _map_smg(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = raw.get("data")
    out: List[Dict[str, Any]] = []
    if data is None:
        return out

    # 若是 JSON 物件（少見）
    if isinstance(data, dict) and ("items" in data or "list" in data):
        arr = data.get("items") or data.get("list") or []
        for it in arr[:10]:
            date = it.get("date") or it.get("pubDate") or it.get("time") or it.get("updated")
            text = it.get("summary") or it.get("description") or it.get("title")
            _append(out, date, text, src="SMG")
        if out:
            return out

    # 若是字串（RSS/XML/HTML）
    if isinstance(data, str):
        txt = data

        # 1) RSS：抓 <item> 集合
        items = re.findall(r"<item>(.*?)</item>", txt, flags=re.DOTALL | re.IGNORECASE)
        if items:
            for chunk in items[:10]:
                title = _clean_text("".join(re.findall(r"<title>(.*?)</title>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                desc  = _clean_text("".join(re.findall(r"<description>(.*?)</description>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                pubd  = _clean_text("".join(re.findall(r"<pubDate>(.*?)</pubDate>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                date = _as_iso_date(pubd) or _as_iso_date(title) or _as_iso_date(desc)
                text = desc or title
                if text:
                    _append(out, date, text, src="SMG")
            if out:
                return out

        # 2) 簡單 HTML 萃取（保底）
        plain = re.sub(r"<[^>]+>", " ", txt)
        lines = [l.strip() for l in plain.splitlines() if l.strip()]
        for i, l in enumerate(lines):
            if re.search(r"(20\d{2})[-/\.](\d{1,2})[-/\.](\d{1,2})", l):
                date = _as_iso_date(l)
                desc = lines[i+1] if i+1 < len(lines) else None
                _append(out, date, desc, src="SMG")
                if len(out) >= 7:
                    break

    return out

# ---------- NOAA / BOM（保留原樣或日後擴充；此處略） ----------

# ---------- 通用 mapper ----------
def _map_generic(raw: Dict[str, Any], src_name: str) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), (list, dict)) else raw
    arr = None
    for key in ("forecasts","daily","items","days","list","data","periods"):
        v = root.get(key) if isinstance(root, dict) else None
        if isinstance(v, list) and v:
            arr = v
            break
    if arr is None and isinstance(root, list):
        arr = root
    out: List[Dict[str, Any]] = []
    if not isinstance(arr, list):
        return out
    for d in arr:
        if not isinstance(d, dict): 
            continue
        date = d.get("date") or d.get("validDate") or d.get("forecastDate") or d.get("startTime")
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("forecast") or d.get("wx") or d.get("overview")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d,"temperature","min") or d.get("temp_min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d,"temperature","max") or d.get("temp_max")
        _append(out, date, text, tmin, tmax, src_name.upper())
    return out[:10]

# ---------- 分派 ----------
def normalize_one(provider: str) -> List[Dict[str, Any]]:
    p = RAW / provider / "latest.json"
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not raw.get("ok"):
        return []
    try:
        if provider == "hko":
            result = _map_hko(raw)
        elif provider == "jma":
            result = _map_jma(raw)
        elif provider == "mss":
            result = _map_mss(raw)
        elif provider == "metno":
            result = _map_metno(raw)
        elif provider == "smg":
            result = _map_smg(raw)
        else:
            result = _map_generic(raw, provider)

        if not result:
            result = _map_generic(raw, provider)
        return result
    except Exception:
        try:
            return _map_generic(raw, provider)
        except Exception:
            return []

def main():
    all_items: Dict[str, List[Dict[str, Any]]] = {}
    for prov in PROVIDERS:
        arr = normalize_one(prov)
        if arr:
            all_items[prov] = arr[:10]

    (OUT / "normalized.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    flat = []
    for k, v in all_items.items():
        for it in v:
            it2 = it.copy(); it2["src"] = k.upper()
            flat.append(it2)
    (OUT / "normalized_flat.json").write_text(
        json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()
