# 把 data/raw/<provider>/latest.json 轉成統一 7 日城市級預報
# 內建專屬 mapper：HKO / JMA / MSS / METNO / SMG / BOM / NOAA
from __future__ import annotations
import json, pathlib, re, xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from providers import PROVIDERS

RAW = pathlib.Path("data/raw")
OUT = pathlib.Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

# ---------- 小工具 ----------
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
    if re.fullmatch(r"\d{8}", s):                  # 20251022
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    m = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return s

def _num(x):
    try:
        return float(x)
    except Exception:
        return None

def _append(out: List[Dict[str, Any]], date, text, tmin=None, tmax=None, src=""):
    if not date and not text:
        return
    out.append({
        "date": _as_iso_date(date),
        "text": _clean_text(text),
        "tmin": tmin if (isinstance(tmin, (int, float)) or tmin is None) else _num(tmin),
        "tmax": tmax if (isinstance(tmax, (int, float)) or tmax is None) else _num(tmax),
        "src": (src or "").upper()
    })

# ---------- 各來源 mapper ----------
# 1) HKO
def _map_hko(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    wf = _safe_get(raw, "data", "weatherForecast", default=[]) or []
    out: List[Dict[str, Any]] = []
    for d in wf:
        _append(
            out,
            d.get("forecastDate"),
            d.get("forecastWeather"),
            _safe_get(d, "forecastMintemp", "value"),
            _safe_get(d, "forecastMaxtemp", "value"),
            "HKO"
        )
    return out

# 2) JMA
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
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 3) MSS（24 小時摘要）
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

# 4) MET Norway
def _map_metno(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = raw.get("data")
    out: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        props = data.get("properties") if isinstance(data.get("properties"), dict) else None
        ts = props.get("timeseries") if isinstance(props, dict) else None
        if isinstance(ts, list) and ts:
            by_date: Dict[str, Dict[str, Any]] = {}
            for p in ts:
                d = _as_iso_date(p.get("time"))
                if not d:
                    continue
                details = _safe_get(p, "data", "instant", "details", default={})
                by_date.setdefault(d, {"tmin": None, "tmax": None, "text": None})
                temp = details.get("air_temperature")
                if isinstance(temp, (int, float)) and by_date[d]["tmax"] is None:
                    by_date[d]["tmax"] = float(temp)
                text = _safe_get(p, "data", "next_1_hours", "summary", "symbol_code") \
                       or _safe_get(p, "data", "next_6_hours", "summary", "symbol_code")
                if text and not by_date[d]["text"]:
                    by_date[d]["text"] = text
            for d in sorted(by_date.keys()):
                x = by_date[d]
                _append(out, d, x["text"], x["tmin"], x["tmax"], "METNO")
        else:
            # 少見：若拿到 XML/文字
            s = raw.get("data") if isinstance(raw.get("data"), str) else ""
            if isinstance(s, str) and s.strip().startswith("<"):
                try:
                    root = ET.fromstring(s)
                    for t in root.findall(".//time"):
                        d = t.get("from") or t.get("to")
                        node = t.find(".//temperature")
                        v = _num(node.get("value")) if node is not None else None
                        _append(out, d, None, None, v, "METNO")
                except ET.ParseError:
                    pass
    return out

# 5) SMG（澳門 7 天 XML）
def _map_smg(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    xml_str = raw.get("data")
    if not isinstance(xml_str, str) or not xml_str.strip().startswith("<"):
        return []
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return []
    out: List[Dict[str, Any]] = []
    for wf in root.findall(".//Custom/WeatherForecast"):
        date = (wf.findtext("ValidFor") or "").strip()
        text = (wf.findtext("WeatherDescription") or "").strip()
        tmin = tmax = None
        for t in wf.findall("Temperature"):
            ttype = (t.findtext("Type") or "").strip()
            val = _num(t.findtext("Value"))
            if ttype == "1":
                tmax = val
            elif ttype == "2":
                tmin = val
        _append(out, date, text, tmin, tmax, "SMG")
    return out

# 6) BOM（保留）
def _map_bom(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    out: List[Dict[str, Any]] = []
    periods = _safe_get(root, "product", "periods", default=[])
    if isinstance(periods, list) and periods:
        for p in periods:
            date = p.get("startTimeLocal") or p.get("startTimeUTC") or p.get("start")
            text = p.get("text") or p.get("detailedForecast") or p.get("summary")
            tmin = p.get("tempMin") or p.get("air_temperature_minimum")
            tmax = p.get("tempMax") or p.get("air_temperature_maximum")
            _append(out, date, text, tmin, tmax, "BOM")
    if not out:
        days = _safe_get(root, "forecasts", "districts", 0, "forecast", "days", default=[])
        if isinstance(days, list) and days:
            for d in days:
                _append(out, d.get("date"), d.get("text"), d.get("temp_min"), d.get("temp_max"), "BOM")
    if not out:
        for key in ("forecasts", "daily", "items", "list", "days"):
            arr = _safe_get(root, key, default=[])
            if isinstance(arr, list) and arr:
                for d in arr:
                    if isinstance(d, dict) and (d.get("date") or d.get("start") or d.get("time")):
                        _append(
                            out,
                            d.get("date") or d.get("start") or d.get("time"),
                            d.get("text") or d.get("detailed") or d.get("summary"),
                            d.get("min") or d.get("tmin") or d.get("temp_min"),
                            d.get("max") or d.get("tmax") or d.get("temp_max"),
                            "BOM"
                        )
                if out:
                    break
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 7) NOAA（NWS）
def _f_to_c(v: Any) -> Optional[float]:
    try:
        return round((float(v) - 32) * 5.0/9.0, 1)
    except Exception:
        return None

def _map_noaa(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    periods = _safe_get(root, "properties", "periods", default=[]) or []
    out: List[Dict[str, Any]] = []
    by_date: Dict[str, Dict[str, Any]] = {}
    for p in periods:
        d = _as_iso_date(p.get("startTime"))
        if not d:
            continue
        by_date.setdefault(d, {"day": None, "night": None})
        if p.get("isDaytime"):
            by_date[d]["day"] = p
        else:
            by_date[d]["night"] = p
    for d in sorted(by_date.keys()):
        cand = by_date[d]["day"] or by_date[d]["night"]
        if not cand:
            continue
        text = cand.get("detailedForecast") or cand.get("shortForecast")
        temp = _num(cand.get("temperature"))
        unit = cand.get("temperatureUnit")
        if temp is not None and unit and unit.upper() == "F":
            temp = _f_to_c(temp)
        _append(out, d, text, None, temp, "NOAA")
    return out

# 8) 通用 mapper（保底）
def _map_generic(raw: Dict[str, Any], src_name: str) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), (list, dict)) else raw
    arr = None
    for key in ("forecasts","daily","items","days","list","data","periods"):
        v = root.get(key) if isinstance(root, dict) else None
        if isinstance(v, list) and v:
            arr = v; break
    if arr is None and isinstance(root, list):
        arr = root
    out: List[Dict[str, Any]] = []
    if not isinstance(arr, list):
        return out
    for d in arr:
        if not isinstance(d, dict):
            continue
        date = d.get("date") or d.get("validDate") or d.get("forecastDate") or d.get("startTime")
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("forecast") or d.get("wx")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d, "temperature", "min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d, "temperature", "max")
        _append(out, date, text, tmin, tmax, src_name)
    return out[:10]

# 入口
def normalize_one(provider: str) -> List[Dict[str, Any]]:
    p = RAW / provider / "latest.json"
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not raw.get("ok"):
        return []
    try:
        result: List[Dict[str, Any]] = []
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
        elif provider == "bom":
            result = _map_bom(raw)
        elif provider == "noaa":
            result = _map_noaa(raw)
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
            x = it.copy(); x["src"] = k.upper()
            flat.append(x)
    (OUT / "normalized_flat.json").write_text(
        json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()
