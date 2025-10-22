# scripts/normalize_all.py
# 將 data/raw/<provider>/latest.json → 統一標準格式
# 已包含：HKO / JMA / MSS / METNO / SMG 的專屬 mapper，
# 其他來源走通用 mapper，不會阻塞流程。
from __future__ import annotations
import json
import pathlib
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

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
    if re.fullmatch(r"\d{8}", s):                  # 20251022
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    m = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)        # 取 ISO datetime 的日期部份
    if m:
        return m.group(1)
    return s

def _append(out: List[Dict[str, Any]], date, text, tmin=None, tmax=None, src=""):
    if not date and not text:
        return
    out.append({
        "date": _as_iso_date(date),
        "text": _clean_text(text),
        "tmin": (float(tmin) if isinstance(tmin, (int, float, str)) and str(tmin).strip() not in ("", "None") else None),
        "tmax": (float(tmax) if isinstance(tmax, (int, float, str)) and str(tmax).strip() not in ("", "None") else None),
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


# ---------- JMA ----------
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
    # 只保留每天第一筆
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())


# ---------- MSS（Singapore / data.gov.sg） ----------
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


# ---------- MET Norway（METNO / api.met.no） ----------
# 時序資料（timeseries）→ 依日期彙總：tmin/tmax 與簡短文字
def _map_metno(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        j = json.loads(raw.get("data")) if isinstance(raw.get("data"), str) else raw.get("data")
    except Exception:
        j = None
    props = _safe_get(j, "properties", default={})
    ts = props.get("timeseries") if isinstance(props, dict) else None
    if not isinstance(ts, list) or not ts:
        return []
    by_day: Dict[str, Dict[str, Any]] = {}
    for p in ts:
        t = p.get("time")
        d = _as_iso_date(t)
        if not d:
            continue
        details = _safe_get(p, "data", "instant", "details", default={}) or {}
        t2m = details.get("air_temperature")
        wx = _safe_get(p, "data", "next_1_hours", "summary", "symbol_code") or \
             _safe_get(p, "data", "next_6_hours", "summary", "symbol_code")
        rec = by_day.setdefault(d, {"tmin": None, "tmax": None, "text": None})
        if isinstance(t2m, (int, float)):
            rec["tmin"] = t2m if rec["tmin"] is None else min(rec["tmin"], t2m)
            rec["tmax"] = t2m if rec["tmax"] is None else max(rec["tmax"], t2m)
        if wx and not rec["text"]:
            rec["text"] = str(wx).replace("_", " ")
    out: List[Dict[str, Any]] = []
    for d in sorted(by_day.keys()):
        rec = by_day[d]
        _append(out, d, rec.get("text"), rec.get("tmin"), rec.get("tmax"), src="METNO")
    return out


# ---------- SMG（澳門氣象 / XML） ----------
# 典型結構示例：
# <forecast>
#   <day>
#     <date>2025-10-22</date>
#     <forecast>多雲</forecast>
#     <minTemp>23</minTemp>
#     <maxTemp>30</maxTemp>
#   </day>
#   ...
# </forecast>
def _map_smg(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    xml_str = raw.get("data")
    if not xml_str or not isinstance(xml_str, str):
        return []
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return []
    out: List[Dict[str, Any]] = []
    for d in root.findall(".//day"):
        date = (d.findtext("date") or "").strip()
        text = (d.findtext("forecast") or "").strip()
        tmin = d.findtext("minTemp")
        tmax = d.findtext("maxTemp")
        try:
            tmin = float(tmin) if tmin not in (None, "") else None
            tmax = float(tmax) if tmax not in (None, "") else None
        except Exception:
            tmin = tmax = None
        _append(out, date, text, tmin, tmax, src="SMG")
    return out


# ---------- BOM（澳洲） / NOAA（NWS） ----------
def _map_bom(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        j = json.loads(raw.get("data")) if isinstance(raw.get("data"), str) else raw.get("data")
    except Exception:
        j = None
    root = j.get("data") if isinstance(j, dict) else j
    out: List[Dict[str, Any]] = []

    periods = _safe_get(root, "product", "periods", default=[])
    if isinstance(periods, list) and periods:
        for p in periods:
            date = p.get("startTimeLocal") or p.get("startTimeUTC") or p.get("start")
            text = p.get("text") or p.get("detailedForecast") or p.get("summary")
            tmin = p.get("tempMin") or p.get("air_temperature_minimum")
            tmax = p.get("tempMax") or p.get("air_temperature_maximum")
            _append(out, date, text, tmin, tmax, src="BOM")

    if not out:
        days = _safe_get(root, "forecasts", "districts", 0, "forecast", "days", default=[])
        if isinstance(days, list) and days:
            for d in days:
                _append(out, d.get("date"), d.get("text"), d.get("temp_min"), d.get("temp_max"), src="BOM")

    if not out and isinstance(root, dict):
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
                            src="BOM"
                        )
                if out:
                    break

    # 去重日期
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())


def _f_to_c(v: Any) -> Optional[float]:
    try:
        return round((float(v) - 32) * 5.0/9.0, 1)
    except Exception:
        return None

def _map_noaa(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        j = json.loads(raw.get("data")) if isinstance(raw.get("data"), str) else raw.get("data")
    except Exception:
        j = None
    periods = _safe_get(j, "properties", "periods", default=[])
    out: List[Dict[str, Any]] = []
    if not isinstance(periods, list) or not periods:
        return out

    by_date: Dict[str, Dict[str, Any]] = {}
    for p in periods:
        date = _as_iso_date(p.get("startTime"))
        if not date:
            continue
        entry = by_date.setdefault(date, {"day": None, "night": None})
        if p.get("isDaytime"):
            entry["day"] = p
        else:
            entry["night"] = p

    for d in sorted(by_date.keys()):
        cand = by_date[d]["day"] or by_date[d]["night"]
        if not cand:
            continue
        text = cand.get("detailedForecast") or cand.get("shortForecast")
        temp = cand.get("temperature")
        unit = cand.get("temperatureUnit")
        t = None
        if temp is not None:
            t = float(temp)
            if unit and unit.upper() == "F":
                t = _f_to_c(t)
        _append(out, d, text, None, t, src="NOAA")
    return out


# ---------- 通用 mapper ----------
def _map_generic(raw: Dict[str, Any], src_name: str) -> List[Dict[str, Any]]:
    # 支援 raw["data"] 可能是 str(JSON) 或已經是 dict/list
    data_obj: Any
    try:
        data_obj = json.loads(raw.get("data")) if isinstance(raw.get("data"), str) else raw.get("data")
    except Exception:
        data_obj = raw.get("data")

    root = data_obj
    if not isinstance(root, (dict, list)):
        return []

    arr = None
    if isinstance(root, dict):
        for key in ("forecasts", "daily", "items", "days", "list", "data", "periods"):
            v = root.get(key)
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
        date = d.get("date") or d.get("validDate") or d.get("forecastDate") or d.get("startTime") or d.get("time")
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("forecast") or d.get("wx")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d, "temperature", "min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d, "temperature", "max")
        _append(out, date, text, tmin, tmax, src_name)
    return out[:10]


# ---------- 入口 ----------
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
        # 解析失敗也回退到通用 mapper
        try:
            return _map_generic(raw, provider)
        except Exception:
            return []


def main():
    all_items: Dict[str, List[Dict[str, Any]]] = {}
    for prov in PROVIDERS:
        arr = normalize_one(prov)
        if arr:
            all_items[prov] = arr[:10]   # 最多保留 10 天

    # 分來源
    (OUT / "normalized.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 扁平清單（debug 用）
    flat: List[Dict[str, Any]] = []
    for k, v in all_items.items():
        for it in v:
            x = it.copy()
            x["src"] = k.upper()
            flat.append(x)
    (OUT / "normalized_flat.json").write_text(
        json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
