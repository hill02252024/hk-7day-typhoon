# scripts/normalize_all.py
# 目的：把 data/raw/<provider>/latest.json 轉成統一的 7~10 日城市級預報格式
# 已內建專屬 mapper：HKO / JMA / MSS / METNO / SMG / (BOM / NOAA 可部分支援)
# 其他來源走通用 mapper。

from __future__ import annotations
import json, pathlib, re, math
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
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
    # YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # YYYY-MM-DD / YYYY/MM/DD
    m = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # ISO datetime 取日期
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
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

def _round1(x: Optional[float]) -> Optional[float]:
    try:
        return round(float(x), 1)
    except Exception:
        return None

# ---------- 各來源 mapper ----------

# 1) HKO – 香港天文台 9-day JSON
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

# 2) JMA – bosai/forecast JSON（取第一個地區的 weathers）
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
    # 同日去重
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 3) MSS – 新加坡 24h JSON（items[0].general）
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

# 4) MET Norway – locationforecast/compact 或類似 JSON
# 結構常見：{"properties":{"timeseries":[{"time":"...","data":{"instant":{"details":...},"next_12_hours":{"summary":{"symbol_code":"..."}}}}]}}
# 我們把 timeseries 依日期分組，計算 tmin/tmax（氣溫），摘要文字用 symbol_code（first of the day）。
def _map_metno(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    ts = _safe_get(root, "properties", "timeseries", default=[])
    if not isinstance(ts, list) or not ts:
        return []
    day_bucket: Dict[str, Dict[str, Any]] = {}
    for p in ts:
        d = _as_iso_date(p.get("time"))
        if not d:
            continue
        tmp = _safe_get(p, "data", "instant", "details", "air_temperature")
        sym = _safe_get(p, "data", "next_12_hours", "summary", "symbol_code") or \
              _safe_get(p, "data", "next_6_hours", "summary", "symbol_code") or \
              _safe_get(p, "data", "next_1_hours", "summary", "symbol_code")
        if d not in day_bucket:
            day_bucket[d] = {"tmin": None, "tmax": None, "text": None}
        if isinstance(tmp, (int, float)):
            if day_bucket[d]["tmin"] is None or tmp < day_bucket[d]["tmin"]:
                day_bucket[d]["tmin"] = float(tmp)
            if day_bucket[d]["tmax"] is None or tmp > day_bucket[d]["tmax"]:
                day_bucket[d]["tmax"] = float(tmp)
        if not day_bucket[d]["text"] and isinstance(sym, str):
            day_bucket[d]["text"] = sym.replace("_", " ")

    out: List[Dict[str, Any]] = []
    for d in sorted(day_bucket.keys()):
        b = day_bucket[d]
        _append(out, d, b.get("text"), _round1(b.get("tmin")), _round1(b.get("tmax")), src="METNO")
    return out

# 5) SMG（澳門）– RSS（feedparser 優先、regex 保底）
def _map_smg(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = raw.get("data")
    out: List[Dict[str, Any]] = []
    if data is None:
        return out

    if isinstance(data, str):
        # feedparser 優先
        try:
            import feedparser
            feed = feedparser.parse(data)
            for e in (feed.entries or [])[:10]:
                title = _clean_text(getattr(e, "title", None))
                desc  = _clean_text(getattr(e, "summary", None) or getattr(e, "description", None))
                pubd  = _clean_text(getattr(e, "published", None) or getattr(e, "updated", None))
                date  = _as_iso_date(pubd) or _as_iso_date(title) or _as_iso_date(desc)
                text  = desc or title
                if text:
                    _append(out, date, text, src="SMG")
            if out:
                return out
        except Exception:
            pass

        # regex 保底
        items = re.findall(r"<item>(.*?)</item>", data, flags=re.DOTALL | re.IGNORECASE)
        if items:
            for chunk in items[:10]:
                title = _clean_text("".join(re.findall(r"<title>(.*?)</title>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                desc  = _clean_text("".join(re.findall(r"<description>(.*?)</description>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                pubd  = _clean_text("".join(re.findall(r"<pubDate>(.*?)</pubDate>", chunk, flags=re.DOTALL | re.IGNORECASE)) or "")
                date  = _as_iso_date(pubd) or _as_iso_date(title) or _as_iso_date(desc)
                text  = desc or title
                if text:
                    _append(out, date, text, src="SMG")
            if out:
                return out

        # HTML 文字保底（通常用不到）
        plain = re.sub(r"<[^>]+>", " ", data)
        lines = [l.strip() for l in plain.splitlines() if l.strip()]
        for i, l in enumerate(lines):
            if re.search(r"(20\d{2})[-/\.](\d{1,2})[-/\.](\d{1,2})", l):
                date = _as_iso_date(l)
                desc = lines[i+1] if i+1 < len(lines) else None
                _append(out, date, desc, src="SMG")
                if len(out) >= 7:
                    break
        return out

    # 若 data 是 dict/list（極少見）
    if isinstance(data, dict):
        arr = data.get("items") or data.get("list") or []
        for it in arr[:10]:
            date = it.get("date") or it.get("pubDate") or it.get("time") or it.get("updated")
            text = it.get("summary") or it.get("description") or it.get("title")
            _append(out, date, text, src="SMG")
    elif isinstance(data, list):
        for it in data[:10]:
            if isinstance(it, dict):
                date = it.get("date") or it.get("pubDate") or it.get("time") or it.get("updated")
                text = it.get("summary") or it.get("description") or it.get("title")
                _append(out, date, text, src="SMG")
    return out

# 6) BOM（保留：有些端點差異很大，做多種嘗試）
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
            _append(out, date, text, tmin, tmax, src="BOM")

    if not out:
        days = _safe_get(root, "forecasts", "districts", 0, "forecast", "days", default=[])
        if isinstance(days, list) and days:
            for d in days:
                _append(out, d.get("date"), d.get("text"), d.get("temp_min"), d.get("temp_max"), src="BOM")

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
                            src="BOM"
                        )
                if out:
                    break

    # 去重
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 7) NOAA （簡化版：把白天 period 當作當日摘要）
def _f_to_c(v: Any) -> Optional[float]:
    try:
        return round((float(v) - 32) * 5.0/9.0, 1)
    except Exception:
        return None

def _map_noaa(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    periods = _safe_get(root, "properties", "periods", default=[])
    out: List[Dict[str, Any]] = []
    if not isinstance(periods, list) or not periods:
        return out
    by_date: Dict[str, Dict[str, Any]] = {}
    for p in periods:
        date = _as_iso_date(p.get("startTime"))
        if not date:
            continue
        if date not in by_date:
            by_date[date] = {"day": None, "night": None}
        if p.get("isDaytime"):
            by_date[date]["day"] = p
        else:
            by_date[date]["night"] = p
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

# 8) 通用 mapper（當某來源尚未寫專屬解析）
def _map_generic(raw: Dict[str, Any], src_name: str) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), (list, dict, str)) else raw
    # 若抓到的是 XML / HTML 純字串，沒有統一規格，很難泛用標準化，就回傳空
    if isinstance(root, str):
        return []

    arr = None
    if isinstance(root, dict):
        for key in ("forecasts","daily","items","days","list","data","periods"):
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
        date = d.get("date") or d.get("validDate") or d.get("forecastDate") or d.get("startTime")
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("forecast") or d.get("wx")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d,"temperature","min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d,"temperature","max")
        _append(out, date, text, tmin, tmax, src_name)
    return out[:10]

# 入口：根據 provider 決定 mapper（含容錯回退）
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
            all_items[prov] = arr[:10]   # 至多保留 10 天

    # 分來源
    (OUT / "normalized.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 扁平陣列（方便 debug）
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
