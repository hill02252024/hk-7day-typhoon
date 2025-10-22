# scripts/normalize_all.py
# 目的：把 data/raw/<provider>/latest.json 轉成統一的 7 日城市級預報格式
# 已內建專屬 mapper：HKO / JMA / MSS / BOM / NOAA
# 其他（JTWC / CWA / KMA / TMD / BMKG）先走通用 mapper（不會阻塞流程）

from __future__ import annotations
import json, pathlib, re
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

def _as_iso_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    # 常見格式：YYYYMMDD / YYYY-MM-DD / YYYY/MM/DD / 2025-10-22T00:00:00+08:00
    if re.fullmatch(r"\d{8}", s):                  # 20251022
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
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
        "text": (text or "").strip() or None,
        "tmin": tmin if (isinstance(tmin, (int, float)) or tmin is None) else None,
        "tmax": tmax if (isinstance(tmax, (int, float)) or tmax is None) else None,
        "src": src.upper()
    })

# ---------- 各來源 mapper ----------

# 1) HKO – 香港天文台 9-day API（JSON）
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

# 2) JMA – 日本氣象廳 bosai/forecast（JSON）
# 典型結構：[{ "timeSeries": [ { "timeDefines": [...], "areas": [{ "weathers": [...], ...}] }, ... ] }]
# 我們取第一個有 "weathers" 的 timeSeries，配上 timeDefines 生成逐日摘要。
def _map_jma(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = raw.get("data") if isinstance(raw.get("data"), list) else raw
    if isinstance(arr, list) and arr:
        root = arr[0]
    else:
        root = arr
    ts_list = _safe_get(root, "timeSeries", default=[]) or []
    out: List[Dict[str, Any]] = []
    for ts in ts_list:
        time_def = ts.get("timeDefines")
        areas = ts.get("areas")
        if isinstance(time_def, list) and isinstance(areas, list) and areas:
            # 取第一個地區（JMA JSON 依地區代碼；這裡只為了 MVP 取一個，避免重複）
            weathers = areas[0].get("weathers") or areas[0].get("weatherCodes")
            # 部分 feed 放在 "pops"、"temps"；我們只取 weathers 作文字
            if isinstance(weathers, list) and weathers:
                for i, t in enumerate(time_def):
                    text = weathers[i] if i < len(weathers) else None
                    _append(out, t, text, src="JMA")
                break
    # JMA 可能是 3 小時/12 小時粒度；簡化：只保留每天第一筆
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 3) MSS – 新加坡 data.gov.sg 24h 預報（JSON）
# 結構：items[0].general.forecast，valid_period.start / end
# 我們輸出 1 天的摘要（MVP；之後可擴充 2–3 天展望端點）
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

# 4) BOM – 澳洲 BOM district forecast JSON（例如 IDN11060.json）
# 常見結構：{"data":{...}} 或直接 {"forecasts": ...}；不同產品略有差異。
# 我們嘗試多種常見鍵位，抓出 (date, text, min/max temperature)。
def _map_bom(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    out: List[Dict[str, Any]] = []

    # A. 有些 feed 長這樣：{"product":{"periods":[{"startTimeLocal":"...","text":"...", "tempMin":..,"tempMax":..}, ...]}}
    periods = _safe_get(root, "product", "periods", default=[])
    if isinstance(periods, list) and periods:
        for p in periods:
            date = p.get("startTimeLocal") or p.get("startTimeUTC") or p.get("start")
            text = p.get("text") or p.get("detailedForecast") or p.get("summary")
            tmin = p.get("tempMin") or p.get("air_temperature_minimum")
            tmax = p.get("tempMax") or p.get("air_temperature_maximum")
            _append(out, date, text, tmin, tmax, src="BOM")

    # B. 另一種：{"forecasts":{"districts":[{"forecast":{"days":[{"date":"YYYY-MM-DD","text":"...","temp_min":..,"temp_max":..}]}}]}}
    if not out:
        days = _safe_get(root, "forecasts", "districts", 0, "forecast", "days", default=[])
        if isinstance(days, list) and days:
            for d in days:
                _append(out, d.get("date"), d.get("text"), d.get("temp_min"), d.get("temp_max"), src="BOM")

    # C. 保底：在陣列中找帶有 date 和 text 的物件
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
    # 去重日期
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in out:
        d = it.get("date")
        if d and d not in dedup:
            dedup[d] = it
    return list(dedup.values())

# 5) NOAA / NWS – api.weather.gov gridpoints（JSON）
# 結構：{"properties":{"periods":[{"startTime":"2025-10-22T06:00:00-10:00","isDaytime":true,"temperature":86,"temperatureUnit":"F","detailedForecast":"..."}]}}
# 我們把 periods 依「日期」分組，偏好 isDaytime=True 的那筆；華氏→攝氏。
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
    # 依日期分組
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
    # 優先取白天；溫度只放入對應那一筆
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
        # 由於 NWS 給的是單一溫度（時段），我們把它放到 tmax，tmin 留空
        _append(out, d, text, None, t, src="NOAA")
    return out

# 6) 通用 mapper（當某來源尚未寫專屬解析）
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
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("forecast")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d,"temperature","min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d,"temperature","max")
        _append(out, date, text, tmin, tmax, src_name)
    # 只取前 10 天（之後會在共識步驟切成 5 天或 7 天）
    return out[:10]

# 入口：根據 provider 決定 mapper
def normalize_one(provider: str) -> List[Dict[str, Any]]:
    p = RAW / provider / "latest.json"
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not raw.get("ok"):
        return []
    try:
        if provider == "hko":
            return _map_hko(raw)
        if provider == "jma":
            return _map_jma(raw)
        if provider == "mss":
            return _map_mss(raw)
        if provider == "bom":
            return _map_bom(raw)
        if provider == "noaa":
            return _map_noaa(raw)
        # 其他來源先走通用解析；等你之後補 API 後再寫專屬 mapper
        return _map_generic(raw, provider)
    except Exception:
        # 任何解析失敗都不讓流程中斷
        return []

def main():
    all_items: Dict[str, List[Dict[str, Any]]] = {}
    for prov in PROVIDERS:
        arr = normalize_one(prov)
        if arr:
            # 最多保留 10 天（共識步驟會選 0–5 / 6–7）
            all_items[prov] = arr[:10]

    # 存 dict（分來源）
    (OUT / "normalized.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 另存一份扁平陣列方便 debug
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
