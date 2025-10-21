# scripts/normalize_all.py
from __future__ import annotations
import json, pathlib
from typing import List, Dict, Any
from providers import PROVIDERS

RAW = pathlib.Path("data/raw")
OUT = pathlib.Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

def _safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _as_iso_date(s: str | None) -> str | None:
    # 有些來源會用 YYYYMMDD / YYYY-MM-DD；這裡盡量原樣保留
    if not s:
        return None
    s = str(s).strip()
    return s

def _map_hko(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    wf = _safe_get(raw, "data", "weatherForecast", default=[])
    out = []
    for d in wf:
        out.append({
            "date": _as_iso_date(d.get("forecastDate")),
            "text": d.get("forecastWeather"),
            "tmin": _safe_get(d, "forecastMintemp", "value"),
            "tmax": _safe_get(d, "forecastMaxtemp", "value"),
            "src": "HKO",
        })
    return out

# 其他來源的通用嘗試：
def _map_generic(raw: Dict[str, Any], src_name: str) -> List[Dict[str, Any]]:
    """
    嘗試從常見欄位推斷 7 日結構：
    - 陣列 items/forecasts/daily
    - 欄位 date / validDate / day
    - 文本 text / summary / weather
    - 溫度 min/max / tmin/tmax
    """
    root = raw.get("data") if isinstance(raw.get("data"), (list, dict)) else raw
    candidates = []
    for key in ("forecasts","daily","items","days","list","data"):
        val = root.get(key) if isinstance(root, dict) else None
        if isinstance(val, list) and len(val)>0:
            candidates = val
            break
    if not candidates and isinstance(root, list):
        candidates = root

    out=[]
    for d in candidates:
        if not isinstance(d, dict): 
            continue
        date = d.get("date") or d.get("validDate") or d.get("forecastDate") or d.get("day")
        text = d.get("text") or d.get("summary") or d.get("weather") or d.get("wx")
        tmin = d.get("tmin") or d.get("min") or d.get("min_temp") or _safe_get(d,"temperature","min")
        tmax = d.get("tmax") or d.get("max") or d.get("max_temp") or _safe_get(d,"temperature","max")
        out.append({
            "date": _as_iso_date(date),
            "text": text,
            "tmin": tmin,
            "tmax": tmax,
            "src": src_name.upper()
        })
    return out

def normalize_one(provider: str) -> List[Dict[str, Any]]:
    p = RAW / provider / "latest.json"
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))

    # 只有 ok==True 才處理
    if not raw.get("ok"):
        return []

    # 專屬 mapper
    if provider == "hko":
        return _map_hko(raw)

    # 其他先走通用 mapper（若某來源格式特殊，再加一個 _map_xxx）
    return _map_generic(raw, provider)

def main():
    all_items: Dict[str, List[Dict[str, Any]]] = {}
    total=0
    for prov in PROVIDERS:
        arr = normalize_one(prov)
        if arr:
            all_items[prov] = arr[:10]  # 最多取 10 天，後面會切 7 天
            total += len(arr)
    (OUT / "normalized.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # 也輸出一份 flat 方便 debug
    flat=[]
    for k,v in all_items.items():
        for it in v:
            it2=it.copy(); it2["src"]=k.upper(); flat.append(it2)
    (OUT / "normalized_flat.json").write_text(
        json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()
