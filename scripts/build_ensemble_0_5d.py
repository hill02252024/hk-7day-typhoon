# scripts/build_ensemble_0_5d.py
from __future__ import annotations
import json, pathlib
from typing import Dict, Any, List, Optional
from datetime import datetime

INP = pathlib.Path("data/processed/normalized.json")
OUT = pathlib.Path("data/processed/ensemble_0_5.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

def _as_date(s: str) -> str:
    # 已是 YYYY-MM-DD 就直接回傳；否則嘗試切片
    if isinstance(s, str) and len(s) >= 10 and s[4] == "-":
        return s[:10]
    return s

def _pick_temp(records: List[Dict[str, Any]], kind: str) -> Optional[float]:
    """優先用 HKO 的 tmin/tmax；沒有就用其他來源第一個有數字的。"""
    # 1) HKO
    for r in records:
        if r.get("src") == "HKO" and isinstance(r.get(kind), (int, float)):
            return r[kind]
    # 2) 其他
    for r in records:
        v = r.get(kind)
        if isinstance(v, (int, float)):
            return v
    return None

def main():
    if not INP.exists():
        print("normalized.json not found; skip ensemble.")
        return
    norm = json.loads(INP.read_text(encoding="utf-8"))

    # 動態使用所有在 normalized.json 裡「有資料」的來源
    sources: List[str] = [k for k,v in norm.items() if isinstance(v, list) and len(v) > 0]
    sources = sorted(sources, key=lambda x: ["hko","jma","mss","metno","smg"].index(x) if x in ["hko","jma","mss","metno","smg"] else 99)

    # 聚合所有日期
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for prov, arr in norm.items():
        if not isinstance(arr, list) or not arr:
            continue
        for it in arr:
            d = _as_date(it.get("date"))
            if not d:
                continue
            rec = {
                "src": (prov or "").upper(),
                "text": (it.get("text") or "").strip() or None,
                "tmin": it.get("tmin"),
                "tmax": it.get("tmax"),
            }
            by_date.setdefault(d, []).append(rec)

    # 取未來 0–5 天（以日期排序後前 5 個）
    dates = sorted(by_date.keys())[:5]

    rows = []
    for d in dates:
        recs = by_date[d]
        # 文字：把多來源文字去重後串起（順序用上面 sources 排序）
        texts: List[str] = []
        for s in [s.upper() for s in sources]:
            for r in recs:
                if r["src"] == s and r.get("text"):
                    if r["text"] not in texts:
                        texts.append(r["text"])
        text_join = " | ".join(texts) if texts else None

        tmin = _pick_temp(recs, "tmin")
        tmax = _pick_temp(recs, "tmax")

        # 參與該日的來源列表
        part = sorted({r["src"] for r in recs})
        rows.append({
            "date": d,
            "text": text_join,
            "tmin": tmin,
            "tmax": tmax,
            "source_count": len(part),
            "sources": part,
        })

    meta = {
        "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "sources_used": [s.upper() for s in sources],
        "source_count": len(sources),
        "note": "0–5 天共識使用 normalized.json 中所有可用來源；溫度優先 HKO，否則取其他首個數值。",
    }

    OUT.write_text(json.dumps({"meta": meta, "rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("ensemble_0_5.json written. sources_used:", meta["sources_used"])

if __name__ == "__main__":
    main()
