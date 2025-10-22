# scripts/build_ensemble_0_5d.py
# 目的：把 data/processed/normalized.json 中的多來源資料，合成 0–5 天「共識」輸出
# 作法：
# 1) 動態讀取所有已標準化成功的來源（不再用硬編碼白名單）；
# 2) 依日期彙總各來源文字，tmin/tmax 取「中位數」（更抗極端值）；
# 3) 寫出 data/processed/consensus_0_5d.json 給前端使用。

from __future__ import annotations
import json, pathlib, statistics
from typing import Dict, List, Any, Optional

PROC = pathlib.Path("data/processed")
PROC.mkdir(parents=True, exist_ok=True)

def _median(nums: List[float]) -> Optional[float]:
    nums2 = [float(x) for x in nums if isinstance(x, (int, float))]
    if not nums2:
        return None
    try:
        return round(statistics.median(nums2), 1)
    except Exception:
        return None

def main():
    nfile = PROC / "normalized.json"
    if not nfile.exists():
        print("normalized.json not found; skip")
        return
    norm = json.loads(nfile.read_text(encoding="utf-8"))

    # ---- 來源使用邏輯 ----
    # 偏好順序（只用來排序顯示；不限制來源）
    PREFERRED = [
        "hko", "jma", "metno", "mss", "smg",
        "bom", "noaa", "cwa", "kma", "tmd", "bmkg", "jtwc"
    ]

    # 先按偏好順序取交集，再把其它有資料的來源接在後面
    present = list(norm.keys()) if isinstance(norm, dict) else []
    ordered = [p for p in PREFERRED if p in present] + [p for p in present if p not in PREFERRED]

    # 建立全域的日期清單（所有來源的 union），然後取最早的 5 天
    date_set = set()
    by_src: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for src in ordered:
        items = norm.get(src, [])
        m: Dict[str, Dict[str, Any]] = {}
        for it in items:
            d = it.get("date")
            if not d:
                continue
            date_set.add(d)
            m[d] = it
        by_src[src] = m

    all_dates = sorted(date_set)[:5]  # 只做 0–5 天
    out_days: List[Dict[str, Any]] = []

    for d in all_dates:
        texts = []
        tmins, tmaxs = [], []
        used_srcs = []

        for src in ordered:
            it = by_src.get(src, {}).get(d)
            if not it:
                continue
            used_srcs.append(src.upper())
            if it.get("text"):
                texts.append(it["text"])
            if isinstance(it.get("tmin"), (int, float)):
                tmins.append(it["tmin"])
            if isinstance(it.get("tmax"), (int, float)):
                tmaxs.append(it["tmax"])

        day_obj = {
            "date": d,
            "text": " | ".join(texts) if texts else None,
            "tmin": _median(tmins),
            "tmax": _median(tmaxs),
            "sources": used_srcs,   # 這一天實際有資料的來源
        }
        out_days.append(day_obj)

    out = {
        "meta": {
            "sources_used": [s.upper() for s in ordered],  # 全局層級顯示的來源列表（依偏好排序）
            "provider_count": len(ordered),
        },
        "days": out_days,
    }

    (PROC / "consensus_0_5d.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("consensus_0_5d.json written. sources_used =", out["meta"]["sources_used"])

if __name__ == "__main__":
    main()
