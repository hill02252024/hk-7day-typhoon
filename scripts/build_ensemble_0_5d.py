# scripts/build_ensemble_0_5d.py
# 目的：把 data/processed/normalized.json 中的多來源資料，合成 0–5 天「共識」輸出
# 保留原本硬編碼白名單，並加入 SMG（共 5 家：HKO / JMA / METNO / MSS / SMG）

from __future__ import annotations
import json, pathlib, statistics
from typing import Dict, List, Any, Optional

PROC = pathlib.Path("data/processed")
PROC.mkdir(parents=True, exist_ok=True)

# --- 小工具 ---
def _median(nums: List[float]) -> Optional[float]:
    vals = [float(x) for x in nums if isinstance(x, (int, float))]
    if not vals:
        return None
    try:
        return round(statistics.median(vals), 1)
    except Exception:
        return None

def main():
    nfile = PROC / "normalized.json"
    if not nfile.exists():
        print("normalized.json not found; skip")
        return

    norm = json.loads(nfile.read_text(encoding="utf-8"))
    if not isinstance(norm, dict):
        print("normalized.json format unexpected; skip")
        return

    # === 只用這 5 家（保留原本 4 家 + 新增 SMG） ===
    ALLOWED = ["hko", "jma", "metno", "mss", "smg"]

    # 依白名單順序、且實際有資料才納入
    sources = [s for s in ALLOWED if s in norm and isinstance(norm.get(s), list) and norm.get(s)]

    # 蒐集日期（union），只做前 5 天
    date_set = set()
    by_src: Dict[str, Dict[str, Any]] = {}
    for s in sources:
        m: Dict[str, Any] = {}
        for it in norm.get(s, []):
            d = it.get("date")
            if not d:
                continue
            date_set.add(d)
            m[d] = it
        by_src[s] = m

    dates = sorted(date_set)[:5]

    out_days: List[Dict[str, Any]] = []
    for d in dates:
        texts: List[str] = []
        tmins: List[float] = []
        tmaxs: List[float] = []
        used: List[str] = []
        for s in sources:
            it = by_src.get(s, {}).get(d)
            if not it:
                continue
            used.append(s.upper())
            if it.get("text"):
                texts.append(it["text"])
            if isinstance(it.get("tmin"), (int, float)):
                tmins.append(it["tmin"])
            if isinstance(it.get("tmax"), (int, float)):
                tmaxs.append(it["tmax"])

        out_days.append({
            "date": d,
            "text": " | ".join(texts) if texts else None,
            "tmin": _median(tmins),
            "tmax": _median(tmaxs),
            "sources": used,  # 這一天實際有資料的來源（大寫）
        })

    out = {
        "meta": {
            "sources_used": [s.upper() for s in sources],  # 全域使用的來源（大寫）
            "provider_count": len(sources),
        },
        "days": out_days,
    }

    (PROC / "consensus_0_5d.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("consensus_0_5d.json written. sources_used =", out["meta"]["sources_used"])

if __name__ == "__main__":
    main()
