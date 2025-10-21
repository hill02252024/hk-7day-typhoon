# scripts/build_risk_6_7d.py
from __future__ import annotations
import json, pathlib
from collections import defaultdict

INP = pathlib.Path("data/processed/normalized.json")
OUT = pathlib.Path("data/processed/risk_6_7d.json")

def main():
    if not INP.exists():
        OUT.write_text("{}", encoding="utf-8"); return
    allprov = json.loads(INP.read_text(encoding="utf-8"))

    # 聚合同一天多來源，統計來源數
    agg = defaultdict(lambda: {"texts":[], "srcs":set()})
    for prov, arr in allprov.items():
        for it in arr[:7]:
            d = it.get("date")
            if not d: continue
            agg[d]["texts"].append(it.get("text"))
            agg[d]["srcs"].add(prov.upper())

    dates = sorted(agg.keys())[5:7]  # day6~7
    out = {"days": []}
    for d in dates:
        src_n = len(agg[d]["srcs"])
        level = "low"
        if src_n >= 6: level = "medium"
        if src_n >= 8: level = "medium-high"
        if src_n >= 9: level = "high"
        out["days"].append({
            "date": d,
            "source_count": src_n,
            "confidence": level,
            "note": "Extended outlook (6–7d). Confidence depends on how many agencies agree."
        })
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
