# scripts/build_ensemble_0_5d.py
from __future__ import annotations
import json, pathlib, statistics
from collections import Counter, defaultdict

INP = pathlib.Path("data/processed/normalized.json")
OUT = pathlib.Path("data/processed/consensus_0_5d.json")

def pick_text_majority(texts):
    texts = [t for t in texts if isinstance(t, str) and t.strip()]
    if not texts: return None
    c = Counter([t.strip() for t in texts])
    top = c.most_common(2)
    if len(top)==1: return top[0][0]
    # 前二名組合，增加資訊量
    return f"{top[0][0]} | {top[1][0]}"

def median_or_none(nums):
    nums = [float(x) for x in nums if x is not None]
    if not nums: return None
    try:
        return round(statistics.median(nums), 1)
    except statistics.StatisticsError:
        return None

def main():
    if not INP.exists():
        OUT.write_text("{}", encoding="utf-8"); return
    allprov = json.loads(INP.read_text(encoding="utf-8"))

    # 聚合同一天的多來源：以日期為鍵
    # out[date] = {"tmin":[...],"tmax":[...],"texts":[...],"sources":[...]}
    agg = defaultdict(lambda: {"tmin":[], "tmax":[], "texts":[], "sources":set()})
    for prov, arr in allprov.items():
        for it in arr:
            date = it.get("date")
            if not date: continue
            agg[date]["tmin"].append(it.get("tmin"))
            agg[date]["tmax"].append(it.get("tmax"))
            agg[date]["texts"].append(it.get("text"))
            agg[date]["sources"].add(prov.upper())

    # 產出按日期排序的前 5 天
    dates = sorted(agg.keys())[:5]
    out = {
        "days": [],
        "meta": {
            "sources_used": sorted({s for d in dates for s in agg[d]["sources"]}),
            "source_count_by_day": {d: len(agg[d]["sources"]) for d in dates}
        }
    }
    for d in dates:
        out["days"].append({
            "date": d,
            "text": pick_text_majority(agg[d]["texts"]),
            "tmin": median_or_none(agg[d]["tmin"]),
            "tmax": median_or_none(agg[d]["tmax"]),
        })

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
