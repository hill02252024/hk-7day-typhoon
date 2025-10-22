# scripts/build_ensemble_0_5d.py
from __future__ import annotations
import json, pathlib, statistics, datetime as dt
from collections import Counter, defaultdict

INP = pathlib.Path("data/processed/normalized.json")
OUT = pathlib.Path("data/processed/consensus_0_5d.json")

HK_TZ = dt.timezone(dt.timedelta(hours=8))  # Asia/Hong_Kong（不依賴第三方）

def today_hk_str():
    d = dt.datetime.now(tz=HK_TZ).date()
    return d.strftime("%Y-%m-%d")

def to_ymd(s):
    # 支援 "YYYYMMDD" / "YYYY-MM-DD" / "YYYY-MM-DDTHH:MM"
    if not s:
        return None
    s = str(s)
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]  # 取日期部分

def pick_text_majority(texts):
    texts = [t.strip() for t in texts if isinstance(t, str) and t.strip()]
    if not texts:
        return None
    c = Counter(texts).most_common(3)
    if len(c) == 1:
        return c[0][0]
    # 前二名用「 | 」拼接，保留資訊量
    return " | ".join([x for x,_ in c[:2]])

def median_or_none(nums):
    nums = [float(x) for x in nums if x is not None]
    if not nums:
        return None
    try:
        return round(statistics.median(nums), 1)
    except statistics.StatisticsError:
        return None

def load_normalized():
    """同時支援
    - 新版：{ "hko":[...], "jma":[...], ... }
    - 舊版：[ {...}, {...} ]
    回傳統一的扁平列表，每筆含 {date,text,tmin,tmax,src}
    """
    raw = json.loads(INP.read_text(encoding="utf-8"))
    flat = []
    if isinstance(raw, dict):
        for prov, arr in raw.items():
            if not isinstance(arr, list): 
                continue
            for it in arr:
                it2 = {**it}
                it2["src"] = (it.get("src") or prov or "").upper()
                it2["date"] = to_ymd(it.get("date"))
                flat.append(it2)
    elif isinstance(raw, list):
        for it in raw:
            if isinstance(it, dict):
                it2 = {**it}
                it2["src"] = (it.get("src") or "").upper()
                it2["date"] = to_ymd(it.get("date"))
                flat.append(it2)
    return flat

def main():
    if not INP.exists():
        OUT.write_text("{}", encoding="utf-8"); return

    flat = load_normalized()
    if not flat:
        OUT.write_text("{}", encoding="utf-8"); return

    # 以「日期」聚合多來源
    agg = defaultdict(lambda: {"tmin":[], "tmax":[], "texts":[], "sources":set()})
    for it in flat:
        d = to_ymd(it.get("date"))
        if not d:
            continue
        agg[d]["tmin"].append(it.get("tmin"))
        agg[d]["tmax"].append(it.get("tmax"))
        agg[d]["texts"].append(it.get("text"))
        src = (it.get("src") or "").upper()
        if src:
            agg[d]["sources"].add(src)

    # 只取「今天（香港）起」的日期，避免把 JMA/MSS 的較早日期排最前
    today = today_hk_str()
    dates = sorted([d for d in agg.keys() if d >= today])[:5]

    out = {
        "days": [],
        "meta": {
            "sources_used": sorted({s for d in dates for s in agg[d]["sources"]}),
            "source_count_by_day": {d: len(agg[d]["sources"]) for d in dates},
            "from_date_hk": today
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
