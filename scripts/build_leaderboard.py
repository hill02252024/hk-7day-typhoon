# scripts/build_leaderboard.py
from __future__ import annotations
import json, pathlib, time

INP = pathlib.Path("data/processed/normalized.json")
OUT = pathlib.Path("data/processed/leaderboard.json")

def main():
    lb = {
      "as_of_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
      "overall_best": "—",
      "by_lead": {},
      "by_metric": {},
      "weights": {}
    }
    if INP.exists():
        allprov = json.loads(INP.read_text(encoding="utf-8"))
        # 先以「有提供資料的來源數」排行（示範）
        counts = {k.upper(): len(v) for k,v in allprov.items()}
        sorted_list = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        lb["overall_best"] = sorted_list[0][0] if sorted_list else "—"
        lb["weights"] = {k: round(c/sum(counts.values()), 3) for k,c in counts.items()} if counts else {}
    OUT.write_text(json.dumps(lb, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
