import json, pathlib
inp = pathlib.Path("data/processed/normalized.json")
out = pathlib.Path("data/processed/consensus_0_5d.json")

if inp.exists():
    data = json.loads(inp.read_text(encoding="utf-8"))
    # 先示範只轉存前5天
    out.write_text(json.dumps(data[:5], ensure_ascii=False), encoding="utf-8")
