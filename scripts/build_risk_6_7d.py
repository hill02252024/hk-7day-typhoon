import json, pathlib
inp = pathlib.Path("data/processed/normalized.json")
out = pathlib.Path("data/processed/risk_6_7d.json")
if inp.exists():
    data = json.loads(inp.read_text(encoding="utf-8"))
    out.write_text(json.dumps(data[5:7], ensure_ascii=False), encoding="utf-8")
