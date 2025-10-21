import json, pathlib, time
out = pathlib.Path("data/processed/hk_impact.json")
payload = {
    "as_of": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
    "risk": "Low",
    "message": "目前香港天氣穩定，7天內無颱風警報。"
}
out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
