# scripts/build_hk_impact.py
from __future__ import annotations
import json, pathlib, time

OUT = pathlib.Path("data/processed/hk_impact.json")

def main():
    payload = {
        "as_of_utc": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "risk": "Low",
        "note": "MVP demo: impact metrics will be added when track/intensity ensemble is ready."
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
