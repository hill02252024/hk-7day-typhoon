import json, pathlib
OUT = pathlib.Path("data/processed"); OUT.mkdir(parents=True, exist_ok=True)

def main():
    p = pathlib.Path("data/raw/hko/latest.json")
    if not p.exists():
        return
    raw = json.loads(p.read_text(encoding="utf-8"))
    days = raw["data"]["weatherForecast"]
    normalized = []
    for d in days:
        normalized.append({
            "agency": "HKO",
            "date": d["forecastDate"],
            "forecast": d["forecastWeather"],
            "min_temp": d["forecastMintemp"]["value"],
            "max_temp": d["forecastMaxtemp"]["value"]
        })
    (OUT / "normalized.json").write_text(json.dumps(normalized, ensure_ascii=False), encoding="utf-8")

if __name__ == "__main__":
    main()
