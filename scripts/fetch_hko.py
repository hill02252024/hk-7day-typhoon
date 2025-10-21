import json, time, pathlib, requests
out = pathlib.Path("data/raw/hko"); out.mkdir(parents=True, exist_ok=True)

def main():
    url = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=en"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    payload = {"fetched_at": int(time.time()), "data": r.json()}
    (out / "latest.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

if __name__ == "__main__":
    main()
