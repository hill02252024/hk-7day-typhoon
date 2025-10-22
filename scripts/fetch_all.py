# scripts/fetch_all.py
from __future__ import annotations
import json, time, pathlib, requests, os, random
from providers import PROVIDERS, get_url

RAW = pathlib.Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

UA = (
    "hk-7day-typhoon/1.0 (+https://github.com/<YOUR_USER>/hk-7day-typhoon; contact=github-actions@noreply.github.com)"
)
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
}

def fetch_generic(url: str, max_try: int = 3):
    """抓取任意 URL：
       - 預設帶 User-Agent（NOAA 要求）
       - 簡單重試
       - 若回 JSON 就回傳 JSON，否則以文字回傳
    """
    last_err = None
    for i in range(1, max_try + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            ctype = (r.headers.get("content-type") or "").lower()
            if "json" in ctype:
                return {"_kind": "json", "data": r.json()}
            else:
                txt = r.text
                try:
                    return {"_kind": "json", "data": json.loads(txt)}
                except Exception:
                    return {"_kind": "text", "data": txt}
        except Exception as e:
            last_err = e
            # NOAA 有時偶發 5xx/429，退避一下
            time.sleep(0.8 + random.random() * 0.6)
    raise last_err

def url_with_default(provider: str) -> str | None:
    # 使用 Secrets 之 URL；HKO 提供預設值，其他來源須你在 Secrets 內填入
    url = get_url(provider)
    if url:
        return url
    if provider == "hko":
        return "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=en"
    return None

def main():
    for p in PROVIDERS:
        out_dir = RAW / p
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {"fetched_at": int(time.time()), "provider": p, "ok": False}
        try:
            url = url_with_default(p)
            if not url:
                payload["error"] = f"Missing URL env for {p}"
            else:
                res = fetch_generic(url)
                payload.update({"ok": True, "content_type": res["_kind"], "data": res["data"]})
        except Exception as e:
            payload["error"] = str(e)

        (out_dir / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

if __name__ == "__main__":
    main()
