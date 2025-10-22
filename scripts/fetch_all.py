# scripts/fetch_all.py
from __future__ import annotations
import json, time, pathlib, requests, os, random, urllib.parse

from providers import PROVIDERS, get_url

RAW = pathlib.Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

UA = "hk-7day-typhoon/1.0 (+github actions; contact=noreply@example.com)"
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
}

def _redact(url: str) -> str:
    """避免把 API key/Authorization 印到日誌"""
    try:
        u = urllib.parse.urlsplit(url)
        qs = urllib.parse.parse_qsl(u.query, keep_blank_values=True)
        safe_qs = []
        for k, v in qs:
            if k.lower() in {"authorization", "token", "apikey", "key", "appkey"}:
                safe_qs.append((k, "***"))
            else:
                safe_qs.append((k, v))
        new_query = urllib.parse.urlencode(safe_qs)
        return urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, new_query, u.fragment))
    except Exception:
        return url

def fetch_generic(url: str, max_try: int = 3):
    """帶 UA + 重試；回傳 (meta, payload)"""
    last_err = None
    for i in range(1, max_try + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            meta = {
                "http_status": r.status_code,
                "headers": {k.lower(): v for k, v in r.headers.items()},
            }
            r.raise_for_status()
            ctype = (r.headers.get("content-type") or "").lower()
            if "json" in ctype:
                data = r.json()
                return meta, {"_kind": "json", "data": data}
            else:
                txt = r.text
                try:
                    data = json.loads(txt)
                    return meta, {"_kind": "json", "data": data}
                except Exception:
                    return meta, {"_kind": "text", "data": txt}
        except Exception as e:
            last_err = e
            time.sleep(0.8 + random.random() * 0.7)
    raise last_err

def url_with_default(provider: str) -> str | None:
    url = get_url(provider)
    if url:
        return url
    # 只有 HKO 提供預設
    if provider == "hko":
        return "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=en"
    return None

def main():
    for p in PROVIDERS:
        out_dir = RAW / p
        out_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            "fetched_at": int(time.time()),
            "provider": p,
            "ok": False,
        }

        try:
            url = url_with_default(p)
            payload["requested_url"] = _redact(url) if url else None
            print(f"[fetch] {p.upper()} → {payload['requested_url']}")
            if not url:
                payload["error"] = f"Missing URL env for {p}"
            else:
                meta, res = fetch_generic(url)
                # 加入偵錯用中繼資料
                payload.update({
                    "ok": True,
                    "content_type": res["_kind"],
                    "http_status": meta.get("http_status"),
                    "response_content_type": meta.get("headers", {}).get("content-type"),
                })
                # 限制純文字回應的長度，避免日誌爆掉
                data = res["data"]
                if isinstance(data, str) and len(data) > 2000:
                    data = data[:2000] + "...<truncated>"
                payload["data"] = data
                # 打印摘要到 Actions 日誌
                data_len = len(json.dumps(data, ensure_ascii=False)) if not isinstance(data, str) else len(data)
                print(f"[fetch] {p.upper()} ok={payload['ok']} http={payload.get('http_status')} "
                      f"ctype={payload.get('response_content_type')} len≈{data_len}")
        except Exception as e:
            payload["error"] = str(e)
            print(f"[fetch] {p.upper()} ERROR: {e}")

        (out_dir / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

if __name__ == "__main__":
    main()
