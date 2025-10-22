# scripts/fetch_all.py
# 將各來源的資料抓下來，存成 data/raw/<provider>/latest.json
from __future__ import annotations
import json, os, pathlib, time
from typing import Dict, Any, List, Optional
import requests

from providers import PROVIDERS, get_url

RAW = pathlib.Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

UA = "hk-7day-typhoon/1.0 (contact: your-email@example.com)"  # met.no 要求必填 UA
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/xml, application/rss+xml, text/plain, */*",
}

def url_with_default(provider: str) -> List[str]:
    """依來源回傳候選 URL（優先 Secrets，再補上預設）"""
    cands: List[str] = []
    s = get_url(provider)
    if s:
        cands.append(s)

    if provider == "hko":
        cands += [
            "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=en"
        ]
    if provider == "jma":
        # 範例：bosai forecast（需你自己配置；若未設，先留空由 normalize fallback）
        pass
    if provider == "mss":
        cands += [
            "https://api.data.gov.sg/v1/environment/24-hour-weather-forecast"
        ]
    if provider == "metno":
        # 香港中環座標；可用 METNO_URL 自訂
        cands += [
            "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=22.302&lon=114.177"
        ]
    if provider == "smg":
        # 澳門 SMG 七日預報 RSS（中/英都可）
        cands += [
            "http://rss.smg.gov.mo/c_WForecast7days_rss.xml",
            "http://rss.smg.gov.mo/c_WForecast_rss.xml",
        ]
    return cands

def fetch_one(provider: str) -> Dict[str, Any]:
    out_dir = RAW / provider
    out_dir.mkdir(parents=True, exist_ok=True)
    latest = out_dir / "latest.json"

    cands = url_with_default(provider)
    last_err: Optional[str] = None
    last_status: Optional[int] = None

    for url in cands:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            last_status = r.status_code
            if r.status_code // 100 != 2:
                last_err = f"http {r.status_code}"
                continue

            ctype = (r.headers.get("Content-Type") or "").lower()
            text = r.text

            # 嘗試 JSON 解析
            data: Any
            try:
                data = r.json()
                content_type = "json"
            except Exception:
                data = text
                # 簡單判定 RSS/HTML 純文字
                if "xml" in ctype or text.strip().startswith("<?xml"):
                    content_type = "xml"
                else:
                    content_type = "text"

            payload = {
                "fetched_at": int(time.time()),
                "provider": provider,
                "ok": True,
                "requested_url": url,
                "http_status": r.status_code,
                "content_type": content_type,
                "data": data,
            }
            latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return payload

        except Exception as e:
            last_err = repr(e)
            continue

    # 全部候選都失敗
    payload = {
        "fetched_at": int(time.time()),
        "provider": provider,
        "ok": False,
        "requested_url": cands[0] if cands else None,
        "http_status": last_status,
        "error": last_err or "no url / no response",
    }
    latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

def main():
    report: List[str] = []
    for p in PROVIDERS:
        res = fetch_one(p)
        report.append(f"[{p.upper()}] ok={res.get('ok')} http={res.get('http_status')} url={res.get('requested_url')}")
    # 方便在 Actions 日誌上看到摘要
    print("\n=== Fetch Summary ===")
    for line in report:
        print(line)

if __name__ == "__main__":
    main()
