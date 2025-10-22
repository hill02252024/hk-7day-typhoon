# 統一抓取 data/raw/<provider>/latest.json
# - 會寫入 ok/http_status/response_content_type/requested_url/data/error
# - 為 metno/mss/smg 調整必要 header 與重試；並即時打印抓取摘要
from __future__ import annotations
import json, pathlib, time, os
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter, Retry

from providers import PROVIDERS, get_url

RAW_ROOT = pathlib.Path("data/raw")

def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def _headers_for(provider: str) -> Dict[str, str]:
    ua_default = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    h = {"User-Agent": ua_default, "Accept": "*/*"}

    # 澳門 SMG RSS/XML
    if provider == "smg":
        h.update({
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://xml.smg.gov.mo/",
        })

    # 新加坡 data.gov.sg
    if provider == "mss":
        h.update({
            "Accept": "application/json",
            "Referer": "https://api.data.gov.sg/",
        })

    # MET Norway（嚴格要求 UA 與聯絡資訊）
    if provider == "metno":
        proj = os.getenv("METNO_PROJECT", "hk-7day-typhoon/1.0")
        contact = os.getenv("METNO_CONTACT", "contact@example.com")
        h.update({
            "User-Agent": f"{proj} ({contact})",
            "From": contact,
            "Accept": "application/json,application/xml;q=0.9,*/*;q=0.8",
        })
    return h

def _fetch_one(session: requests.Session, provider: str, url: str) -> Dict[str, Any]:
    now = int(time.time())
    out: Dict[str, Any] = {
        "fetched_at": now,
        "provider": provider,
        "ok": False,
        "requested_url": url,
        "http_status": None,
        "response_content_type": None,
        "data": None,
        "error": None,
    }
    try:
        resp = session.get(url, timeout=30, headers=_headers_for(provider))
        out["http_status"] = resp.status_code
        ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        out["response_content_type"] = ctype

        if resp.status_code != 200:
            out["error"] = f"HTTP {resp.status_code}"
            return out

        text = resp.text or ""
        if not text.strip():
            out["error"] = "Empty body"
            return out

        out["data"] = text  # 解析放 normalize 階段
        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = repr(e)
        return out

def main():
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    s = _make_session()

    for prov in PROVIDERS:
        url = get_url(prov)
        d = RAW_ROOT / prov
        d.mkdir(parents=True, exist_ok=True)
        outp = d / "latest.json"

        if not url:
            result = {
                "fetched_at": int(time.time()),
                "provider": prov,
                "ok": False,
                "requested_url": None,
                "http_status": None,
                "response_content_type": None,
                "data": None,
                "error": "MISSING_URL",
            }
            outp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[{prov.upper()}] url=∅  -> skip")
            continue

        result = _fetch_one(s, prov, url)
        outp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        # 立刻打印重點（一眼看出哪個失敗）
        ok = result.get("ok")
        http = result.get("http_status")
        rctype = result.get("response_content_type")
        err = result.get("error")
        print(f"[{prov.upper()}] ok={ok} http={http} type={rctype} err={err} url={url}")

if __name__ == "__main__":
    main()
