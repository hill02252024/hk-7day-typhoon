# scripts/fetch_all.py
# 統一抓取 data/raw/<provider>/latest.json
# - 會寫入中繼欄位：ok/http_status/response_content_type/requested_url/data/error
# - 針對 SMG 加上 Referer 與「瀏覽器 UA」避免被擋
from __future__ import annotations
import json
import pathlib
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from providers import PROVIDERS, get_url

RAW_ROOT = pathlib.Path("data/raw")


def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _headers_for(provider: str) -> Dict[str, str]:
    # 一般 UA
    ua_default = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    h = {
        "User-Agent": ua_default,
        "Accept": "*/*",
    }
    # SMG（澳門）RSS/XML 有時會擋非瀏覽器：加強 Accept 與 Referer
    if provider == "smg":
        h.update({
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            "Referer": "https://xml.smg.gov.mo/",
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
        resp = session.get(url, timeout=25, headers=_headers_for(provider))
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

        # 允許 JSON / XML / RSS / 純文字（回到 normalize 再判斷）
        out["data"] = text
        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = repr(e)
        return out


def main():
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    session = _make_session()

    for prov in PROVIDERS:
        url = get_url(prov)
        prov_dir = RAW_ROOT / prov
        prov_dir.mkdir(parents=True, exist_ok=True)
        out_path = prov_dir / "latest.json"

        if not url:
            # 沒提供 URL → 不抓，但寫個簡短 stub 方便 debug
            stub = {
                "fetched_at": int(time.time()),
                "provider": prov,
                "ok": False,
                "requested_url": None,
                "http_status": None,
                "response_content_type": None,
                "data": None,
                "error": "MISSING_URL",
            }
            out_path.write_text(json.dumps(stub, ensure_ascii=False, indent=2), encoding="utf-8")
            continue

        result = _fetch_one(session, prov, url)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
