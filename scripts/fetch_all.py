# scripts/fetch_all.py
from __future__ import annotations
import json
import time
import pathlib
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from providers import PROVIDERS, get_url


RAW = pathlib.Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

UA = (
    "hk-7day-typhoon/1.0 (+github-actions; for research & non-commercial use) "
    "python-requests"
)

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    retries = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _save(provider: str, payload: Dict[str, Any]) -> None:
    outdir = RAW / provider
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "latest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


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
        resp = session.get(url, timeout=20)
        out["http_status"] = resp.status_code
        ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        out["response_content_type"] = ctype

        if resp.status_code != 200:
            out["error"] = f"HTTP {resp.status_code}"
            return out

        # JSON → 解析為物件；RSS/XML/HTML → 儲存純文字
        if "json" in ctype or url.lower().endswith(".json"):
            try:
                out["data"] = resp.json()
                out["ok"] = True
            except Exception as e:
                out["error"] = f"JSON parse error: {e}"
        else:
            text = resp.text
            out["data"] = text
            out["ok"] = True

        return out
    except Exception as e:
        out["error"] = repr(e)
        return out


def main():
    s = _make_session()
    any_success = False

    for prov in PROVIDERS:
        url = get_url(prov)
        if not url:
            # 沒有設定 URL 就跳過，但仍然留一個 latest.json 告知 skipped
            _save(prov, {
                "fetched_at": int(time.time()),
                "provider": prov,
                "ok": False,
                "requested_url": None,
                "http_status": None,
                "response_content_type": None,
                "data": None,
                "error": "skipped: no URL",
            })
            continue

        payload = _fetch_one(s, prov, url)
        _save(prov, payload)
        if payload.get("ok"):
            any_success = True

    if not any_success:
        # 讓 workflow 不要因沒有資料就靜默成功
        print("No provider fetched successfully. Check secrets/URLs.")
    else:
        print("Fetch done.")


if __name__ == "__main__":
    main()
