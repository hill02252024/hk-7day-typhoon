# scripts/fetch_all.py
from __future__ import annotations
import json, time, pathlib, requests
from providers import PROVIDERS, get_url

RAW = pathlib.Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

# 有些來源可能回傳 HTML/XML/TXT，這裡一律以文字存檔（如果可 JSON 就轉為 JSON 優先）
def fetch_generic(url: str):
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    ctype = (r.headers.get("content-type") or "").lower()
    if "json" in ctype:
        return {"_kind":"json", "data": r.json()}
    else:
        text = r.text
        # 嘗試把文本轉 JSON（如果剛好是 JSON 字串）
        try:
            return {"_kind":"json", "data": json.loads(text)}
        except Exception:
            return {"_kind":"text", "data": text}

def main():
    for p in PROVIDERS:
        out_dir = RAW / p
        out_dir.mkdir(parents=True, exist_ok=True)
        url = get_url(p)
        payload = {"fetched_at": int(time.time()), "provider": p, "ok": False}
        try:
            if not url:
                payload["error"] = f"Missing URL env for {p}"
            else:
                res = fetch_generic(url)
                payload["ok"] = True
                payload["content_type"] = res["_kind"]
                payload["data"] = res["data"]
        except Exception as e:
            payload["error"] = str(e)

        (out_dir / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

if __name__ == "__main__":
    main()
