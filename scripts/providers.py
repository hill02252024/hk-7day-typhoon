# scripts/providers.py
from __future__ import annotations
import os
from typing import Dict, List, Optional

# === 所有 10 個官方來源（你希望都能被自動抓取） ===
PROVIDERS: List[str] = [
    "hko",   # 香港天文台
    "jma",   # 日本氣象廳
    "jtwc",  # 聯合颱風警報中心
    "cwa",   # 台灣中央氣象署
    "kma",   # 韓國氣象廳
    "bom",   # 澳洲氣象局 (Bureau of Meteorology)
    "mss",   # 新加坡氣象局 (Meteorological Service Singapore)
    "tmd",   # 泰國氣象局
    "noaa",  # 美國國家海洋與大氣總署 (NOAA/NWS)
    "bmkg",  # 印尼氣象氣候地球物理局
]

# === 每個來源對應的 GitHub Secrets 名稱 ===
ENV_KEYS: Dict[str, str] = {
    "hko": "HKO_URL",
    "jma": "JMA_URL",
    "jtwc": "JTWC_URL",
    "cwa": "CWA_URL",
    "kma": "KMA_URL",
    "bom": "BOM_URL",
    "mss": "MSS_URL",
    "tmd": "TMD_URL",
    "noaa": "NOAA_URL",
    "bmkg": "BMKG_URL",
}

def get_url(provider: str) -> Optional[str]:
    """
    從環境變數中讀取對應 provider 的 URL。
    若沒有設定對應的 Secret，回傳 None。
    """
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    val = os.getenv(key, "").strip()
    if not val:
        return None
    return val
