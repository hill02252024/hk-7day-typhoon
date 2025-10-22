# scripts/providers.py
from __future__ import annotations
import os
from typing import Dict, List

"""
列出要抓取與處理的官方來源，並定義每個來源對應的環境變數名稱。
只要在 PROVIDERS 裡有列出且對應的 ENV 有值，fetch_all.py 就會抓；
normalize_all.py 也會依此清單嘗試標準化。
"""

# 你可以自行調整順序；前 5 個是你目前使用的骨幹來源
PROVIDERS: List[str] = [
    "hko",      # 香港天文台
    "jma",      # 日本氣象廳
    "metno",    # MET Norway
    "mss",      # 新加坡
    "smg",      # 澳門（RSS）
    # 其餘先保留，之後需要時再補 secrets 與專屬 mapper
    "jtwc", "cwa", "kma", "bom", "tmd", "noaa", "bmkg",
]

# 對應每個 provider 要讀取的環境變數名稱
ENV_KEYS: Dict[str, str] = {
    "hko": "HKO_URL",
    "jma": "JMA_URL",
    "metno": "METNO_URL",
    "mss": "MSS_URL",
    "smg": "SMG_URL",

    "jtwc": "JTWC_URL",
    "cwa":  "CWA_URL",
    "kma":  "KMA_URL",
    "bom":  "BOM_URL",
    "tmd":  "TMD_URL",
    "noaa": "NOAA_URL",
    "bmkg": "BMKG_URL",
}


def get_url(provider: str) -> str | None:
    """回傳對應 provider 的完整端點（來自 Actions Secrets）"""
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    val = os.getenv(key, "").strip()
    return val or None
