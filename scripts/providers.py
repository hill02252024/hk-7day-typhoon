# scripts/providers.py
from __future__ import annotations
import os
from typing import Dict, List

# 10 個官方來源（鍵名=資料夾名/顯示名）
PROVIDERS: List[str] = [
    "hko", "jma", "jtwc", "cwa", "kma",
    "bom", "mss", "tmd", "noaa", "bmkg",
]

# 對應的環境變數名稱（工作流會把 secrets 注入成 env）
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

def get_url(provider: str) -> str | None:
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    val = os.getenv(key, "").strip()
    return val or None
