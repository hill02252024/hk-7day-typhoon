# scripts/providers.py
from __future__ import annotations
import os
from typing import Dict, List, Optional

# 你可以自由調整順序。以下列出 10+ 官方來源，實際是否抓取取決於是否提供對應的 URL。
PROVIDERS: List[str] = [
    # 香港 & 近域
    "hko",          # 香港天文台（9-day）
    "jma",          # 日本氣象廳（bosai/forecast）
    "mss",          # 新加坡（data.gov.sg）
    "metno",        # 挪威氣象 MET Norway（locationforecast）
    "smg",          # 澳門 SMG（7-day XML）

    # 其餘保留（可選）
    "jtwc",
    "cwa",
    "kma",
    "bom",
    "tmd",
    "noaa",
    "bmkg",
]

# 對應的環境變數名稱（工作流會把 secrets 注入成 env）。沒有值的來源會被自動跳過。
ENV_KEYS: Dict[str, str] = {
    "hko": "HKO_URL",
    "jma": "JMA_URL",
    "mss": "MSS_URL",
    "metno": "METNO_URL",
    "smg": "SMG_URL",

    "jtwc": "JTWC_URL",
    "cwa": "CWA_URL",
    "kma": "KMA_URL",
    "bom": "BOM_URL",
    "tmd": "TMD_URL",
    "noaa": "NOAA_URL",
    "bmkg": "BMKG_URL",
}

def get_url(provider: str) -> Optional[str]:
    """回傳該 provider 的環境變數 URL（空字串或不存在時回傳 None）"""
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    val = os.getenv(key, "").strip()
    return val or None
