from __future__ import annotations
import os
from typing import Dict, List, Optional

# 想抓取的來源（順序不影響）
PROVIDERS: List[str] = [
    "hko",      # 香港天文台
    "jma",      # 日本氣象廳
    "mss",      # 新加坡 data.gov.sg
    "metno",    # MET Norway
    "smg",      # 澳門 SMG
    # 其餘保留（目前不影響 0–5 天共識）
    "jtwc", "cwa", "kma", "bom", "tmd", "noaa", "bmkg",
]

# 對應的 Actions Secrets / Env key
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
    """回傳該 provider 的 URL（空字串或缺少時回傳 None）"""
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    v = os.getenv(key, "").strip()
    return v or None
