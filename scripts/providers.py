# scripts/providers.py
from __future__ import annotations
import os
from typing import Dict, List, Optional

# 這裡列出要參與抓取/標準化/共識的來源（現在是 5 家）
PROVIDERS: List[str] = [
    "hko",   # 香港天文台
    "jma",   # 日本氣象廳（bosai）
    "mss",   # 新加坡
    "metno", # MET Norway (YR / locationforecast)
    "smg",   # 澳門地球物理暨氣象局（RSS）
]

# 對應的環境變數（workflow 會把 Secrets 注入成 env）
ENV_KEYS: Dict[str, str] = {
    "hko":   "HKO_URL",
    "jma":   "JMA_URL",
    "mss":   "MSS_URL",
    "metno": "METNO_URL",  # 可不設，fetch 會有預設 URL（香港座標）
    "smg":   "SMG_URL",    # 建議設；未設會走備用 RSS 清單
}

def get_url(provider: str) -> Optional[str]:
    key = ENV_KEYS.get(provider)
    if not key:
        return None
    val = os.getenv(key, "").strip()
    return val or None
