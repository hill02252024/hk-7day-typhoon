# 🇭🇰 HK 7-Day Typhoon Project

自動從全球 10 個官方氣象台 (HKO / JMA / JTWC / CWA / KMA / BoM / MSS / TMD / NOAA / BMKG)
抽取可公開 API 資料，分析共識預報 (ensemble)，並在 GitHub Pages 上顯示香港 7 天颱風與天氣風險。

- 0–5 天：確定性預報（多源共識）
- 6–7 天：風險/機率區間（低信賴度）
- 免費架構：GitHub Actions + Pages + JSON + Leaflet/D3 前端
