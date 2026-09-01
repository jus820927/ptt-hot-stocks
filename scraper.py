"""
========================================================================
PTT Stock 板個股熱度統計爬蟲
------------------------------------------------------------------------
資料流程：
  1. 從看板最新頁面開始往回翻頁，蒐集「發文時間在過去 24 小時內」的候選文章
  2. 逐篇讀取內文與推噓文，統計每檔股票代碼的：
       article_count（出現在幾篇文章，同篇重複出現只算一次）
       push_count / boo_count（那些文章的推文數／噓文數加總）
  3. 熱度分數 = article_count * 3 + (push_count - boo_count)
  4. 取分數最高前 30 檔，輸出 docs/hot_stocks.json

零容忍原則（重要）：
  * 完全抓不到候選文章、或 24 小時內納入統計的文章數為 0 → 直接以失敗結束
    （exit code 1），絕不輸出「假的空結果」蓋掉舊檔案。GitHub Actions 會因此
    顯示紅色失敗記號，Pages 上仍保留上一次成功的資料，不會被覆蓋。
  * 個股代碼一律用黑名單排除法過濾，不做任何白名單推測；黑名單以外的
    4 位數字一律視為候選代碼，即使實際上不是股票代碼也不做二次驗證
    （這是黑名單方法的已知取捨，見 README 說明）。
  * 任何一篇文章讀取失敗，只記錄警告並跳過該篇，不影響其他文章的統計。

已用真實 PTT 文章驗證過的關鍵細節：
  * 內文常見「代碼緊接公司名」寫法，例如「2609陽明」中間沒有空白。
    Python 的 \b（單字邊界）會把中文字視為文字字元，數字與緊鄰中文字之間
    不構成邊界，導致 \b\d{4}\b 抓不到這類寫法。因此改用
    (?<!\d)\d{4}(?!\d) —— 只要求前後不是「數字」，不管兩側是中文、
    英文或標點，同時仍可避免從 5 位數以上的長數字中截出錯誤的 4 位片段。
  * 文章時間格式固定為英文「Tue Sep 1 00:00:44 2026」（PTT 網頁的時間欄位
    不受看板語言影響，一律是這個格式），已用 strptime 對應驗證。
  * 推文標記固定為「推 」「噓 」「→ 」三種（含後面的全形空白），
    只有「推」「噓」列入計分，「→」是註解/箭頭不計入。
========================================================================
"""
from __future__ import annotations

import json
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ======================================================================
# 參數設定
# ======================================================================
BASE = "https://www.ptt.cc"
BOARD_URL = f"{BASE}/bbs/Stock/index.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}
# Stock 板本身不是滿 18 歲看板，理論上不需要這個 cookie，但保留無妨，
# 避免日後板規調整或誤判時整批被導去年齡驗證頁。
COOKIES = {"over18": "1"}

TZ = timezone(timedelta(hours=8))  # 台灣時間，PTT 網頁顯示的時間本身就是台灣時間
WINDOW_HOURS = 24
TOP_N = 30

REQUEST_TIMEOUT = 15
REQUEST_DELAY_RANGE = (0.6, 1.2)  # 對 PTT 客氣一點的隨機請求間隔（秒）
MAX_RETRY = 3
MAX_PAGES = 30  # 翻頁安全上限，正常情況（24小時內容量）用不到這麼多頁

OUTPUT_PATH = Path(__file__).resolve().parent / "docs" / "hot_stocks.json"


# ======================================================================
# 個股代碼過濾（黑名單排除法）
# ======================================================================
# 前後不是數字即可，不倚賴 \b（見檔頭說明：中文字會被視為文字字元，
# 導致「2609陽明」這種常見寫法抓不到）
STOCK_CODE_RE = re.compile(r"(?<!\d)\d{4}(?!\d)")


def build_year_blacklist(today: datetime) -> set[str]:
    """涵蓋前後幾年，避免年份被誤判成股票代碼（例如 2026、2027）。"""
    return {str(y) for y in range(today.year - 6, today.year + 3)}


# 常見「整數」寫法：指數點位、金額、百分比等情境很容易出現，通常不是股票代碼
ROUND_NUMBER_BLACKLIST = {f"{n}00" for n in range(10, 100)} | {
    "0000",
    "1111",
    "2222",
    "3333",
    "4444",
    "5555",
    "6666",
    "7777",
    "8888",
    "9999",
    "1234",
    "2345",
    "3456",
    "4567",
    "5678",
    "6789",
}

# 依實際觀察到的誤判持續補充在這裡即可（例如特定慣用縮寫、罰則條文編號等）
EXTRA_BLACKLIST: set[str] = set()


def is_valid_code(code: str, year_blacklist: set[str]) -> bool:
    if code in year_blacklist:
        return False
    if code in ROUND_NUMBER_BLACKLIST:
        return False
    if code in EXTRA_BLACKLIST:
        return False
    return True


def extract_codes(text: str, year_blacklist: set[str]) -> set[str]:
    """從文字中抽取通過黑名單過濾的候選股票代碼；同一篇文章內重複出現只回傳一次。"""
    found = set(STOCK_CODE_RE.findall(text))
    return {c for c in found if is_valid_code(c, year_blacklist)}


# ======================================================================
# 網路請求（含重試、節流、逾時）
# ==================================================================
