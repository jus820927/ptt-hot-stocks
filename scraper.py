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
# ======================================================================
_session = requests.Session()
_session.headers.update(HEADERS)
_session.cookies.update(COOKIES)


def fetch(url: str) -> Optional[str]:
    """GET 一個網址，失敗時重試，全部失敗回傳 None 並印出警告（不拋例外中止整體流程）。"""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = _session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
                return resp.text
            print(f"  [警告] {url} 回傳 HTTP {resp.status_code}（第 {attempt}/{MAX_RETRY} 次）",
                  file=sys.stderr)
        except requests.RequestException as exc:
            print(f"  [警告] {url} 請求失敗：{exc}（第 {attempt}/{MAX_RETRY} 次）", file=sys.stderr)
        time.sleep(1.5 * attempt)
    return None


# ======================================================================
# 看板列表頁：蒐集候選文章連結
# ======================================================================
def parse_index_page(html: str) -> tuple[list[dict], Optional[str]]:
    """
    解析單一看板列表頁。

    Returns
    -------
    (entries, prev_url)
        entries：[{"url": 文章網址, "date": "M/DD" 字串}, ...]
        已被刪除的文章沒有連結（div.title 內只有文字沒有 <a>），直接跳過，
        這是資料本身就不存在，不是抓取失敗，不視為錯誤。
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    for div in soup.select("div.r-ent"):
        a = div.select_one("div.title a")
        if not a or not a.get("href"):
            continue
        date_div = div.select_one("div.meta div.date")
        date_text = date_div.get_text(strip=True) if date_div else ""
        entries.append({"url": BASE + a["href"], "date": date_text})

    prev_url = None
    for a in soup.select("div.btn-group-paging a"):
        if "上頁" in a.get_text():
            href = a.get("href")
            prev_url = BASE + href if href else None
    return entries, prev_url


def collect_recent_article_urls(now: datetime) -> list[str]:
    """
    從看板最新頁往回翻頁，蒐集「日期落在今天或昨天」的候選文章連結。

    精確的 24 小時邊界判斷交給逐篇文章的完整時間戳記處理（見 parse_article）；
    這裡的日期比對只用來決定「要不要繼續往回翻頁」，屬於效率考量，
    寧可稍微多抓一點候選也不要漏抓。
    """
    today_str = f"{now.month}/{now.day:02d}"
    yesterday = now - timedelta(days=1)
    yesterday_str = f"{yesterday.month}/{yesterday.day:02d}"
    valid_dates = {today_str, yesterday_str}

    urls: list[str] = []
    url = BOARD_URL
    for page_i in range(MAX_PAGES):
        html = fetch(url)
        if html is None:
            print(f"[錯誤] 無法讀取看板列表頁：{url}", file=sys.stderr)
            break

        entries, prev_url = parse_index_page(html)
        urls.extend(e["url"] for e in entries)
        in_window = sum(1 for e in entries if e["date"] in valid_dates)
        print(f"  第 {page_i + 1} 頁：{len(entries)} 篇候選，日期落在區間內 {in_window} 篇",
              file=sys.stderr)

        # 第一頁（index.html）可能混有置底公告（日期可能很舊），
        # 因此只有第二頁以後才用「本頁完全沒有落在區間內的文章」當作停止條件。
        if page_i > 0 and in_window == 0:
            break
        if prev_url is None:
            break
        url = prev_url

    return urls


# ======================================================================
# 文章頁：解析內文、時間、推噓文
# ======================================================================
def parse_article(url: str, cutoff: datetime) -> Optional[dict]:
    """
    解析單篇文章。

    Returns
    -------
    None            讀取失敗或格式無法解析（已印出警告）
    {"too_old": True}   文章時間早於 cutoff，呼叫端應略過但不視為錯誤
    {...}           正常結果
    """
    html = fetch(url)
    if html is None:
        return None

    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("#main-content")
    if main is None:
        print(f"  [警告] 找不到內文區塊（版面可能已變更），略過：{url}", file=sys.stderr)
        return None

    # --- 解析 meta 欄位（作者／看板／標題／時間）---
    meta: dict[str, str] = {}
    for line in main.select("div.article-metaline, div.article-metaline-right"):
        tag = line.select_one("span.article-meta-tag")
        val = line.select_one("span.article-meta-value")
        if tag and val:
            meta[tag.get_text(strip=True)] = val.get_text(strip=True)

    time_str = meta.get("時間", "").strip()
    try:
        dt_naive = datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        print(f"  [警告] 時間格式無法解析（{time_str!r}），略過：{url}", file=sys.stderr)
        return None
    published_at = dt_naive.replace(tzinfo=TZ)

    if published_at < cutoff:
        return {"too_old": True}

    title = meta.get("標題", "")

    # --- 推噓文計數（必須在移除 push 區塊之前數）---
    push_count = 0
    boo_count = 0
    for push_div in main.select("div.push"):
        tag_span = push_div.select_one("span.push-tag")
        tag_text = tag_span.get_text(strip=True) if tag_span else ""
        if tag_text.startswith("推"):
            push_count += 1
        elif tag_text.startswith("噓"):
            boo_count += 1
        # 「→」為註解／箭頭，不計入推噓分數

    # --- 取出正文（移除 meta 區塊與推文區塊後剩下的文字）---
    for tag in main.select("div.article-metaline, div.article-metaline-right, div.push"):
        tag.decompose()
    body_text = main.get_text("\n", strip=True)

    return {
        "url": url,
        "title": title,
        "published_at": published_at.isoformat(),
        "push_count": push_count,
        "boo_count": boo_count,
        "text_for_codes": f"{title}\n{body_text}",
    }


# ======================================================================
# 主流程
# ======================================================================
def main() -> int:
    now = datetime.now(TZ)
    cutoff = now - timedelta(hours=WINDOW_HOURS)
    year_blacklist = build_year_blacklist(now)

    print(f"執行時間：{now.isoformat()}", file=sys.stderr)
    print(f"統計區間：{cutoff.isoformat()} ~ {now.isoformat()}", file=sys.stderr)

    print("步驟 1/2：蒐集候選文章連結…", file=sys.stderr)
    urls = collect_recent_article_urls(now)
    urls = list(dict.fromkeys(urls))  # 去重，保留原順序
    print(f"候選文章共 {len(urls)} 篇（含可能已超出區間或已被刪除者）", file=sys.stderr)

    if not urls:
        print("[錯誤] 完全抓不到候選文章連結，可能是 PTT 網站結構已變更或連線失敗。"
              "不產出任何檔案，維持 Pages 上次成功的結果。", file=sys.stderr)
        return 1

    print("步驟 2/2：逐篇讀取內文與推噓文…", file=sys.stderr)
    stats: dict[str, dict] = {}
    fetched = 0
    skipped_old = 0
    skipped_error = 0

    for i, url in enumerate(urls, start=1):
        if i % 20 == 0:
            print(f"  進度 {i}/{len(urls)}…", file=sys.stderr)

        result = parse_article(url, cutoff)
        if result is None:
            skipped_error += 1
            continue
        if result.get("too_old"):
            skipped_old += 1
            continue

        fetched += 1
        codes = extract_codes(result["text_for_codes"], year_blacklist)
        for code in codes:
            s = stats.setdefault(code, {"article_count": 0, "push_count": 0, "boo_count": 0})
            s["article_count"] += 1
            s["push_count"] += result["push_count"]
            s["boo_count"] += result["boo_count"]

    print(
        f"實際納入統計 {fetched} 篇｜超出 24 小時區間 {skipped_old} 篇｜讀取失敗 {skipped_error} 篇",
        file=sys.stderr,
    )

    if fetched == 0:
        print("[錯誤] 24 小時內沒有任何文章可統計，不覆蓋舊的 JSON 檔"
              "（避免用空結果蓋掉正常資料）。", file=sys.stderr)
        return 1

    total = fetched + skipped_error
    if total and skipped_error / total > 0.3:
        print(f"[警告] 讀取失敗比例偏高（{skipped_error}/{total}），"
              f"本次結果的覆蓋率可能不足，但仍會產出結果。", file=sys.stderr)

    # --- 排序與輸出 ---
    def score_of(item: tuple[str, dict]) -> int:
        s = item[1]
        return s["article_count"] * 3 + (s["push_count"] - s["boo_count"])

    ranked = sorted(stats.items(), key=score_of, reverse=True)[:TOP_N]

    output = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "window_hours": WINDOW_HOURS,
        "articles_scanned": fetched,
        "articles_skipped_error": skipped_error,
        "stocks": [
            {
                "rank": i + 1,
                "stock_id": code,
                "articles": s["article_count"],
                "push_count": s["push_count"],
                "boo_count": s["boo_count"],
                "net_pushes": s["push_count"] - s["boo_count"],
                "score": s["article_count"] * 3 + (s["push_count"] - s["boo_count"]),
            }
            for i, (code, s) in enumerate(ranked)
        ],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已輸出：{OUTPUT_PATH}（共 {len(output['stocks'])} 檔股票）", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
