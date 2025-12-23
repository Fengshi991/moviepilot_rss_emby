import os
import time
import json
import random
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree
from email.utils import format_datetime
from datetime import datetime, timezone

_SESSION = None

def smart_input(prompt_text: str, default: str = "") -> str:
    """
    优先使用 prompt_toolkit（方向键编辑、历史记录等），否则回退到 readline+input。
    """
    global _SESSION

    # 方案 A：prompt_toolkit（最稳）
    try:
        from prompt_toolkit import PromptSession  # noqa
        if _SESSION is None:
            _SESSION = PromptSession()
        # prompt_toolkit 支持 default 参数作为默认输入 [web:37]
        return _SESSION.prompt(prompt_text, default=default).strip()
    except Exception:
        pass

    # 方案 B：readline（有些 Python 环境可用）
    try:
        import readline  # noqa: F401
    except Exception:
        pass

    # 最后回退：原生 input
    s = input(prompt_text)
    return s.strip() if s else ""


# ========================= 基本配置 =========================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    # 如需要登录态/更稳，可把浏览器 Cookie 整行复制进来
    # "Cookie": "xxx=yyy; ...",
}

OUTPUT_ROOT_DIR = "rss_files"
CACHE_ROOT_DIR = "cache_doulist"
FILTERED_ROOT_DIR = "rss_filtered"


# ========================= 工具函数 =========================

def safe_mkdir(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path)


def fetch_page(url: str, retries: int = 10, delay: int = 10) -> str:
    """
    请求页面，如果失败会进行重试，最多重试 retries 次，每次重试之间等待 delay 秒。
    如果重试多次仍失败，会提示用户输入继续或中断。
    """
    attempt = 0
    while attempt < retries:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            attempt += 1
            print(f"请求失败，第 {attempt} 次重试... 错误信息: {e}")
            if attempt < retries:
                print(f"等待 {delay} 秒后重试...")
                time.sleep(delay)
            else:
                user_input = smart_input("重试已达上限，是否继续尝试？（y/n）：", default="n")
                if user_input.lower() == "y":
                    return fetch_page(url, retries=retries, delay=delay)
                print("程序中断。")
                raise SystemExit(1)


def parse_page(html: str) -> Tuple[List[Dict], BeautifulSoup]:
    """
    返回当前页所有条目：
    每条是 dict: {title, link, year, director, cast, genre, country}
    """
    # lxml 不存在时回退到 html.parser
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    items: List[Dict] = []

    for block in soup.select(".doulist-item"):
        title_a = block.select_one(".title a")
        abstract = block.select_one(".abstract")
        if not title_a or not abstract:
            continue

        full_title = title_a.get_text(strip=True)
        link = (title_a.get("href") or "").strip()

        info = {"director": "", "cast": "", "genre": "", "country": "", "year": ""}

        lines = abstract.get_text("\n", strip=True).split("\n")
        for line in lines:
            line = line.strip()
            if line.startswith("导演:"):
                info["director"] = line.replace("导演:", "", 1).strip()
            elif line.startswith("主演:"):
                info["cast"] = line.replace("主演:", "", 1).strip()
            elif line.startswith("类型:"):
                info["genre"] = line.replace("类型:", "", 1).strip()
            elif line.startswith("制片国家/地区:"):
                info["country"] = line.replace("制片国家/地区:", "", 1).strip()
            elif line.startswith("年份:"):
                info["year"] = line.replace("年份:", "", 1).strip()

        items.append({"title": full_title, "link": link, **info})

    return items, soup


def find_next_page(soup: BeautifulSoup) -> Optional[str]:
    """
    找到“后页”链接，找不到则返回 None
    """
    next_a = soup.select_one(".paginator .next a")
    if not next_a:
        return None
    href = next_a.get("href")
    return href.strip() if href else None


def year_to_pubdate(year_str: str) -> str:
    """
    把“2004”这样的年份转成 RSS pubDate 使用的日期字符串，
    这里统一虚构为该年 1 月 1 日 00:00:00 UTC。
    """
    year_str = (year_str or "").strip()
    if not year_str.isdigit():
        return ""
    y = int(year_str)
    dt = datetime(y, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return format_datetime(dt)


# ========================= 去重与年份过滤 =========================

def normalize_year(year_str: str) -> Optional[int]:
    year_str = (year_str or "").strip()
    if not year_str.isdigit():
        return None
    return int(year_str)


def deduplicate_items(items: List[Dict], mode: str = "title_year") -> List[Dict]:
    """
    mode:
      - title_year: (title, year)
      - title_link: (title, link)
      - link: link
    保留第一条出现的记录。
    """
    seen = set()
    result = []

    for m in items:
        title = (m.get("title") or "").strip()
        year = (m.get("year") or "").strip()
        link = (m.get("link") or "").strip()

        if mode == "title_link":
            key = (title, link)
        elif mode == "link":
            key = link
        else:
            key = (title, year)

        if key in seen:
            continue
        seen.add(key)
        result.append(m)

    return result


def filter_items_by_year(
    items: List[Dict],
    min_year: Optional[int] = None,
    max_year: Optional[int] = None
) -> List[Dict]:
    """
    min_year: 只保留 year >= min_year
    max_year: 只保留 year <= max_year
    年份无法解析 -> 丢弃（避免脏数据混入过滤结果）
    """
    result = []
    for m in items:
        y = normalize_year(m.get("year", ""))
        if y is None:
            continue
        if min_year is not None and y < min_year:
            continue
        if max_year is not None and y > max_year:
            continue
        result.append(m)
    return result


# ========================= RSS 生成 =========================

def build_rss(
    items: List[Dict],
    start_url: str,
    output_file: str,
    title: str = "",
    description: str = ""
):
    safe_mkdir(os.path.dirname(output_file))

    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = title or "豆瓣豆列 - RSS 抓取"
    SubElement(channel, "link").text = start_url
    SubElement(channel, "description").text = description or "从多个豆瓣豆列抓取的电影列表"

    for m in items:
        item_el = SubElement(channel, "item")

        year = (m.get("year") or "").strip()
        title_text = (m.get("title") or "").strip()
        if year:
            title_text = f"{title_text} ({year})"

        SubElement(item_el, "title").text = title_text
        SubElement(item_el, "link").text = (m.get("link") or "").strip()

        pubdate = year_to_pubdate(year)
        if pubdate:
            SubElement(item_el, "pubDate").text = pubdate

        desc_parts = []
        if m.get("director"):
            desc_parts.append(f"导演: {m['director']}")
        if m.get("cast"):
            desc_parts.append(f"主演: {m['cast']}")
        if m.get("genre"):
            desc_parts.append(f"类型: {m['genre']}")
        if m.get("country"):
            desc_parts.append(f"制片国家/地区: {m['country']}")
        if year:
            desc_parts.append(f"年份: {year}")

        SubElement(item_el, "description").text = " | ".join(desc_parts)

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"RSS 已写入 {output_file}")


# ========================= 缓存 =========================

def extract_doulist_id(url: str) -> str:
    parts = url.strip().strip("/").split("/")
    return parts[-1] if parts else "unknown"


def cache_file_path(cache_dir: str, doulist_id: str) -> str:
    safe_mkdir(cache_dir)
    return os.path.join(cache_dir, f"doulist_{doulist_id}.json")


def load_cache(cache_dir: str, doulist_id: str) -> List[Dict]:
    path = cache_file_path(cache_dir, doulist_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_cache(cache_dir: str, doulist_id: str, items: List[Dict]):
    path = cache_file_path(cache_dir, doulist_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


# ========================= 抓取单豆列 =========================

def crawl_single_doulist(
    start_url: str,
    start_page: int = 1,
    output_dir: str = OUTPUT_ROOT_DIR,
    cache_dir: str = CACHE_ROOT_DIR,
    random_delay_range: Tuple[int, int] = (5, 10),
) -> List[Dict]:
    doulist_id = extract_doulist_id(start_url)
    print(f"=== 开始抓取豆列 {doulist_id}，起始页 {start_page} ===")

    all_items = load_cache(cache_dir, doulist_id)
    print(f"缓存中已有 {len(all_items)} 条记录")

    url = f"{start_url}?start={start_page - 1}"
    page = start_page

    while True:
        print(f"抓取豆列 {doulist_id} 第 {page} 页: {url}")
        html = fetch_page(url)
        page_items, soup = parse_page(html)
        print(f"本页抓到 {len(page_items)} 条")

        if not page_items:
            break

        all_items.extend(page_items)

        # 每页落盘缓存 + 写进度 RSS
        save_cache(cache_dir, doulist_id, all_items)

        doulist_output_dir = os.path.join(output_dir, doulist_id)
        safe_mkdir(doulist_output_dir)
        rss_path = os.path.join(doulist_output_dir, f"doulist_{doulist_id}_progress.xml")
        build_rss(
            all_items,
            start_url=start_url,
            output_file=rss_path,
            title=f"豆瓣豆列 {doulist_id} 实时进度",
            description=f"豆列 {start_url} 抓取进度 RSS（未去重、未过滤）"
        )

        next_url = find_next_page(soup)
        if not next_url:
            print(f"豆列 {doulist_id} 已无后续页面，停止抓取。")
            break

        delay_time = random.randint(*random_delay_range)
        print(f"等待 {delay_time} 秒再抓下一页...")
        for i in range(delay_time, 0, -1):
            print(f"  剩余 {i} 秒...")
            time.sleep(1)

        url = next_url
        page += 1

    print(f"豆列 {doulist_id} 共抓到 {len(all_items)} 条（未去重、未过滤）。")
    return all_items


# ========================= 多豆列合并流程 =========================

def crawl_multiple_doulists(
    urls: List[str],
    start_page: int = 1,
    dedup_mode: str = "title_year",
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    output_root: str = OUTPUT_ROOT_DIR,
    cache_root: str = CACHE_ROOT_DIR,
    filtered_root: str = FILTERED_ROOT_DIR,
):
    safe_mkdir(output_root)
    safe_mkdir(cache_root)
    safe_mkdir(filtered_root)

    all_items_raw: List[Dict] = []

    for u in urls:
        if not u.strip():
            continue
        items = crawl_single_doulist(
            start_url=u.strip(),
            start_page=start_page,
            output_dir=output_root,
            cache_dir=cache_root,
        )
        all_items_raw.extend(items)

    print(f"所有豆列合并后，共有 {len(all_items_raw)} 条（未去重）。")

    all_items_dedup = deduplicate_items(all_items_raw, mode=dedup_mode)
    print(f"去重后剩余 {len(all_items_dedup)} 条。")

    merged_rss_path = os.path.join(output_root, "merged_all_doulists.xml")
    build_rss(
        all_items_dedup,
        start_url=";".join(urls),
        output_file=merged_rss_path,
        title="多个豆瓣豆列合并（去重后全量）",
        description="从多个豆列抓取并合并的电影列表，已去重"
    )

    if min_year is not None or max_year is not None:
        filtered_items = filter_items_by_year(all_items_dedup, min_year=min_year, max_year=max_year)
        print(f"按年份过滤后剩余 {len(filtered_items)} 条。")

        desc_parts = []
        if min_year is not None:
            desc_parts.append(f"年份 >= {min_year}")
        if max_year is not None:
            desc_parts.append(f"年份 <= {max_year}")
        desc_text = "，".join(desc_parts) if desc_parts else "无年份过滤"

        filtered_path = os.path.join(filtered_root, "merged_filtered.xml")
        build_rss(
            filtered_items,
            start_url=";".join(urls),
            output_file=filtered_path,
            title="多个豆瓣豆列合并（按年份过滤）",
            description=f"（{desc_text}）"
        )


# ========================= 交互入口 =========================

def main():
    print("=== 多豆瓣豆列爬取 → RSS 生成工具 ===")
    print("说明：")
    print("1）支持一次输入多个豆列 URL（用英文逗号 , 分隔）；")
    print("2）支持全局去重（片名+年份 / 片名+链接 / 链接）；")
    print("3）支持只保留大于等于某年，或只保留小于等于某年的影片；")
    print("4）结果会输出到 rss_files/ 和 rss_filtered/ 目录中。")
    print("-" * 60)

    urls_str = smart_input("请输入豆列 URL，可多个，用英文逗号 , 分隔：\n> ")
    if not urls_str:
        print("未输入 URL，程序结束。")
        return
    url_list = [u.strip() for u in urls_str.split(",") if u.strip()]

    start_page_str = smart_input("请输入起始页数（默认 1）：", default="1")
    start_page = int(start_page_str) if start_page_str.isdigit() else 1

    print("去重方式：")
    print("  1）title_year（默认）使用 片名+年份 去重")
    print("  2）title_link           使用 片名+链接 去重")
    print("  3）link                 使用 链接 去重")
    dedup_choice = smart_input("请选择去重方式（输入 1/2/3，默认 1）：", default="1")
    dedup_mode = {"1": "title_year", "2": "title_link", "3": "link"}.get(dedup_choice, "title_year")

    print("年份过滤模式：")
    print("  0）不过滤（默认）")
    print("  1）只保留 >= 某年 的影片")
    print("  2）只保留 <= 某年 的影片")
    year_mode = smart_input("请选择年份过滤模式（输入 0/1/2，默认 0）：", default="0")

    min_year = None
    max_year = None
    if year_mode == "1":
        y_str = smart_input("请输入最小年份，例如 2000：")
        if y_str.isdigit():
            min_year = int(y_str)
    elif year_mode == "2":
        y_str = smart_input("请输入最大年份，例如 2010：")
        if y_str.isdigit():
            max_year = int(y_str)

    crawl_multiple_doulists(
        urls=url_list,
        start_page=start_page,
        dedup_mode=dedup_mode,
        min_year=min_year,
        max_year=max_year,
        output_root=OUTPUT_ROOT_DIR,
        cache_root=CACHE_ROOT_DIR,
        filtered_root=FILTERED_ROOT_DIR,
    )
    print("全部任务完成。")


if __name__ == "__main__":
    main()
