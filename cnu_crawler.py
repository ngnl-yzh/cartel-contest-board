"""
전남대학교 CNU 보드 크롤러
대상 사이트:
  - 전남대 장학공지  https://www.jnu.ac.kr/WebApp/web/HOM/COM/Board/board.aspx?boardID=5&cate=8
  - 전남대 취업정보  https://www.jnu.ac.kr/WebApp/web/HOM/COM/Board/board.aspx?boardID=5&cate=7
  - 전남대 공모전    https://www.jnu.ac.kr/WebApp/web/HOM/COM/Board/board.aspx?boardID=5&cate=15
  - 취업정보포털     https://capd.jnu.ac.kr/
  - SW중심사업단     https://sojoong.kr/join/education/
  - AI혁신융합사업단 https://www.aicoss.kr/www/notice/
크롤 조건: 이번 연도 게시 + 마감 미경과
"""
import asyncio
import os
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

try:
    import lxml  # noqa
    _PARSER = "lxml"
except ImportError:
    _PARSER = "html.parser"

from crawler import HEADERS, _norm

_YEAR = date.today().year


# ─── 공통 헬퍼 ────────────────────────────────────────────────────────────────

def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, _PARSER)


def _cnu_item(source: str, source_label: str, board_type: str,
              title: str, link: str,
              posted_date: str = "", deadline: Optional[str] = None) -> dict:
    return {
        "source":       source,
        "source_label": source_label,
        "board_type":   board_type,
        "title":        title,
        "link":         link,
        "posted_date":  posted_date,
        "deadline":     deadline,
        "summary":      "",
    }


def _is_this_year(date_str: str) -> bool:
    """YYYY-MM-DD 또는 YYYY.MM.DD 형식에서 올해인지 확인"""
    if not date_str:
        return True  # 날짜 없으면 포함
    m = re.search(r"(\d{4})", date_str)
    if m:
        return int(m.group(1)) >= _YEAR
    return True


def _is_not_expired(deadline_str: Optional[str]) -> bool:
    """마감일이 오늘 이전이면 False"""
    if not deadline_str:
        return True
    try:
        dl = datetime.fromisoformat(deadline_str).date()
        return dl >= date.today()
    except Exception:
        return True


def _abs_url(href: str, base: str) -> str:
    if not href:
        return base
    if href.startswith("http"):
        return href
    return urljoin(base, href)


# ─── 전남대 통합게시판 (boardID=5, cate별) ────────────────────────────────────

async def _crawl_jnu_board(
    client: httpx.AsyncClient,
    cate: int,
    source: str,
    source_label: str,
    board_type: str,
) -> list:
    """
    전남대 WebApp 게시판 파서.
    URL: /WebApp/web/HOM/COM/Board/board.aspx?boardID=5&cate={cate}
    HTML 구조: <table class="tbl_style01"> / <table class="board_list">
    각 행: [번호] [제목(링크)] [작성자] [날짜] [조회]
    """
    base = "https://www.jnu.ac.kr"
    url  = f"{base}/WebApp/web/HOM/COM/Board/board.aspx?boardID=5&cate={cate}"
    results: list = []
    try:
        r = await client.get(url)
        if r.status_code >= 400:
            return [{"_error": f"{source_label} HTTP {r.status_code}"}]

        soup = _soup(r.text)

        # 전남대 게시판 tbody tr 선택 — 다양한 table 클래스 시도
        rows = (
            soup.select("table.tbl_style01 tbody tr") or
            soup.select("table.board_list tbody tr") or
            soup.select("table tbody tr") or
            []
        )

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            # 제목 컬럼: a 태그가 있는 첫 번째 td
            title_tag = None
            for col in cols:
                a = col.find("a", href=True)
                if a and a.get_text(strip=True):
                    title_tag = a
                    break
            if not title_tag:
                continue

            title = _norm(title_tag.get_text())
            if not title or len(title) < 2:
                continue

            href = title_tag["href"]
            link = _abs_url(href, base)

            # 날짜: 마지막 컬럼들에서 날짜 패턴만 추출
            posted_date = ""
            _d_re = re.compile(r"(\d{4}[.\-]\d{1,2}[.\-]\d{1,2})")
            for col in reversed(cols):
                txt = col.get_text(strip=True)
                m = _d_re.search(txt)
                if m:
                    posted_date = m.group(1)
                    break

            # 올해 게시물만 수집
            if not _is_this_year(posted_date):
                continue

            results.append(_cnu_item(source, source_label, board_type,
                                     title, link, posted_date))

        if not results and not any("_error" in r for r in results):
            results.append({"_error": f"{source_label}: 항목 0개 (HTML 구조 확인 필요)"})

    except Exception as e:
        results.append({"_error": f"{source_label} 예외: {type(e).__name__}: {e}"})

    return results


async def _crawl_jnu_scholarship(client: httpx.AsyncClient) -> list:
    return await _crawl_jnu_board(client, 8, "jnu_scholarship", "전남대 장학공지", "scholarship")

async def _crawl_jnu_job(client: httpx.AsyncClient) -> list:
    return await _crawl_jnu_board(client, 7, "jnu_job", "전남대 취업정보", "job_contest")

async def _crawl_jnu_contest(client: httpx.AsyncClient) -> list:
    return await _crawl_jnu_board(client, 15, "jnu_contest", "전남대 공모전", "job_contest")


# ─── 전남대 취업정보포털 capd.jnu.ac.kr ────────────────────────────────────────

async def _crawl_capd(client: httpx.AsyncClient) -> list:
    """
    CAPD는 별도 포털 — 메인/공고 페이지에서 취업·인턴 공고 목록 수집.
    """
    base = "https://capd.jnu.ac.kr"
    results: list = []
    try:
        r = await client.get(base + "/")
        if r.status_code >= 400:
            return [{"_error": f"취업정보포털 HTTP {r.status_code}"}]

        soup = _soup(r.text)

        # 공통 리스트 패턴들 시도
        anchors: list = []
        for sel in [
            "ul.board_list a", "ul.list a",
            ".notice_area a", ".job_list a", ".recruit_list a",
            "table tbody tr td a", ".board-wrap a",
        ]:
            anchors = soup.select(sel)
            if anchors:
                break

        # fallback: 모든 a 태그 중 텍스트 20자 이상
        if not anchors:
            anchors = [a for a in soup.find_all("a", href=True)
                       if len(a.get_text(strip=True)) >= 10]

        seen: set = set()
        for a in anchors[:30]:
            title = _norm(a.get_text())
            if not title or len(title) < 5:
                continue
            href  = a.get("href", "")
            link  = _abs_url(href, base)
            if link in seen:
                continue
            seen.add(link)
            results.append(_cnu_item("capd", "취업정보포털", "job_contest", title, link))

        if not results:
            results.append({"_error": "취업정보포털: 항목 없음 (HTML 구조 확인 필요)"})

    except Exception as e:
        results.append({"_error": f"취업정보포털 예외: {type(e).__name__}: {e}"})

    return results


# ─── SW중심사업단 sojoong.kr ───────────────────────────────────────────────────

async def _crawl_sojoong(client: httpx.AsyncClient) -> list:
    """
    sojoong.kr/join/education/ — 교육프로그램 목록.
    WordPress 계열 또는 자체 CMS.
    """
    url = "https://sojoong.kr/join/education/"
    results: list = []
    try:
        r = await client.get(url)
        if r.status_code >= 400:
            return [{"_error": f"SW중심사업단 HTTP {r.status_code}"}]

        soup = _soup(r.text)

        # 다양한 셀렉터 시도
        items: list = []
        for sel in [
            "article", ".program-item", ".edu-item", ".card",
            "table tbody tr", ".board_list li", ".post", ".entry-summary",
        ]:
            items = soup.select(sel)
            if items and any(it.find("a") for it in items):
                break

        # fallback: <a> 중 본문 링크
        if not items:
            items = [BeautifulSoup(f"<div>{a}</div>", _PARSER)
                     for a in soup.find_all("a", href=True)
                     if len(a.get_text(strip=True)) >= 8]

        seen: set = set()
        for item in items[:30]:
            a = item.find("a", href=True) if hasattr(item, "find") else item
            if not a:
                continue
            title = _norm(a.get_text())
            if not title or len(title) < 5:
                continue
            href  = a.get("href", "")
            link  = _abs_url(href, url)
            if link in seen or "sojoong.kr" not in urlparse(link).netloc:
                continue
            seen.add(link)

            # 기간 텍스트 찾기
            date_txt = ""
            for cls in ["date", "period", "term", "duration"]:
                d = item.find(class_=re.compile(cls, re.I))
                if d:
                    date_txt = d.get_text(strip=True)
                    break

            results.append(_cnu_item("sojoong", "SW중심사업단", "program",
                                     title, link, date_txt))

        if not results:
            results.append({"_error": "SW중심사업단: 항목 없음 (HTML 구조 확인 필요)"})

    except Exception as e:
        results.append({"_error": f"SW중심사업단 예외: {type(e).__name__}: {e}"})

    return results


# ─── AI혁신융합사업단 aicoss.kr ────────────────────────────────────────────────

async def _crawl_aicoss(client: httpx.AsyncClient) -> list:
    """
    aicoss.kr/www/notice/ — 공지사항 목록.
    """
    url = "https://www.aicoss.kr/www/notice/"
    results: list = []
    try:
        r = await client.get(url)
        if r.status_code >= 400:
            return [{"_error": f"AI혁신사업단 HTTP {r.status_code}"}]

        soup = _soup(r.text)

        rows: list = []
        for sel in [
            "table.board_list tbody tr", "table tbody tr",
            "ul.board_list li", "ul.notice li", ".board-item",
        ]:
            rows = soup.select(sel)
            if rows:
                break

        _DATE_RE = re.compile(r"(\d{4}[.\-]\d{1,2}[.\-]\d{1,2})")

        seen: set = set()
        for row in rows[:30]:
            a = row.find("a", href=True)
            if not a:
                continue
            title = _norm(a.get_text())
            if not title or len(title) < 3:
                continue
            href  = a.get("href", "")

            # javascript: 링크는 쓸모없음 — 상세 페이지 URL을 직접 구성
            if href.startswith("javascript"):
                m = re.search(r"\d+", href)
                if m:
                    href = f"https://www.aicoss.kr/www/notice/view/{m.group()}"
                else:
                    href = "https://www.aicoss.kr/www/notice/"
            link = _abs_url(href, "https://www.aicoss.kr")

            if link in seen:
                continue
            seen.add(link)

            # 날짜: 날짜 패턴만 추출 (전체 텍스트가 아닌 첫 번째 매치만)
            posted_date = ""
            for col in row.find_all(["td", "span", "div"]):
                txt = col.get_text(strip=True)
                m = _DATE_RE.search(txt)
                if m:
                    posted_date = m.group(1)
                    break

            if not _is_this_year(posted_date):
                continue

            results.append(_cnu_item("aicoss", "AI혁신융합사업단", "program",
                                     title, link, posted_date))

        if not results:
            results.append({"_error": "AI혁신사업단: 항목 없음 (HTML 구조 확인 필요)"})

    except Exception as e:
        results.append({"_error": f"AI혁신사업단 예외: {type(e).__name__}: {e}"})

    return results


# ─── GPT 요약 ─────────────────────────────────────────────────────────────────

async def cnu_gpt_summarize(title: str, url: str) -> str:
    """
    URL 페이지 내용을 GPT로 요약.
    공모전 크롤러의 _gpt_process_item과 같은 패턴이지만
    CNU 항목용으로 간소화 — 핵심 3줄 불릿 반환.
    """
    import openai

    try:
        # 1. 페이지 fetch
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=25, write=10, pool=5),
            follow_redirects=True,
            headers=HEADERS,
        ) as cli:
            r = await cli.get(url)

        page_soup = _soup(r.text)
        for tag in page_soup.select("nav, header, footer, script, style, .gnb, .lnb, #header, #footer, #nav"):
            tag.decompose()
        page_text = page_soup.get_text(separator="\n", strip=True)
        # 최대 3000자
        page_text = page_text[:3000]

        # 2. GPT 요약
        oai = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        resp = await oai.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{
                "role": "system",
                "content": "당신은 대학 공지사항·프로그램 안내를 간결하게 정리하는 도우미입니다.",
            }, {
                "role": "user",
                "content": (
                    f"제목: {title}\n\n{page_text}\n\n"
                    "위 내용에서 핵심 정보만 뽑아 아래 형식으로 3줄 이내 한국어로 요약하세요.\n"
                    "• [대상] ...\n• [기간/마감] ...\n• [혜택/내용] ..."
                ),
            }],
            max_tokens=200,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()

    except Exception as e:
        return f"(요약 실패: {type(e).__name__}: {e})"


# ─── 소스 등록 ────────────────────────────────────────────────────────────────

CNU_SOURCES: dict = {
    # key: (label, board_type, crawl_func)
    "jnu_scholarship": ("전남대 장학공지",    "scholarship", _crawl_jnu_scholarship),
    "jnu_job":         ("전남대 취업정보",    "job_contest", _crawl_jnu_job),
    "jnu_contest":     ("전남대 공모전",      "job_contest", _crawl_jnu_contest),
    "capd":            ("취업정보포털(CAPD)", "job_contest", _crawl_capd),
    "sojoong":         ("SW중심사업단",       "program",     _crawl_sojoong),
    "aicoss":          ("AI혁신융합사업단",   "program",     _crawl_aicoss),
}


async def crawl_cnu_all(sources: list = None) -> dict:
    """지정 소스(기본 전체) 크롤링 후 결과 반환"""
    valid = [s for s in (sources or list(CNU_SOURCES.keys())) if s in CNU_SOURCES]
    if not valid:
        return {"items": [], "errors": ["선택된 소스가 없습니다."], "counts": {}}

    try:
        async with httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            headers=HEADERS,
        ) as client:
            raw = await asyncio.gather(
                *[CNU_SOURCES[s][2](client) for s in valid],
                return_exceptions=True,
            )
    except Exception as e:
        return {"items": [], "errors": [f"크롤러 초기화 실패: {e}"], "counts": {}}

    items, errors, counts = [], [], {}
    for s, res in zip(valid, raw):
        if isinstance(res, Exception):
            errors.append(f"{s} 예외: {res}")
            counts[s] = 0
            continue
        errs = [r["_error"] for r in res if "_error" in r]
        good = [r for r in res if "_error" not in r]
        errors.extend(errs)
        items.extend(good)
        counts[CNU_SOURCES[s][0]] = len(good)

    return {"items": items, "errors": errors, "counts": counts}
