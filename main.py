import asyncio
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

# 한국 표준시 (UTC+9) — Railway 서버는 UTC이므로 명시적으로 변환
_KST = timezone(timedelta(hours=9))
def _now() -> datetime:
    """현재 KST 시각을 naive datetime으로 반환 (DB 저장용)"""
    return datetime.now(_KST).replace(tzinfo=None)

def _today() -> date:
    """현재 KST 날짜 반환"""
    return datetime.now(_KST).date()

from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from sqlalchemy import and_, case, func, or_
from sqlalchemy import update as _sa_update
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from ai_parser import parse_document_file, parse_image_file, parse_text
from crawler import CRAWL_SOURCES, JOB_SOURCES, crawl_all as _do_crawl_all, run_job_crawlers as _do_crawl_jobs
from cnu_crawler import CNU_SOURCES, crawl_cnu_all as _do_crawl_cnu, cnu_gpt_summarize as _cnu_summarize
from auth import create_token, verify_token
from database import SessionLocal, get_db, init_db
from member_auth import create_member_token, hash_password, verify_member_token, verify_password, verify_team_password
from models import (
    BOARDS, NOTICE_ONLY_BOARDS,
    AppSetting,
    ChatMessage, ChatRoom, ChatRoomMember,
    Comment, CommentLike,
    Competition, CompetitionScrap, CrawlSession, InviteCode, InviteCodeUseLog,
    JobCrawlSession, JobPosting, Member,
    DirectMessage, ExternalAchievement, Follow, Notification,
    Post, PostLike,
    CalendarEvent,
    GalleryPost, PersonalPost, PushSubscription, SiteBanner, Team, TeamCompetitionEntry, TeamKickRequest, TeamMember, TeamResult,
    CourseEntry, CoursePost, CourseFile,
    TimetableEntry,
    CnuItem,
)

app = FastAPI(title="공모전 보드")

# ── 파일 저장소 ────────────────────────────────────────────────────────────────
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

def _storage_upload(content: bytes, filename: str, content_type: str = "application/octet-stream") -> None:
    (UPLOAD_DIR / filename).write_bytes(content)

def _storage_delete(filename: str) -> None:
    if not filename:
        return
    try:
        (UPLOAD_DIR / filename).unlink(missing_ok=True)
    except OSError:
        pass

templates = Jinja2Templates(directory=BASE_DIR / "templates")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    accepts_html = "text/html" in request.headers.get("accept", "")
    if not accepts_html:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)
    detail = html.escape(exc.detail if isinstance(exc.detail, str) else "요청을 처리할 수 없습니다.")
    return HTMLResponse(
        f"""
        <!doctype html>
        <html lang="ko">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>오류 · 공모전 보드</title>
          <link rel="stylesheet" href="/static/css/style.css">
        </head>
        <body>
          <main class="auth-page">
            <section class="auth-panel">
              <p class="eyebrow">Error {exc.status_code}</p>
              <h1>요청을 처리하지 못했습니다.</h1>
              <p class="muted">{detail}</p>
              <div class="modal-actions">
                <a href="javascript:history.back()" class="btn btn-outline">이전으로</a>
                <a href="/" class="btn btn-primary">홈으로</a>
              </div>
            </section>
          </main>
        </body>
        </html>
        """,
        status_code=exc.status_code,
        headers=exc.headers,
    )


def _from_json(value) -> list:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def _optional_int(value, field_name: str) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} 값이 올바른 숫자가 아닙니다.") from exc


def _compact_text(value: str) -> str:
    return "".join((value or "").lower().split())


def _compact_column(column):
    return func.replace(func.replace(func.lower(column), " ", ""), "\t", "")


def _parse_expiry(valid_days: Optional[str], expires_at: Optional[str]) -> Optional[datetime]:
    if expires_at:
        try:
            return datetime.fromisoformat(expires_at)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="만료일 형식이 올바르지 않습니다.") from exc
    days = _optional_int(valid_days, "유효 기간")
    if days:
        return _now() + timedelta(days=days)
    return None


templates.env.filters["fromjson"] = _from_json


def _unique_filter(iterable):
    seen = []
    for x in iterable:
        if x not in seen:
            seen.append(x)
    return seen


templates.env.filters["unique"] = _unique_filter


import markdown as _md
from markupsafe import Markup

def _markdown_filter(text: str) -> str:
    """서버사이드 마크다운 → HTML 변환 (nl2br, tables, fenced_code 지원)"""
    if not text:
        return ""
    return _md.markdown(
        text,
        extensions=["nl2br", "tables", "fenced_code"],
    )

templates.env.filters["markdown"] = _markdown_filter

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin1234")
from crawler import CONTESTKOREA_CATS as _CONTESTKOREA_CATS
_DEFAULT_TAGS = list(_CONTESTKOREA_CATS)
TAGS = _DEFAULT_TAGS  # fallback (DB 접근 전 사용)
ROLES = ["기획", "개발", "디자인", "마케팅"]

# 중간관리자에게 부여할 수 있는 권한 목록 (중간관리자 임명 제외)
ADMIN_PERMISSIONS = [
    ("competitions", "공모전 관리"),
    ("crawl",        "크롤링 / URL 등록"),
    ("teams",        "팀 직접 편집"),
    ("members",      "회원 관리"),
    ("gallery",      "갤러리 관리"),
    ("calendar",     "캘린더 일정 관리"),   # 일정 추가·수정·삭제
    ("invites",      "초대코드 관리"),
    ("jobs",         "채용 관리"),
    ("settings",     "분야·태그 설정"),  # /admin/settings (공모전 분야 편집)
]


def _member_perms(member) -> list:
    """멤버의 permissions JSON 리스트 반환"""
    if not member:
        return []
    return _from_json(getattr(member, "permissions", None) or "[]")


def _has_perm(request, db, perm: str) -> bool:
    """관리자는 항상 True, 중간관리자는 permissions 목록에 perm이 있어야 True"""
    if _is_admin(request):
        return True
    cm = _current_member(request, db)
    if cm and cm.role == "sub_admin":
        return perm in _member_perms(cm)
    return False


def _get_tags(db: Session) -> list[str]:
    """AppSetting에서 분야 태그 목록을 로드. 설정 없으면 기본값 반환."""
    try:
        row = db.query(AppSetting).filter(AppSetting.key == "tags").first()
        if row and row.value:
            parsed = json.loads(row.value)
            if isinstance(parsed, list) and parsed:
                return parsed
    except Exception:
        pass
    return list(_DEFAULT_TAGS)
AWARD_RANKS = ["대상", "최우수상", "우수상", "장려상", "입선"]

# ── 프로덕션 분기 ──────────────────────────────────────────────────────────────
IS_PRODUCTION = os.getenv("RAILWAY_ENVIRONMENT") is not None or os.getenv("PRODUCTION", "").lower() == "true"

# ── CSRF 기본 구현 (함수 준비, samesite=lax 이미 적용 중) ─────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")
CSRF_SECRET = os.getenv("CSRF_SECRET", SECRET_KEY + "_csrf")
_EASTER_COOKIE = hmac.new((SECRET_KEY + ":easter").encode(), b"cartel-easter", hashlib.sha256).hexdigest()[:32]


def _generate_csrf(session_token: str) -> str:
    """세션 토큰 기반 CSRF 토큰 생성"""
    return hmac.new(CSRF_SECRET.encode(), session_token.encode(), hashlib.sha256).hexdigest()[:32]


def _validate_csrf(request: Request, form_token: str) -> bool:
    """폼에서 전달된 CSRF 토큰 검증"""
    session_token = request.cookies.get("admin_token") or request.cookies.get("member_token") or ""
    if not session_token:
        return False
    expected = _generate_csrf(session_token)
    return hmac.compare_digest(expected, form_token or "")


# ── 로그인 실패 카운터 (IP → (횟수, 마지막 실패 시각)) ──────────────────────────
_admin_fail_count: dict = {}   # 관리자
_member_fail_count: dict = {}  # 회원

_LOGIN_MAX_FAIL = 10          # 최대 허용 실패 횟수
_LOGIN_LOCKOUT  = 300         # 잠금 시간(초)
_FAIL_TTL       = 3600        # 오래된 항목 청소 기준(초)


def _prune_fail_counter(counter: dict) -> None:
    """1시간 이상 된 항목 제거 (메모리 누수 방지)"""
    now = _now()
    stale = [ip for ip, (_, last) in counter.items()
             if (now - last).total_seconds() > _FAIL_TTL]
    for ip in stale:
        del counter[ip]


def _is_locked(counter: dict, ip: str, max_fail: int = _LOGIN_MAX_FAIL) -> bool:
    count, last = counter.get(ip, (0, datetime.min))
    return count >= max_fail and (_now() - last).total_seconds() < _LOGIN_LOCKOUT


def _record_fail(counter: dict, ip: str) -> None:
    count, _ = counter.get(ip, (0, datetime.min))
    counter[ip] = (count + 1, _now())
    _prune_fail_counter(counter)


import logging as _logging
_log = _logging.getLogger("uvicorn.error")


@app.on_event("startup")
def startup():
    # 보안 기본값 경고
    if os.getenv("SECRET_KEY", "change-me-in-production") == "change-me-in-production":
        _log.warning("[보안] SECRET_KEY가 기본값입니다. 환경변수로 강력한 랜덤 키를 설정하세요.")
    if os.getenv("ADMIN_PASSWORD", "admin1234") == "admin1234":
        _log.warning("[보안] ADMIN_PASSWORD가 기본값(admin1234)입니다. 즉시 변경하세요.")

    init_db()
    # review_dates 컬럼 마이그레이션: review_1_date/review_2_date 데이터를 review_dates JSON으로 이전
    try:
        db = SessionLocal()
        for comp in db.query(Competition).filter(
            (Competition.review_dates == None) | (Competition.review_dates == "[]")
        ).all():
            old = []
            if comp.review_1_date:
                old.append({"label": "1차 심사", "date": comp.review_1_date.isoformat()})
            if comp.review_2_date:
                old.append({"label": "2차 심사", "date": comp.review_2_date.isoformat()})
            if old:
                comp.review_dates = json.dumps(old, ensure_ascii=False)
        db.commit()
        db.close()
    except Exception:
        pass

    # ── VAPID 키 초기화 ──────────────────────────────────────────────────────
    _init_vapid_keys()

    # ── APScheduler: 생일/행사 알림 ──────────────────────────────────────────
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        import pytz
        _scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Seoul"))
        # 매일 오전 8시 KST 에 실행
        _scheduler.add_job(_scheduled_push_daily, "cron", hour=8, minute=0, id="daily_push")
        _scheduler.start()
        _log.info("APScheduler 시작: 매일 08:00 KST 푸시 알림 스케줄 등록")
    except Exception as _e:
        _log.warning("APScheduler 시작 실패: %s", _e)


# ── VAPID 키 관리 ──────────────────────────────────────────────────────────────

_vapid_private_key: str = ""
_vapid_public_key:  str = ""


def _init_vapid_keys():
    """VAPID 키를 환경변수 또는 DB(AppSetting)에서 로드. 없으면 생성 후 DB에 저장."""
    global _vapid_private_key, _vapid_public_key
    # 1순위: 환경변수
    if os.getenv("VAPID_PRIVATE_KEY") and os.getenv("VAPID_PUBLIC_KEY"):
        _vapid_private_key = os.getenv("VAPID_PRIVATE_KEY")
        _vapid_public_key  = os.getenv("VAPID_PUBLIC_KEY")
        return
    # 2순위: DB
    try:
        db = SessionLocal()
        priv = db.query(AppSetting).filter(AppSetting.key == "vapid_private_key").first()
        pub  = db.query(AppSetting).filter(AppSetting.key == "vapid_public_key").first()
        if priv and pub:
            _vapid_private_key = priv.value
            _vapid_public_key  = pub.value
            db.close(); return
        # 생성
        from py_vapid import Vapid
        import base64
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
        v = Vapid()
        v.generate_keys()
        _vapid_private_key = v.private_pem().decode()
        pub_bytes = v.public_key.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
        _vapid_public_key = base64.urlsafe_b64encode(pub_bytes).decode().rstrip("=")
        db.add(AppSetting(key="vapid_private_key", value=_vapid_private_key))
        db.add(AppSetting(key="vapid_public_key",  value=_vapid_public_key))
        db.commit(); db.close()
        _log.info("VAPID 키 신규 생성 완료")
    except Exception as _e:
        _log.warning("VAPID 키 초기화 실패: %s", _e)


# ── 푸시 전송 헬퍼 ────────────────────────────────────────────────────────────

def _send_push_to_subscription(sub: "PushSubscription", title: str, body: str,
                                url: str = "/", tag: str = "cartel") -> bool:
    """단일 구독에 푸시 전송. 실패(구독 만료 등) 시 False 반환."""
    if not _vapid_private_key:
        return False
    try:
        from pywebpush import webpush, WebPushException
        webpush(
            subscription_info={
                "endpoint": sub.endpoint,
                "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
            },
            data=json.dumps({"title": title, "body": body, "url": url, "tag": tag},
                            ensure_ascii=False),
            vapid_private_key=_vapid_private_key,
            vapid_claims={"sub": "mailto:admin@cartel.kr"},
        )
        return True
    except Exception as _e:
        _log.debug("푸시 전송 실패 (endpoint=%s…): %s", sub.endpoint[:40], _e)
        return False


def _broadcast_push(db, title: str, body: str, url: str = "/",
                    tag: str = "cartel", member_ids: list = None):
    """모든(또는 지정) 구독자에게 푸시 전송. 만료된 구독은 자동 삭제."""
    query = db.query(PushSubscription)
    if member_ids is not None:
        query = query.filter(PushSubscription.member_id.in_(member_ids))
    subs = query.all()
    expired = []
    for sub in subs:
        ok = _send_push_to_subscription(sub, title, body, url, tag)
        if not ok:
            expired.append(sub.id)
    if expired:
        db.query(PushSubscription).filter(PushSubscription.id.in_(expired)).delete(synchronize_session=False)
        db.commit()
    return len(subs) - len(expired)


# ── 스케줄 푸시 (매일 08:00 KST) ─────────────────────────────────────────────

def _scheduled_push_daily():
    """생일 알림 + 캘린더 행사 알림 (당일/전날)"""
    try:
        db = SessionLocal()
        today = _today()

        # ── 중복 방지: 오늘 이미 실행했으면 스킵 ────────────────────────────
        key = f"push_daily_{today.isoformat()}"
        already = db.query(AppSetting).filter(AppSetting.key == key).first()
        if already:
            db.close(); return
        db.add(AppSetting(key=key, value="1"))
        db.commit()

        # ── 생일 알림 ────────────────────────────────────────────────────────
        mm_dd = today.strftime("%m-%d")
        bd_members = db.query(Member).filter(Member.birthday == mm_dd).all()
        for m in bd_members:
            _broadcast_push(db,
                title="🎂 오늘은 생일이에요!",
                body=f"오늘은 {m.activity_name}님의 생일입니다! 생일 축하해요 🎉",
                url="/calendar",
                tag="birthday",
            )

        # ── 캘린더 행사 — 당일 ───────────────────────────────────────────────
        events_today = db.query(CalendarEvent).filter(CalendarEvent.start_date == today).all()
        for ev in events_today:
            _broadcast_push(db,
                title=f"📅 오늘 일정: {ev.title}",
                body=ev.description[:80] if ev.description else f"{ev.event_type} 일정이 있어요!",
                url="/calendar",
                tag="event-today",
            )

        # ── 캘린더 행사 — 전날 알림 ──────────────────────────────────────────
        tomorrow = today + timedelta(days=1)
        events_tomorrow = db.query(CalendarEvent).filter(CalendarEvent.start_date == tomorrow).all()
        for ev in events_tomorrow:
            _broadcast_push(db,
                title=f"📢 내일 일정 알림: {ev.title}",
                body=f"내일({tomorrow.strftime('%m/%d')}) {ev.event_type} 일정이 있어요!",
                url="/calendar",
                tag="event-tomorrow",
            )

        db.close()
        _log.info("스케줄 푸시 완료: 생일%d명, 오늘행사%d개, 내일행사%d개",
                  len(bd_members), len(events_today), len(events_tomorrow))
    except Exception as _e:
        _log.error("스케줄 푸시 오류: %s", _e)


# ── 날짜 / 상태 헬퍼 ──────────────────────────────────────────────────────────

def _days_left(deadline: date) -> int:
    return (deadline - _today()).days


def _urgency(deadline: date) -> str:
    d = _days_left(deadline)
    if d < 0:    return "closed"
    if d <= 7:   return "urgent"
    if d <= 30:  return "soon"
    return "open"


# 공모전 단계 정의
COMP_STAGES = [
    ("review_1",     "review_1_date",     "1차 심사"),
    ("review_2",     "review_2_date",     "2차 심사"),
    ("announcement", "announcement_date", "결과 발표"),
    ("award",        "award_date",        "시상식"),
]

# 제출 서류 미리 정해진 목록
SUBMISSION_DOC_TYPES = ["활동계획서", "기획서", "포트폴리오", "발표자료(PPT)", "영상", "결과보고서"]

# CSS 캐시 버스팅 버전
import time as _time
_CSS_VER = str(int(_time.time()))


def _next_upcoming_event(comp) -> Optional[tuple]:
    """7일 이내 또는 당일인 다음 이벤트. 없으면 None.
    반환: (stage_key, label, event_date, days_left)"""
    today = _today()
    candidates = []

    # 고정 단계 (announcement, award)
    for stage_key, attr, label in [
        ("announcement", "announcement_date", "결과 발표"),
        ("award",        "award_date",        "시상식"),
    ]:
        d = getattr(comp, attr, None)
        if d and 0 <= (d - today).days <= 7:
            candidates.append((stage_key, label, d, (d - today).days))

    # 동적 심사 일정 (review_dates JSON)
    try:
        for i, rd in enumerate(json.loads(comp.review_dates or "[]")):
            rd_label = rd.get("label") or f"{i + 1}차 심사"
            rd_str   = rd.get("date", "")
            if not rd_str:
                continue
            d = date.fromisoformat(rd_str)
            if 0 <= (d - today).days <= 7:
                candidates.append((f"review_{i}", rd_label, d, (d - today).days))
    except Exception:
        pass

    if not candidates:
        return None
    return min(candidates, key=lambda x: x[3])


def _comp_stage(c) -> str:
    """접수중 / 심사중 / 발표준비중 / 마감 — 날짜 자동 + 수동 오버라이드"""
    if getattr(c, 'stage_override', None):
        return c.stage_override
    today = _today()
    if c.deadline >= today:
        return "접수중"
    ann = getattr(c, 'announcement_date', None)
    if ann is None or ann > today:
        return "심사중"
    award = getattr(c, 'award_date', None)
    if award is None or award > today:
        return "발표준비중"
    return "마감"


def _annotate(competitions: list) -> list:
    for c in competitions:
        c.upcoming_event = _next_upcoming_event(c)
        c.days_left = _days_left(c.deadline)
        d = c.days_left
        if d < 0:
            c.status = "upcoming" if c.upcoming_event else "closed"
        elif d <= 7:
            c.status = "urgent"
        elif d <= 30:
            c.status = "soon"
        else:
            c.status = "open"
        c.comp_stage = _comp_stage(c)
    return competitions


# ── 인증 헬퍼 ─────────────────────────────────────────────────────────────────

def _is_admin(request: Request) -> bool:
    token = request.cookies.get("admin_token")
    return bool(token and verify_token(token))


def _admin_redirect(request: Request):
    if not _is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    return None


def _privileged_redirect(request: Request, db: Session):
    """관리자 또는 중간관리자만 통과, 아니면 로그인 페이지로"""
    if _is_admin(request):
        return None
    m = _current_member(request, db)
    if m and m.role == "sub_admin":
        return None
    return RedirectResponse(url="/member/login", status_code=303)


def _current_member(request: Request, db: Session) -> Optional[Member]:
    token = request.cookies.get("member_token")
    if not token:
        return None
    mid = verify_member_token(token)
    if not mid:
        return None
    return db.query(Member).filter(Member.id == mid).first()


def _is_privileged(request: Request, db: Session) -> bool:
    if _is_admin(request):
        return True
    m = _current_member(request, db)
    return bool(m and m.role == "sub_admin")


def _ctx(request: Request, db: Session, **extra) -> dict:
    is_admin = _is_admin(request)
    cm = _current_member(request, db)
    base = {
        "request": request,
        "is_admin": is_admin,
        "current_member": cm,
        "is_privileged": is_admin or bool(cm and cm.role == "sub_admin"),
        "boards": BOARDS,
        "now": _now(),
        "today": _today(),
        "css_version": _CSS_VER,
        "submission_doc_types": SUBMISSION_DOC_TYPES,
    }
    # 알림 / DM 미읽음 뱃지
    notif_count = 0
    dm_unread   = 0
    if cm:
        notif_count = db.query(Notification).filter(
            Notification.member_id == cm.id, Notification.is_read.is_(False)
        ).count()
        dm_unread = db.query(DirectMessage).filter(
            DirectMessage.receiver_id == cm.id, DirectMessage.is_read.is_(False)
        ).count()
    base["notif_count"] = notif_count
    base["dm_unread"]   = dm_unread

    # 활성 사이트 배너
    now_dt = _now()
    active_banners = db.query(SiteBanner).filter(
        SiteBanner.is_active.is_(True),
        or_(SiteBanner.expires_at.is_(None), SiteBanner.expires_at > now_dt),
    ).order_by(SiteBanner.created_at.desc()).all()
    base["site_banners"] = active_banners

    base.update(extra)
    return base


def _render(request: Request, name: str, context: dict, status_code: int = 200):
    return templates.TemplateResponse(name=name, request=request, context=context, status_code=status_code)


# ── 파일 저장 헬퍼 ────────────────────────────────────────────────────────────

ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
ALLOWED_FILE_EXT = {".pdf", ".hwp", ".hwpx", ".zip", ".docx", ".pptx", ".xlsx", ".txt", ".png", ".jpg", ".jpeg"}
MAX_IMAGE_SIZE = int(os.getenv("MAX_IMAGE_SIZE", str(10 * 1024 * 1024)))   # 10 MB
MAX_FILE_SIZE  = int(os.getenv("MAX_FILE_SIZE",  str(50 * 1024 * 1024)))   # 50 MB


def _optimize_image(data: bytes, max_px: int = 1920, quality: int = 85) -> tuple[bytes, str]:
    """
    이미지 자동 최적화:
    - EXIF orientation 보정 (세로로 찍힌 사진 자동 회전)
    - 최대 1920px로 리사이즈
    - JPEG quality=85로 압축
    - RGBA/P → RGB 변환
    반환: (최적화된 bytes, 'image/jpeg')
    """
    try:
        from PIL import Image, ImageOps
        import io as _io
        img = Image.open(_io.BytesIO(data))
        img = ImageOps.exif_transpose(img)          # EXIF 회전 보정
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")
        if max(img.width, img.height) > max_px:
            img.thumbnail((max_px, max_px), Image.LANCZOS)
        buf = _io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception:
        return data, "image/jpeg"   # Pillow 실패 시 원본 그대로


async def _gpt_gallery_caption(image_bytes: bytes, ctype: str, hint: str = "") -> str:
    """
    GPT-4o 비전으로 갤러리 사진에 재미있는 코멘트 자동 생성.
    OPENAI_API_KEY 없으면 빈 문자열 반환.
    """
    if not os.getenv("OPENAI_API_KEY"):
        return ""
    try:
        import base64 as _b64
        from ai_parser import _client as _ai_client
        encoded  = _b64.b64encode(image_bytes).decode()
        data_url = f"data:{ctype};base64,{encoded}"
        prompt   = (
            "이 사진을 보고 재미있고 유쾌한 한 줄 갤러리 코멘트를 한국어로 작성해주세요. "
            "위트 있고 공감 가는 표현으로, 이모지를 적절히 섞어 100자 이내로 써주세요."
        )
        if hint.strip():
            prompt += f" 작성자 힌트: \"{hint.strip()}\""
        resp = await _ai_client().chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            messages=[{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": data_url, "detail": "low"}},
                {"type": "text",      "text": prompt},
            ]}],
            max_tokens=120,
            temperature=0.9,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return ""


def _is_valid_image_bytes(content: bytes) -> bool:
    """매직 바이트로 실제 이미지 파일 여부 확인 (확장자 스푸핑 방지)"""
    if len(content) < 12:
        return False
    return (
        content[:3] == b"\xff\xd8\xff"                          # JPEG
        or content[:8] == b"\x89PNG\r\n\x1a\n"                 # PNG
        or content[:6] in (b"GIF87a", b"GIF89a")               # GIF
        or (content[:4] == b"RIFF" and content[8:12] == b"WEBP")  # WebP
    )


async def _save_image(upload: Optional[UploadFile]) -> Optional[str]:
    if not upload or not upload.filename:
        return None
    ext = Path(upload.filename).suffix.lower()
    if ext not in ALLOWED_IMAGE_EXT:
        raise HTTPException(status_code=400, detail=f"이미지 파일만 업로드 가능합니다. (허용: {', '.join(ALLOWED_IMAGE_EXT)})")
    content = await upload.read()
    if not content:
        return None
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=413, detail=f"이미지 파일 크기는 {MAX_IMAGE_SIZE // 1024 // 1024}MB를 초과할 수 없습니다.")
    if not _is_valid_image_bytes(content):
        raise HTTPException(status_code=400, detail="유효하지 않은 이미지 파일입니다.")
    name = f"{uuid.uuid4().hex}{ext}"
    _storage_upload(content, name, upload.content_type or "image/jpeg")
    return name


async def _save_images(uploads: List[UploadFile]) -> list:
    saved = []
    for up in uploads or []:
        name = await _save_image(up)
        if name:
            saved.append(name)
    return saved


async def _save_files(files: List[UploadFile]) -> list:
    saved = []
    for f in files or []:
        if not f.filename:
            continue
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_FILE_EXT:
            continue  # 허용되지 않은 확장자는 건너뜀
        content = await f.read()
        if not content:
            continue
        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=413, detail=f"첨부 파일 크기는 {MAX_FILE_SIZE // 1024 // 1024}MB를 초과할 수 없습니다.")
        name = f"{uuid.uuid4().hex}{ext}"
        _storage_upload(content, name, f.content_type or "application/octet-stream")
        saved.append({"name": f.filename, "path": name})
    return saved


# ── 파일 삭제 헬퍼 ────────────────────────────────────────────────────────────

def _delete_upload(filename: Optional[str]) -> None:
    """업로드 파일 안전 삭제 (없거나 실패해도 무시)"""
    _storage_delete(filename or "")


# ── 리다이렉트 보안 헬퍼 ──────────────────────────────────────────────────────

def _safe_referer(request: Request, fallback: str = "/") -> str:
    """Referer 헤더를 검증해 같은 호스트의 경로만 허용 (open redirect 방지)"""
    ref = request.headers.get("referer", "")
    if not ref:
        return fallback
    try:
        parsed = urlparse(ref)
        host = request.headers.get("host", "")
        if parsed.netloc and parsed.netloc == host:
            path = parsed.path
            if parsed.query:
                path += f"?{parsed.query}"
            return path or fallback
        if not parsed.netloc and ref.startswith("/") and not ref.startswith("//"):
            return ref
    except Exception:
        pass
    return fallback


# ── 공통 헬퍼: 회원 이름 매핑 ────────────────────────────────────────────────

def _member_map(db: Session, ids: list[int]) -> dict[int, Member]:
    if not ids:
        return {}
    members = db.query(Member).filter(Member.id.in_(ids)).all()
    return {m.id: m for m in members}


def _chat_member(db: Session, room_id: int, member_id: int) -> Optional[ChatRoomMember]:
    return (
        db.query(ChatRoomMember)
        .filter(ChatRoomMember.room_id == room_id, ChatRoomMember.member_id == member_id)
        .first()
    )


def _ensure_chat_member(db: Session, room: ChatRoom, member: Member) -> ChatRoomMember:
    row = _chat_member(db, room.id, member.id)
    if row:
        if member.id == room.created_by_id and row.role != "owner":
            row.role = "owner"
            db.commit()
        return row
    role = "owner" if member.id == room.created_by_id else "member"
    row = ChatRoomMember(room_id=room.id, member_id=member.id, role=role)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _room_members(db: Session, room_id: int) -> list[ChatRoomMember]:
    rows = (
        db.query(ChatRoomMember)
        .filter(ChatRoomMember.room_id == room_id)
        .order_by(
            case(
                (ChatRoomMember.role == "owner", 0),
                (ChatRoomMember.role == "co_owner", 1),
                else_=2,
            ),
            ChatRoomMember.joined_at.asc(),
        )
        .all()
    )
    members = _member_map(db, [row.member_id for row in rows])
    for row in rows:
        row.member = members.get(row.member_id)
    return rows


def _can_manage_room(room_member: Optional[ChatRoomMember], request: Request, db: Session) -> bool:
    return _is_privileged(request, db) or bool(room_member and room_member.role in ("owner", "co_owner"))


def _is_comment_muted(member: Member) -> bool:
    return bool(member.comment_muted_until and member.comment_muted_until > _now())


# ════════════════════════════════════════════════════════════════════════════
#  공개 페이지 — 공모전
# ════════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: Session = Depends(get_db)):
    today = _today()

    # 통계
    total_members = db.query(func.count(Member.id)).scalar() or 0
    total_comps   = db.query(func.count(Competition.id)).scalar() or 0
    total_awards  = db.query(func.count(TeamCompetitionEntry.id)).filter(
        TeamCompetitionEntry.is_awarded.is_(True),
        TeamCompetitionEntry.is_public.is_(True),
    ).scalar() or 0

    # 진행 중인 공모전 (최대 4개)
    active_comps = _annotate(
        db.query(Competition)
        .filter(Competition.deadline >= today)
        .order_by(Competition.deadline.asc())
        .limit(4)
        .all()
    )

    # 최근 수상 실적 (공개, 최대 3개)
    recent_awards = db.query(TeamCompetitionEntry).filter(
        TeamCompetitionEntry.is_awarded.is_(True),
        TeamCompetitionEntry.is_public.is_(True),
    ).order_by(TeamCompetitionEntry.updated_at.desc()).limit(3).all()

    award_comp_ids = [e.competition_id for e in recent_awards]
    award_team_ids = [e.team_id for e in recent_awards]
    award_comps_map = {c.id: c for c in db.query(Competition).filter(Competition.id.in_(award_comp_ids)).all()} if award_comp_ids else {}
    award_teams_map = {t.id: t for t in db.query(Team).filter(Team.id.in_(award_team_ids)).all()} if award_team_ids else {}

    # 갤러리 최신 3개 (이스터에그 제외)
    recent_gallery = db.query(GalleryPost).filter(
        GalleryPost.is_public.is_(True),
        GalleryPost.is_easter.is_(False),
    ).order_by(GalleryPost.created_at.desc()).limit(3).all()

    # 홈 캘린더 — 앞으로 30일 이내 일정
    from datetime import timedelta as _td
    _home_end = today + _td(days=30)
    home_calendar_events = (
        db.query(CalendarEvent)
        .filter(CalendarEvent.start_date >= today, CalendarEvent.start_date <= _home_end)
        .order_by(CalendarEvent.start_date.asc())
        .limit(5)
        .all()
    )

    return _render(request, "home.html", _ctx(request, db,
        total_members=total_members,
        total_comps=total_comps,
        total_awards=total_awards,
        active_comps=active_comps,
        recent_awards=recent_awards,
        award_comps_map=award_comps_map,
        award_teams_map=award_teams_map,
        home_calendar_events=home_calendar_events,
        recent_gallery=recent_gallery,
    ))


@app.get("/competitions", response_class=HTMLResponse)
async def index(
    request: Request,
    tag: str = "",
    sort: str = "deadline",
    q: str = "",
    stage: str = "",
    page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    today = _today()
    active_priority = case((Competition.deadline < today, 1), else_=0)

    _FEATURED_MAX = 18  # 캐러셀 최대 표시 개수

    # 관리자가 ⭐ 주목 지정한 공모전 우선, 없으면 마감임박+조회수 자동 선택
    featured_manual = (
        db.query(Competition)
        .filter(Competition.is_featured.is_(True), Competition.deadline >= today,
                Competition.is_active.isnot(False))
        .order_by(Competition.deadline.asc())
        .limit(_FEATURED_MAX)
        .all()
    )
    if featured_manual:
        featured = featured_manual
    else:
        # 1순위: 마감 임박 (14일 이내), 2순위: 조회수, 3순위: 마감일 빠른 순
        featured = (
            db.query(Competition)
            .filter(Competition.deadline >= today, Competition.is_active.isnot(False))
            .order_by(
                case((Competition.deadline <= today + timedelta(days=14), 0), else_=1).asc(),
                Competition.view_count.desc(),
                Competition.deadline.asc(),
            )
            .limit(_FEATURED_MAX)
            .all()
        )
    _annotate(featured)

    # ── 마감 공모전 수 (아카이브 링크용) ──
    closed_count = (
        db.query(func.count(Competition.id))
        .filter(Competition.deadline < today, Competition.is_active.isnot(False))
        .scalar() or 0
    )

    # ── 메인 그리드: is_active 기준으로만 필터링 (마감 포함 표시) ──
    query = db.query(Competition).filter(Competition.is_active.isnot(False))
    if tag and tag != "all":
        query = query.filter(Competition.tags.like(f'%"{tag}"%'))
    if q:
        compact_q = _compact_text(q)
        query = query.filter(
            or_(
                _compact_column(Competition.title).contains(compact_q),
                _compact_column(Competition.organizer).contains(compact_q),
            )
        )

    if sort == "views":
        query = query.order_by(Competition.view_count.desc(), Competition.deadline.asc())
    elif sort == "newest":
        query = query.order_by(Competition.created_at.desc())
    else:
        # 마감 전: 데드라인 오름차순, 마감 후: 데드라인 내림차순(최근 마감 먼저)
        query = query.order_by(active_priority.asc(), Competition.deadline.asc())

    all_competitions = _annotate(query.all())
    # upcoming(이벤트 임박) 공모전을 active 뒤에 배치, 마감은 맨 뒤
    if sort == "deadline":
        def _sort_key(c):
            if c.status in ("urgent", "soon", "open"):
                return (0, c.days_left)
            if c.status == "upcoming":
                ev = c.upcoming_event
                return (1, ev[3] if ev else 99)
            # 마감: 최근 마감 먼저 (-days_left로 정렬, days_left는 음수이므로 양수로 변환)
            return (2, -c.days_left)
        all_competitions.sort(key=_sort_key)

    # 단계별 카운트 (필터 전)
    _STAGES = ["접수중", "심사중", "발표준비중", "마감"]
    stage_counts = {s: sum(1 for c in all_competitions if c.comp_stage == s) for s in _STAGES}

    # 단계 필터
    if stage and stage in _STAGES:
        all_competitions = [c for c in all_competitions if c.comp_stage == stage]

    # 페이지네이션
    _PAGE_SIZE = 12
    total_count = len(all_competitions)
    total_pages = max(1, (total_count + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(1, min(page, total_pages))
    competitions = all_competitions[(_PAGE_SIZE * (page - 1)):(_PAGE_SIZE * page)]

    all_ids = [c.id for c in all_competitions] + [c.id for c in featured]
    counts = dict(
        db.query(TeamMember.competition_id, func.count(TeamMember.id))
        .filter(
            TeamMember.competition_id.in_(all_ids),
            TeamMember.is_leader.is_(False),
            TeamMember.status == "approved",
        )
        .group_by(TeamMember.competition_id)
        .all()
    ) if all_ids else {}
    for c in all_competitions + featured:
        c.member_count = counts.get(c.id, 0)

    # 로그인한 회원의 관심 공모전 ID 집합 (하트 버튼 활성 여부 판단)
    cm = _current_member(request, db)
    user_scrap_ids: set = set()
    if cm:
        user_scrap_ids = {
            s.competition_id for s in
            db.query(CompetitionScrap).filter(CompetitionScrap.member_id == cm.id).all()
        }

    return _render(request,
        "index.html",
        _ctx(request, db,
             featured=featured, competitions=competitions,
             tags=list(_CONTESTKOREA_CATS),
             current_tag=tag or "all",
             current_sort=sort, query=q, today=today,
             page=page, total_pages=total_pages, total_count=total_count,
             closed_count=closed_count,
             current_stage=stage,
             stage_counts=stage_counts,
             user_scrap_ids=user_scrap_ids),
    )


@app.get("/competitions/archive", response_class=HTMLResponse)
async def competitions_archive(
    request: Request,
    tag: str = "",
    sort: str = "deadline",
    q: str = "",
    page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    today = _today()

    query = db.query(Competition).filter(Competition.deadline < today, Competition.is_active.isnot(False))
    if tag and tag != "all":
        query = query.filter(Competition.tags.like(f'%"{tag}"%'))
    if q:
        compact_q = _compact_text(q)
        query = query.filter(
            or_(
                _compact_column(Competition.title).contains(compact_q),
                _compact_column(Competition.organizer).contains(compact_q),
            )
        )

    if sort == "views":
        query = query.order_by(Competition.view_count.desc())
    elif sort == "newest":
        query = query.order_by(Competition.created_at.desc())
    else:
        # 기본: 최근 마감일 순
        query = query.order_by(Competition.deadline.desc())

    all_competitions = _annotate(query.all())

    _PAGE_SIZE = 12
    total_count = len(all_competitions)
    total_pages = max(1, (total_count + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(1, min(page, total_pages))
    competitions = all_competitions[(_PAGE_SIZE * (page - 1)):(_PAGE_SIZE * page)]

    all_ids = [c.id for c in all_competitions]
    counts = dict(
        db.query(TeamMember.competition_id, func.count(TeamMember.id))
        .filter(
            TeamMember.competition_id.in_(all_ids),
            TeamMember.is_leader.is_(False),
            TeamMember.status == "approved",
        )
        .group_by(TeamMember.competition_id)
        .all()
    ) if all_ids else {}
    for c in all_competitions:
        c.member_count = counts.get(c.id, 0)

    return _render(request,
        "archive.html",
        _ctx(request, db,
             competitions=competitions,
             tags=list(_CONTESTKOREA_CATS),
             current_tag=tag or "all",
             current_sort=sort, query=q, today=today,
             page=page, total_pages=total_pages, total_count=total_count),
    )


@app.get("/competition/{comp_id}", response_class=HTMLResponse)
async def detail(request: Request, comp_id: int, db: Session = Depends(get_db)):
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404, detail="공모전을 찾을 수 없습니다.")

    db.execute(
        _sa_update(Competition).where(Competition.id == comp_id).values(view_count=Competition.view_count + 1)
    )
    db.commit()
    db.refresh(comp)

    _annotate([comp])

    teams = (
        db.query(Team)
        .filter(Team.competition_id == comp_id)
        .order_by(Team.created_at.asc())
        .all()
    )
    team_ids = [t.id for t in teams]
    all_tm = (
        db.query(TeamMember)
        .filter(TeamMember.team_id.in_(team_ids))
        .order_by(TeamMember.created_at.asc())
        .all()
    ) if team_ids else []
    # team별 멤버 맵
    tm_by_team: dict = {}
    for tm in all_tm:
        tm_by_team.setdefault(tm.team_id, []).append(tm)
    for t in teams:
        t.members = tm_by_team.get(t.id, [])

    today = _today()
    # 접수 시작~마감 후 30일까지 제출 기록 허용 (팀장·관리자 기준)
    submission_window = today <= comp.deadline + timedelta(days=30)

    # 스크랩 여부
    cm = _current_member(request, db)
    user_scrapped = False
    if cm:
        user_scrapped = bool(
            db.query(CompetitionScrap)
            .filter(CompetitionScrap.competition_id == comp_id, CompetitionScrap.member_id == cm.id)
            .first()
        )

    # 팀장인 팀 IDs
    leader_team_ids: set = set()
    if cm:
        for tm in all_tm:
            if tm.is_leader and (tm.member_id == cm.id or tm.nickname == cm.activity_name):
                leader_team_ids.add(tm.team_id)

    # 현재 로그인 멤버가 이 공모전 팀에 속해 있으면 "참가 중"
    is_participating = False
    my_entry_team_id: Optional[int] = None
    if cm:
        for tm in all_tm:
            if tm.member_id == cm.id or tm.nickname == cm.activity_name:
                is_participating = True
                my_entry_team_id = tm.team_id
                break

    # 각 팀의 단계 결과 맵: {team_id: {stage: TeamResult}}
    team_result_map: dict = {}
    if team_ids:
        results = db.query(TeamResult).filter(TeamResult.team_id.in_(team_ids)).all()
        for r in results:
            team_result_map.setdefault(r.team_id, {})[r.stage] = r

    # 팀별 자기 기재 실적: {team_id: TeamCompetitionEntry}
    team_entries: dict = {}
    if team_ids:
        entries = db.query(TeamCompetitionEntry).filter(
            TeamCompetitionEntry.competition_id == comp_id,
            TeamCompetitionEntry.team_id.in_(team_ids),
        ).all()
        for e in entries:
            team_entries[e.team_id] = e

    # 관리자용 — 팀원 직접 추가 / 팀 생성에 사용할 전체 멤버 목록
    all_members_for_admin = []
    kick_requests = []
    if _is_privileged(request, db):
        all_members_for_admin = db.query(Member).order_by(Member.activity_name).all()
        kick_requests = db.query(TeamKickRequest).filter(
            TeamKickRequest.competition_id == comp_id
        ).order_by(TeamKickRequest.created_at.asc()).all()

    comp_started = bool(comp.start_date and today >= comp.start_date)

    return _render(request,
        "detail.html",
        _ctx(request, db,
             comp=comp, files=_from_json(comp.files),
             tags_list=_from_json(comp.tags),
             review_dates_list=_from_json(comp.review_dates or "[]"),
             teams=teams, roles=ROLES,
             submission_window=submission_window, today=today,
             user_scrapped=user_scrapped,
             leader_team_ids=leader_team_ids,
             team_result_map=team_result_map,
             team_entries=team_entries,
             comp_stages=COMP_STAGES,
             is_participating=is_participating,
             my_entry_team_id=my_entry_team_id,
             all_members_for_admin=all_members_for_admin,
             kick_requests=kick_requests,
             comp_started=comp_started),
    )


@app.post("/competition/{comp_id}/scrap")
async def toggle_scrap(request: Request, comp_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    existing = (
        db.query(CompetitionScrap)
        .filter(CompetitionScrap.competition_id == comp_id, CompetitionScrap.member_id == cm.id)
        .first()
    )
    if existing:
        db.delete(existing)
        scrapped = False
    else:
        db.add(CompetitionScrap(competition_id=comp_id, member_id=cm.id))
        scrapped = True
    db.commit()
    return JSONResponse({"scrapped": scrapped})


@app.post("/competition/{comp_id}/team/{team_id}/stage-result")
async def record_stage_result(
    request: Request,
    comp_id: int,
    team_id: int,
    stage: str = Form(...),
    passed: Optional[str] = Form(None),   # "true"/"false"/None
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    """팀장만 호출 가능. 단계 결과 기입 + 팀원 Member 계정 연결."""
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 팀장 확인
    leader_tm = (
        db.query(TeamMember)
        .filter(
            TeamMember.team_id == team_id,
            TeamMember.is_leader.is_(True),
        )
        .first()
    )
    if not leader_tm:
        raise HTTPException(status_code=403, detail="팀장만 결과를 기입할 수 있습니다.")
    if leader_tm.member_id != cm.id and leader_tm.nickname != cm.activity_name:
        raise HTTPException(status_code=403, detail="팀장만 결과를 기입할 수 있습니다.")

    if stage not in {s[0] for s in COMP_STAGES}:
        raise HTTPException(status_code=400, detail="올바른 단계가 아닙니다.")

    # TeamResult upsert
    result = (
        db.query(TeamResult)
        .filter(TeamResult.team_id == team_id, TeamResult.stage == stage)
        .first()
    )
    passed_bool = True if passed == "true" else (False if passed == "false" else None)
    if result:
        result.passed = passed_bool
        result.note = note.strip()
        result.recorded_at = _now()
        result.recorded_by_id = cm.id
    else:
        db.add(TeamResult(
            team_id=team_id, competition_id=comp_id,
            stage=stage, passed=passed_bool,
            note=note.strip(), recorded_by_id=cm.id,
        ))

    # 팀원 Member 계정 연결 (form에서 tm_{id}_real_name, tm_{id}_student_id 전달 시)
    form_data = await request.form()
    team_members = db.query(TeamMember).filter(TeamMember.team_id == team_id).all()
    for tm in team_members:
        rn_key = f"tm_{tm.id}_real_name"
        sid_key = f"tm_{tm.id}_student_id"
        real_name = (form_data.get(rn_key) or "").strip()
        student_id = (form_data.get(sid_key) or "").strip()
        if real_name and student_id and not tm.member_id:
            matched = (
                db.query(Member)
                .filter(Member.real_name == real_name, Member.student_id == student_id)
                .first()
            )
            if matched:
                tm.member_id = matched.id

    # 수상 단계 통과 시 팀원 award_rank 자동 기록
    if stage == "award" and passed_bool is True:
        for tm in team_members:
            if not tm.award_rank:
                tm.award_rank = "수상"   # 기본값; 관리자가 세부 수정 가능

    db.commit()
    return RedirectResponse(url=f"/my#team-{team_id}", status_code=303)


# ── 팀 공모전 실적 입력 ──────────────────────────────────────────────────────────

def _get_entry_team_leader(request: Request, db: Session, comp_id: int, team_id: int):
    """팀장 권한 확인 후 (cm, team, entry) 반환. 권한 없으면 HTTPException."""
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404, detail="팀을 찾을 수 없습니다.")
    leader_tm = db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)
    ).first()
    is_admin_ = bool(request.cookies.get("admin_token") and verify_token(request.cookies["admin_token"]))
    if not is_admin_:
        if not leader_tm or (leader_tm.member_id != cm.id and leader_tm.nickname != cm.activity_name):
            raise HTTPException(status_code=403, detail="팀장만 실적을 입력할 수 있습니다.")
    entry = db.query(TeamCompetitionEntry).filter(
        TeamCompetitionEntry.team_id == team_id,
        TeamCompetitionEntry.competition_id == comp_id,
    ).first()
    return cm, team, entry


@app.get("/competition/{comp_id}/team/{team_id}/achievement", response_class=HTMLResponse)
async def achievement_form(request: Request, comp_id: int, team_id: int, db: Session = Depends(get_db)):
    cm, team, entry = _get_entry_team_leader(request, db, comp_id, team_id)
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404, detail="공모전을 찾을 수 없습니다.")

    review_dates_list = []
    try:
        review_dates_list = json.loads(comp.review_dates or "[]")
    except Exception:
        pass

    stage_results = []
    if entry:
        try:
            stage_results = json.loads(entry.stage_results or "[]")
        except Exception:
            pass

    # 기존 단계 결과와 review_dates 병합 (label 기준)
    sr_map = {sr["label"]: sr for sr in stage_results}
    merged_stages = []
    for rd in review_dates_list:
        label = rd.get("label", "")
        existing = sr_map.get(label, {})
        merged_stages.append({
            "label": label,
            "date": rd.get("date", ""),
            "passed": existing.get("passed"),   # True/False/None
            "note": existing.get("note", ""),
        })
    # review_dates에 없는 기존 단계도 포함
    for sr in stage_results:
        if not any(ms["label"] == sr["label"] for ms in merged_stages):
            merged_stages.append(sr)

    pending_image = bool(entry and entry.proof_image and not entry.proof_approved)
    approved_image = bool(entry and entry.proof_image and entry.proof_approved)

    return _render(request, "competition_achievement.html", _ctx(request, db,
        comp=comp,
        team=team,
        entry=entry,
        merged_stages=merged_stages,
        pending_image=pending_image,
        approved_image=approved_image,
    ))


@app.post("/competition/{comp_id}/team/{team_id}/achievement", response_class=HTMLResponse)
async def achievement_save(
    request: Request,
    comp_id: int,
    team_id: int,
    is_awarded: Optional[str] = Form(None),
    award_name: str = Form(""),
    prize_amount: str = Form(""),
    is_public: Optional[str] = Form(None),
    note: str = Form(""),
    proof_image: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    cm, team, entry = _get_entry_team_leader(request, db, comp_id, team_id)

    # 단계별 결과 파싱 (form: stage_label_0, stage_passed_0, stage_note_0 …)
    form_data = await request.form()
    stage_results = []
    i = 0
    while f"stage_label_{i}" in form_data:
        label = str(form_data.get(f"stage_label_{i}", "")).strip()
        passed_raw = form_data.get(f"stage_passed_{i}")
        note_i = str(form_data.get(f"stage_note_{i}", "")).strip()
        passed_val = True if passed_raw == "true" else (False if passed_raw == "false" else None)
        if label:
            stage_results.append({"label": label, "passed": passed_val, "note": note_i})
        i += 1

    # 증빙 이미지 처리
    new_proof_path: Optional[str] = None
    if proof_image and proof_image.filename:
        ext = Path(proof_image.filename).suffix.lower()
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            raise HTTPException(status_code=400, detail="이미지 파일만 업로드 가능합니다.")
        img_data = await proof_image.read()
        if len(img_data) > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="파일 크기는 10MB 이하로 제한됩니다.")
        fname = f"{uuid.uuid4().hex}{ext}"
        _storage_upload(img_data, fname, proof_image.content_type or "image/jpeg")
        new_proof_path = fname

    now_dt = _now()

    if entry is None:
        entry = TeamCompetitionEntry(
            team_id=team_id,
            competition_id=comp_id,
            recorded_by_id=cm.id,
        )
        db.add(entry)

    entry.stage_results = json.dumps(stage_results, ensure_ascii=False)
    entry.is_awarded    = is_awarded == "true"
    entry.award_name    = award_name.strip()
    entry.prize_amount  = prize_amount.strip()
    entry.is_public     = is_public == "true"
    entry.note          = note.strip()
    entry.updated_at    = now_dt

    if new_proof_path:
        # 새 이미지 업로드 → 승인 초기화
        entry.proof_image          = new_proof_path
        entry.proof_approved       = False
        entry.proof_approved_at    = None
        entry.proof_approved_by    = None
        entry.proof_rejected_reason = ""

    db.commit()
    return RedirectResponse(
        url=f"/competition/{comp_id}/team/{team_id}/achievement?saved=1",
        status_code=303,
    )


# ── 관리자 증빙 이미지 승인/반려 ─────────────────────────────────────────────────

@app.post("/admin/achievement/{entry_id}/approve-proof")
async def admin_approve_proof(
    request: Request,
    entry_id: int,
    db: Session = Depends(get_db),
):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    entry = db.query(TeamCompetitionEntry).filter(TeamCompetitionEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="실적 기록을 찾을 수 없습니다.")
    entry.proof_approved      = True
    entry.proof_approved_at   = _now()
    entry.proof_rejected_reason = ""
    db.commit()
    return RedirectResponse(
        url=f"/competition/{entry.competition_id}?entry_approved=1",
        status_code=303,
    )


@app.post("/admin/achievement/{entry_id}/reject-proof")
async def admin_reject_proof(
    request: Request,
    entry_id: int,
    reason: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    entry = db.query(TeamCompetitionEntry).filter(TeamCompetitionEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="실적 기록을 찾을 수 없습니다.")
    entry.proof_approved        = False
    entry.proof_approved_at     = None
    entry.proof_rejected_reason = reason.strip()
    db.commit()
    return RedirectResponse(
        url=f"/competition/{entry.competition_id}?entry_rejected=1",
        status_code=303,
    )


# ── 멤버 목록 (기수별) ───────────────────────────────────────────────────────────

@app.get("/members", response_class=HTMLResponse)
async def members_page(request: Request, db: Session = Depends(get_db)):
    all_members = db.query(Member).order_by(
        Member.generation.asc().nullslast(),
        Member.created_at.asc(),
    ).all()

    # 기수별 그룹핑 {기수: [Member, ...]}
    from collections import OrderedDict
    groups: dict = OrderedDict()
    for m in all_members:
        key = m.generation if m.generation else 0
        groups.setdefault(key, []).append(m)

    # 멤버별 수상 실적 (member_id → [TeamCompetitionEntry, ...])
    member_ids = [m.id for m in all_members]
    # 해당 멤버들이 속한 TeamMember 레코드
    tms = db.query(TeamMember).filter(
        TeamMember.member_id.in_(member_ids)
    ).all() if member_ids else []
    # member_id → team_ids
    member_team_ids: dict = {}
    for tm in tms:
        if tm.member_id:
            member_team_ids.setdefault(tm.member_id, set()).add(tm.team_id)
    # 수상 팀 엔트리 (공개 + 수상된 것만)
    all_team_ids = list({tid for tids in member_team_ids.values() for tid in tids})
    award_entries = db.query(TeamCompetitionEntry).filter(
        TeamCompetitionEntry.team_id.in_(all_team_ids),
        TeamCompetitionEntry.is_awarded.is_(True),
        TeamCompetitionEntry.is_public.is_(True),
        TeamCompetitionEntry.proof_approved.is_(True),
    ).order_by(TeamCompetitionEntry.updated_at.desc()).all() if all_team_ids else []
    # 관련 Competition 정보
    ac_comp_ids = list({e.competition_id for e in award_entries})
    ac_comps = {c.id: c for c in db.query(Competition).filter(Competition.id.in_(ac_comp_ids)).all()} if ac_comp_ids else {}
    # member_id → [entry, ...]
    member_awards: dict = {}
    for m in all_members:
        tids = member_team_ids.get(m.id, set())
        ents = [e for e in award_entries if e.team_id in tids]
        member_awards[m.id] = ents[:3]  # 최대 3개

    return _render(request, "members.html", _ctx(request, db,
        groups=groups,
        member_awards=member_awards,
        ac_comps=ac_comps,
    ))


# ── 수상 실적 ────────────────────────────────────────────────────────────────────

@app.get("/awards", response_class=HTMLResponse)
async def awards_page(request: Request, year: Optional[int] = None, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    is_admin_ = _is_admin(request)

    # 공개된 수상 실적 (+ 관리자면 비공개도 포함)
    q = db.query(TeamCompetitionEntry).filter(TeamCompetitionEntry.is_awarded.is_(True))
    if not is_admin_:
        q = q.filter(TeamCompetitionEntry.is_public.is_(True))
    entries = q.order_by(TeamCompetitionEntry.updated_at.desc()).all()

    # 관련 Competition / Team / Member 정보 사전 로드
    comp_ids = list({e.competition_id for e in entries})
    team_ids = list({e.team_id for e in entries})
    comps_map = {c.id: c for c in db.query(Competition).filter(Competition.id.in_(comp_ids)).all()} if comp_ids else {}
    teams_map = {t.id: t for t in db.query(Team).filter(Team.id.in_(team_ids)).all()} if team_ids else {}
    members_map: dict = {}
    if team_ids:
        tms = db.query(TeamMember).filter(TeamMember.team_id.in_(team_ids)).all()
        for tm in tms:
            members_map.setdefault(tm.team_id, []).append(tm)

    # 연도별 그룹핑 (competition.deadline.year 기준, 없으면 updated_at.year)
    from collections import OrderedDict
    year_groups: dict = OrderedDict()
    for e in entries:
        comp = comps_map.get(e.competition_id)
        y = comp.deadline.year if comp and comp.deadline else e.updated_at.year
        year_groups.setdefault(y, []).append(e)
    # 최신 연도 먼저
    year_groups = OrderedDict(sorted(year_groups.items(), reverse=True))

    # 연도 필터
    current_year = year
    all_years = list(year_groups.keys())

    return _render(request, "awards.html", _ctx(request, db,
        entries=entries,
        comps_map=comps_map,
        teams_map=teams_map,
        members_map=members_map,
        year_groups=year_groups,
        all_years=all_years,
        current_year=current_year,
    ))


# ── 갤러리 ───────────────────────────────────────────────────────────────────────

@app.get("/gallery", response_class=HTMLResponse)
async def gallery_page(request: Request, db: Session = Depends(get_db)):
    is_admin_ = _is_admin(request)
    q = db.query(GalleryPost).filter(GalleryPost.is_easter.is_(False))
    if not is_admin_:
        q = q.filter(GalleryPost.is_public.is_(True))
    posts = q.order_by(GalleryPost.sort_order, GalleryPost.event_date.desc().nullslast(), GalleryPost.created_at.desc()).all()

    # 연도별 그룹핑 (event_date 없으면 created_at 연도 사용)
    posts_by_year: dict = {}
    for p in posts:
        y = p.event_date.year if p.event_date else p.created_at.year
        posts_by_year.setdefault(y, []).append(p)
    sorted_years = sorted(posts_by_year.keys(), reverse=True)

    return _render(request, "gallery.html", _ctx(request, db,
        posts=posts,
        posts_by_year=posts_by_year,
        sorted_years=sorted_years,
    ))


@app.post("/gallery/upload-image")
async def gallery_upload_image(
    request: Request,
    image: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """갤러리 에디터 인라인 이미지 업로드"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    data = await image.read()
    if len(data) > 8 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="8MB 이하 이미지만 가능합니다")
    data, ctype = _optimize_image(data, max_px=1200, quality=82)
    fname = f"desc_{uuid.uuid4().hex}.jpg"
    _storage_upload(data, fname, ctype)
    return {"url": f"/uploads/{fname}"}


@app.post("/gallery/new")
async def gallery_new(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    event_type: str = Form("기타"),
    event_date: Optional[str] = Form(None),
    is_public: Optional[str] = Form(None),
    gpt_decorate: Optional[str] = Form(None),
    gpt_hint: str = Form(""),
    images: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    cm = _current_member(request, db)
    if not cm and not _is_admin(request):
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    saved_images = []
    first_img_data: Optional[bytes] = None
    first_img_ctype: str = "image/jpeg"
    for img in images:
        if img and img.filename:
            ext = Path(img.filename).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                continue
            data = await img.read()
            if len(data) > 20 * 1024 * 1024:
                continue
            data, ctype = _optimize_image(data)
            if first_img_data is None:
                first_img_data = data
                first_img_ctype = ctype
            fname = f"{uuid.uuid4().hex}.jpg"
            _storage_upload(data, fname, ctype)
            saved_images.append(fname)

    # GPT 꾸미기 — 첫 번째 사진으로 재미있는 코멘트 자동 생성
    final_desc = description.strip()
    if gpt_decorate and first_img_data:
        gpt_result = await _gpt_gallery_caption(first_img_data, first_img_ctype, gpt_hint)
        if gpt_result:
            final_desc = gpt_result

    parsed_date = None
    if event_date:
        try:
            parsed_date = date.fromisoformat(event_date)
        except ValueError:
            pass

    post = GalleryPost(
        title=title.strip(),
        description=final_desc,
        event_type=event_type,
        event_date=parsed_date,
        images=json.dumps(saved_images, ensure_ascii=False),
        created_by_id=cm.id if cm else 0,
        is_public=is_public == "true",
    )
    db.add(post)
    db.commit()
    return RedirectResponse(url="/gallery", status_code=303)


# ── 이스터에그 갤러리 (/gallery/c) ─────────────────────────────────────────────

@app.get("/gallery/c/enter")
async def easter_gallery_enter(request: Request):
    """기믹(hidden C 링크) 클릭 시 쿠키를 심고 이스터에그 페이지로 이동."""
    response = RedirectResponse(url="/gallery/c", status_code=302)
    response.set_cookie("_ce", _EASTER_COOKIE, httponly=True, samesite="lax", max_age=86400 * 365)
    return response


@app.get("/gallery/c", response_class=HTMLResponse)
async def easter_gallery_page(request: Request, db: Session = Depends(get_db)):
    if request.cookies.get("_ce") != _EASTER_COOKIE:
        return RedirectResponse(url="/gallery", status_code=302)
    posts = db.query(GalleryPost).filter(
        GalleryPost.is_easter.is_(True)
    ).order_by(GalleryPost.sort_order, GalleryPost.event_date.desc().nullslast(), GalleryPost.created_at.desc()).all()

    posts_by_year: dict = {}
    for p in posts:
        y = p.event_date.year if p.event_date else p.created_at.year
        posts_by_year.setdefault(y, []).append(p)
    sorted_years = sorted(posts_by_year.keys(), reverse=True)

    return _render(request, "easter.html", _ctx(request, db,
        posts=posts,
        posts_by_year=posts_by_year,
        sorted_years=sorted_years,
    ))


@app.post("/gallery/c/new")
async def easter_gallery_new(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    event_type: str = Form("기타"),
    event_date: Optional[str] = Form(None),
    gpt_decorate: Optional[str] = Form(None),
    gpt_hint: str = Form(""),
    images: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    if not _is_admin(request):
        raise HTTPException(status_code=403)

    saved_images = []
    first_img_data: Optional[bytes] = None
    first_img_ctype: str = "image/jpeg"
    for img in images:
        if img and img.filename:
            ext = Path(img.filename).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                continue
            data = await img.read()
            if len(data) > 20 * 1024 * 1024:
                continue
            data, ctype = _optimize_image(data)
            if first_img_data is None:
                first_img_data = data
                first_img_ctype = ctype
            fname = f"{uuid.uuid4().hex}.jpg"
            _storage_upload(data, fname, ctype)
            saved_images.append(fname)

    final_desc = description.strip()
    if gpt_decorate and first_img_data:
        gpt_result = await _gpt_gallery_caption(first_img_data, first_img_ctype, gpt_hint)
        if gpt_result:
            final_desc = gpt_result

    parsed_date = None
    if event_date:
        try:
            parsed_date = date.fromisoformat(event_date)
        except ValueError:
            pass

    post = GalleryPost(
        title=title.strip(),
        description=final_desc,
        event_type=event_type,
        event_date=parsed_date,
        images=json.dumps(saved_images, ensure_ascii=False),
        created_by_id=0,
        is_public=True,
        is_easter=True,
    )
    db.add(post)
    db.commit()
    return RedirectResponse(url="/gallery/c", status_code=303)


@app.post("/gallery/c/{post_id}/edit")
async def easter_gallery_edit(
    request: Request,
    post_id: int,
    title: str = Form(...),
    description: str = Form(""),
    event_type: str = Form("기타"),
    event_date: Optional[str] = Form(None),
    delete_images: List[str] = Form(default=[]),
    add_images: List[UploadFile] = File(default=[]),
    image_order: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    post = db.query(GalleryPost).filter(GalleryPost.id == post_id, GalleryPost.is_easter.is_(True)).first()
    if not post:
        raise HTTPException(status_code=404)
    post.title = title.strip()
    post.description = description.strip()
    post.event_type = event_type
    if event_date:
        try:
            post.event_date = date.fromisoformat(event_date)
        except ValueError:
            pass

    existing = _from_json(post.images or "[]")
    kept = [img for img in existing if img not in delete_images]
    for img in add_images:
        if img and img.filename:
            ext = Path(img.filename).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                continue
            data = await img.read()
            if len(data) > 20 * 1024 * 1024:
                continue
            data, ctype = _optimize_image(data)
            fname = f"{uuid.uuid4().hex}.jpg"
            _storage_upload(data, fname, ctype)
            kept.append(fname)
    if image_order:
        order_list = [f.strip() for f in image_order.split(",") if f.strip()]
        kept_set = set(kept)
        ordered = [f for f in order_list if f in kept_set]
        kept = ordered + [f for f in kept if f not in set(ordered)]
    post.images = json.dumps(kept, ensure_ascii=False)
    db.commit()
    return RedirectResponse(url="/gallery/c", status_code=303)


@app.post("/gallery/c/{post_id}/delete")
async def easter_gallery_delete(request: Request, post_id: int, db: Session = Depends(get_db)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    post = db.query(GalleryPost).filter(GalleryPost.id == post_id, GalleryPost.is_easter.is_(True)).first()
    if post:
        db.delete(post)
        db.commit()
    return RedirectResponse(url="/gallery/c", status_code=303)


@app.post("/gallery/reorder")
async def gallery_reorder(request: Request, db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    data = await request.json()
    order = data.get("order", [])
    for idx, post_id in enumerate(order):
        db.query(GalleryPost).filter(
            GalleryPost.id == int(post_id),
            GalleryPost.is_easter.is_(False),
        ).update({"sort_order": idx * 10})
    db.commit()
    return JSONResponse({"ok": True})


@app.post("/gallery/c/reorder")
async def easter_gallery_reorder(request: Request, db: Session = Depends(get_db)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    data = await request.json()
    order = data.get("order", [])
    for idx, post_id in enumerate(order):
        db.query(GalleryPost).filter(
            GalleryPost.id == int(post_id),
            GalleryPost.is_easter.is_(True),
        ).update({"sort_order": idx * 10})
    db.commit()
    return JSONResponse({"ok": True})


@app.post("/gallery/{post_id}/delete")
async def gallery_delete(request: Request, post_id: int, db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    post = db.query(GalleryPost).filter(GalleryPost.id == post_id).first()
    if post:
        db.delete(post)
        db.commit()
    return RedirectResponse(url="/gallery", status_code=303)


@app.post("/gallery/{post_id}/edit")
async def gallery_edit(
    request: Request,
    post_id: int,
    title: str = Form(...),
    description: str = Form(""),
    event_type: str = Form("기타"),
    event_date: Optional[str] = Form(None),
    is_public: Optional[str] = Form(None),
    delete_images: List[str] = Form(default=[]),
    add_images: List[UploadFile] = File(default=[]),
    image_order: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    post = db.query(GalleryPost).filter(GalleryPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404)
    post.title = title.strip()
    post.description = description.strip()
    post.event_type = event_type
    post.is_public = is_public == "true"
    if event_date:
        try:
            post.event_date = date.fromisoformat(event_date)
        except ValueError:
            pass

    # 기존 이미지에서 삭제 선택된 것 제거
    existing = _from_json(post.images or "[]")
    kept = [img for img in existing if img not in delete_images]

    # 새 이미지 추가
    for img in add_images:
        if img and img.filename:
            ext = Path(img.filename).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                continue
            data = await img.read()
            if len(data) > 20 * 1024 * 1024:
                continue
            data, ctype = _optimize_image(data)   # 자동 리사이즈+압축+EXIF 보정
            fname = f"{uuid.uuid4().hex}.jpg"
            _storage_upload(data, fname, ctype)
            kept.append(fname)

    # 드래그로 지정한 순서 반영 (image_order: 콤마 구분 파일명)
    if image_order:
        order_list = [f.strip() for f in image_order.split(",") if f.strip()]
        kept_set = set(kept)
        # 순서대로 kept 이미지만 추출
        ordered = [f for f in order_list if f in kept_set]
        # image_order에 없는 항목(새로 추가된 것)은 뒤에 붙임
        appended = [f for f in kept if f not in set(ordered)]
        kept = ordered + appended

    post.images = json.dumps(kept, ensure_ascii=False)
    db.commit()
    return RedirectResponse(url="/gallery", status_code=303)


@app.get("/my", response_class=HTMLResponse)
async def mypage(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login?next=/my", status_code=303)

    today = _today()

    # 스크랩 공모전
    scrap_ids = [
        s.competition_id for s in
        db.query(CompetitionScrap).filter(CompetitionScrap.member_id == cm.id).all()
    ]
    scrapped_comps = (
        _annotate(db.query(Competition).filter(Competition.id.in_(scrap_ids)).all())
        if scrap_ids else []
    )

    # 내가 참여 중인 팀 (nickname 또는 member_id 기반)
    my_tms = (
        db.query(TeamMember)
        .filter(
            or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name)
        )
        .all()
    )

    # 관련 공모전/팀을 한 번에 일괄 조회
    my_comp_ids = list({tm.competition_id for tm in my_tms if tm.competition_id})
    my_team_ids = list({tm.team_id for tm in my_tms if tm.team_id})
    comps_map_my: dict = {}
    teams_map_my: dict = {}
    if my_comp_ids:
        for c in _annotate(db.query(Competition).filter(Competition.id.in_(my_comp_ids)).all()):
            comps_map_my[c.id] = c
    if my_team_ids:
        for t in db.query(Team).filter(Team.id.in_(my_team_ids)).all():
            teams_map_my[t.id] = t

    # 진행 중 프로젝트 (마감 안 지난 공모전)
    active_projects = []
    seen_comp_ids: set = set()
    for tm in my_tms:
        if tm.competition_id in seen_comp_ids:
            continue
        comp = comps_map_my.get(tm.competition_id)
        if comp and comp.deadline >= today:
            team = teams_map_my.get(tm.team_id)
            active_projects.append({"comp": comp, "team": team, "tm": tm})
            seen_comp_ids.add(tm.competition_id)

    # 팀장 이벤트 알림 (7일 내 이벤트가 있는 공모전) — 팀원 목록도 일괄 조회
    leader_team_ids_my = [tm.team_id for tm in my_tms if tm.is_leader and tm.team_id]
    team_members_map_my: dict = {}
    if leader_team_ids_my:
        all_tms_for_leader = db.query(TeamMember).filter(TeamMember.team_id.in_(leader_team_ids_my)).all()
        for t in all_tms_for_leader:
            team_members_map_my.setdefault(t.team_id, []).append(t)

    # 팀장 이벤트별 기존 결과 일괄 조회
    stage_keys_needed = []
    comp_event_map: dict = {}
    for tm in my_tms:
        if not tm.is_leader:
            continue
        comp = comps_map_my.get(tm.competition_id)
        if not comp:
            continue
        event = _next_upcoming_event(comp)
        if event:
            comp_event_map[tm.team_id] = (comp, event)
            stage_keys_needed.append((tm.team_id, event[0]))

    existing_results_map: dict = {}
    if stage_keys_needed:
        team_ids_q = [s[0] for s in stage_keys_needed]
        for tr in db.query(TeamResult).filter(TeamResult.team_id.in_(team_ids_q)).all():
            existing_results_map[(tr.team_id, tr.stage)] = tr

    leader_events = []
    for tm in my_tms:
        if not tm.is_leader:
            continue
        if tm.team_id not in comp_event_map:
            continue
        comp, event = comp_event_map[tm.team_id]
        team = teams_map_my.get(tm.team_id)
        leader_events.append({
            "comp": comp,
            "team": team,
            "tm": tm,
            "event": event,
            "existing_result": existing_results_map.get((tm.team_id, event[0])),
            "team_members": team_members_map_my.get(tm.team_id, []),
        })

    # 최근 알림 (최신 20개)
    notifications = (
        db.query(Notification)
        .filter(Notification.member_id == cm.id)
        .order_by(Notification.created_at.desc())
        .limit(20)
        .all()
    )
    notif_actor_ids = list({n.actor_id for n in notifications if n.actor_id})
    notif_actors = _member_map(db, notif_actor_ids)
    for n in notifications:
        n.actor = notif_actors.get(n.actor_id)

    # 전체 참여 내역 (과거 + 현재, 마감일 내림차순)
    past_projects = []
    seen_past: set = set()
    for tm in sorted(my_tms, key=lambda t: comps_map_my.get(t.competition_id, type("", (), {"deadline": _today()})()).deadline if hasattr(comps_map_my.get(t.competition_id, None), "deadline") else _today(), reverse=True):
        if tm.competition_id in seen_past:
            continue
        comp = comps_map_my.get(tm.competition_id)
        if comp and comp.deadline < today:  # 마감된 것만 (진행 중은 active_projects에 있음)
            team = teams_map_my.get(tm.team_id)
            past_projects.append({"comp": comp, "team": team, "tm": tm})
            seen_past.add(tm.competition_id)

    # 개인 갤러리 (내 게시물 전체 최신순)
    my_posts = (
        db.query(PersonalPost)
        .filter(PersonalPost.member_id == cm.id)
        .order_by(PersonalPost.created_at.desc())
        .all()
    )

    return _render(request, "my.html", _ctx(request, db,
        scrapped_comps=scrapped_comps,
        active_projects=active_projects,
        past_projects=past_projects,
        leader_events=leader_events,
        comp_stages=COMP_STAGES,
        notifications=notifications,
        my_posts=my_posts,
    ))


@app.post("/my/toggle-participation-history")
async def toggle_participation_history(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    cm.show_participation_history = not cm.show_participation_history
    db.commit()
    return RedirectResponse(url="/my#participation", status_code=303)


@app.get("/my/settings", response_class=HTMLResponse)
async def my_settings_page(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login?next=/my/settings", status_code=303)
    try:
        notif = json.loads(getattr(cm, "notif_settings", None) or "{}")
        if not isinstance(notif, dict):
            notif = {}
    except Exception:
        notif = {}
    return _render(request, "my_settings.html", _ctx(request, db,
        notif=notif,
        saved=request.query_params.get("saved"),
    ))


@app.post("/my/settings")
async def my_settings_save(
    request: Request,
    # 프라이버시
    show_birthday:              str = Form(""),
    show_participation_history: str = Form(""),
    # 팔로우/DM
    follow_auto_approve: str = Form(""),
    dm_allowed_from:     str = Form("all"),
    # 알림 설정 (체크박스 = 체크된 것만 값 있음)
    notif_birthday:   str = Form(""),
    notif_calendar:   str = Form(""),
    notif_chat:       str = Form(""),
    notif_admin:      str = Form(""),
    notif_push_all:   str = Form(""),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    cm.show_birthday              = bool(show_birthday)
    cm.show_participation_history = bool(show_participation_history)
    cm.follow_auto_approve        = bool(follow_auto_approve)
    cm.dm_allowed_from            = dm_allowed_from if dm_allowed_from in ("all","followers","none") else "all"
    notif = {
        "birthday": bool(notif_birthday),
        "calendar": bool(notif_calendar),
        "chat":     bool(notif_chat),
        "admin":    bool(notif_admin),
        "push_all": bool(notif_push_all),
    }
    cm.notif_settings = json.dumps(notif, ensure_ascii=False)
    db.commit()
    return RedirectResponse(url="/my/settings?saved=1", status_code=303)


@app.post("/my/toggle-birthday-public")
async def toggle_birthday_public(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    cm.show_birthday = not getattr(cm, "show_birthday", True)
    db.commit()
    return RedirectResponse(url=_safe_referer(request, "/my"), status_code=303)


# ── 개인 갤러리 ───────────────────────────────────────────────────────────────

@app.post("/my/post/new")
async def personal_post_new(
    request: Request,
    caption: str = Form(""),
    images: list[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    # 이미지 5장 제한
    valid_images = [img for img in images if img and img.filename][:5]

    saved = []
    for img in valid_images:
        data = await img.read()
        if len(data) > MAX_IMAGE_SIZE:
            continue   # 10MB 초과 이미지 스킵
        if not data:
            continue
        ext = img.filename.rsplit(".", 1)[-1].lower()
        if ext not in {"jpg", "jpeg", "png", "gif", "webp"}:
            continue
        data, _ = _optimize_image(data)
        fname = f"pp_{cm.id}_{uuid.uuid4().hex[:12]}.jpg"
        _storage_upload(data, fname)
        saved.append(fname)

    post = PersonalPost(
        member_id=cm.id,
        caption=caption.strip()[:500],
        images=json.dumps(saved, ensure_ascii=False),
    )
    db.add(post)
    db.commit()
    return RedirectResponse(url="/my#gallery", status_code=303)


@app.post("/my/post/{post_id}/delete")
async def personal_post_delete(request: Request, post_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    post = db.query(PersonalPost).filter(
        PersonalPost.id == post_id, PersonalPost.member_id == cm.id
    ).first()
    if post:
        import json as _json
        for fname in _json.loads(post.images or "[]"):
            try: (UPLOAD_DIR / fname).unlink()
            except Exception: pass
        db.delete(post)
        db.commit()
    return RedirectResponse(url="/my#gallery", status_code=303)


@app.get("/my/calendar", response_class=HTMLResponse)
async def my_calendar(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login?next=/my/calendar", status_code=303)

    today = _today()

    # ── 참여 중인 공모전 (TeamMember → Competition)
    my_tms = (
        db.query(TeamMember)
        .filter(or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name))
        .all()
    )
    my_comp_ids = list({tm.competition_id for tm in my_tms if tm.competition_id})
    my_team_ids = list({tm.team_id for tm in my_tms if tm.team_id})

    participating_comps = _annotate(
        db.query(Competition).filter(Competition.id.in_(my_comp_ids)).all()
    ) if my_comp_ids else []
    teams_map = {t.id: t for t in db.query(Team).filter(Team.id.in_(my_team_ids)).all()} if my_team_ids else {}
    tms_by_comp = {}
    for tm in my_tms:
        tms_by_comp.setdefault(tm.competition_id, tm)

    # ── 관심(스크랩) 공모전
    scrap_ids = [s.competition_id for s in
                 db.query(CompetitionScrap).filter(CompetitionScrap.member_id == cm.id).all()]
    interested_comps = _annotate(
        db.query(Competition).filter(
            Competition.id.in_(scrap_ids),
            Competition.id.notin_(my_comp_ids)  # 참여 중인 건 중복 제외
        ).all()
    ) if scrap_ids else []

    # ── 날짜 이벤트 목록 조합
    import json as _json
    events = []  # [{date, label, comp, type, past}]

    def _add_event(date_val, label, comp, ev_type, team=None):
        if date_val:
            events.append({
                "date": date_val,
                "label": label,
                "comp": comp,
                "type": ev_type,   # deadline / review / announce / award / interest
                "team": team,
                "past": date_val < today,
            })

    for comp in participating_comps:
        tm = tms_by_comp.get(comp.id)
        team = teams_map.get(tm.team_id) if tm else None
        _add_event(comp.deadline, "마감", comp, "deadline", team)
        _add_event(comp.announcement_date, "발표", comp, "announce", team)
        _add_event(comp.award_date, "시상", comp, "award", team)
        # review_dates JSON
        try:
            for rd in _json.loads(comp.review_dates or "[]"):
                if rd.get("date"):
                    import datetime as _dt
                    rd_date = _dt.date.fromisoformat(rd["date"])
                    _add_event(rd_date, rd.get("label", "심사"), comp, "review", team)
        except Exception:
            pass
        _add_event(comp.review_1_date, "1차 심사", comp, "review", team)
        _add_event(comp.review_2_date, "2차 심사", comp, "review", team)

    for comp in interested_comps:
        _add_event(comp.deadline, "마감", comp, "interest")

    # 날짜 오름차순 정렬 (지난 것 뒤로)
    events.sort(key=lambda e: (e["past"], e["date"]))

    return _render(request, "my_calendar.html", _ctx(request, db,
        events=events,
        today=today,
        participating_count=len(participating_comps),
        interested_count=len(interested_comps),
    ))


# ════════════════════════════════════════════════════════════════════════════
#  관리자 — 인증 / 대시보드
# ════════════════════════════════════════════════════════════════════════════

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request, db: Session = Depends(get_db)):
    if _is_admin(request):
        return RedirectResponse(url="/admin", status_code=303)
    return _render(request, "admin/login.html", _ctx(request, db, error=None))


@app.post("/admin/login")
async def admin_login(request: Request, password: str = Form(...), db: Session = Depends(get_db)):
    client_ip = request.client.host if request.client else "unknown"

    if _is_locked(_admin_fail_count, client_ip, max_fail=5):
        return _render(request, "admin/login.html", _ctx(request, db, error="너무 많은 로그인 시도입니다. 5분 후 다시 시도하세요."), status_code=429)

    if hmac.compare_digest(password, ADMIN_PASSWORD):
        _admin_fail_count.pop(client_ip, None)
        resp = RedirectResponse(url="/admin", status_code=303)
        resp.set_cookie("admin_token", create_token(), httponly=True, max_age=86400, samesite="lax", secure=IS_PRODUCTION)
        return resp

    _record_fail(_admin_fail_count, client_ip)
    return _render(request,
        "admin/login.html",
        _ctx(request, db, error="비밀번호가 올바르지 않습니다."),
        status_code=401,
    )


@app.get("/admin/logout")
async def admin_logout():
    resp = RedirectResponse(url="/", status_code=303)
    resp.delete_cookie("admin_token")
    return resp


@app.get("/admin/debug/storage")
async def admin_debug_storage(request: Request, db: Session = Depends(get_db)):
    """Volume 마운트 및 파일 저장 상태 진단 (관리자 전용)"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    try:
        files = list(UPLOAD_DIR.iterdir()) if UPLOAD_DIR.exists() else []
        file_list = sorted([f.name for f in files if f.is_file()])
    except Exception as e:
        file_list = [f"ERROR: {e}"]
    return JSONResponse({
        "upload_dir": str(UPLOAD_DIR),
        "exists": UPLOAD_DIR.exists(),
        "is_absolute": UPLOAD_DIR.is_absolute(),
        "file_count": len(file_list),
        "files": file_list[:30],  # 최대 30개만 표시
    })


@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, db: Session = Depends(get_db)):
    """관리자 허브 페이지 — 4개 섹션 카드 표시"""
    if r := _privileged_redirect(request, db):
        return r
    # 허브에서 간단한 통계 표시
    comp_count    = db.query(Competition).count()
    job_count     = db.query(JobPosting).count()
    member_count  = db.query(Member).count()
    return _render(request, "admin/dashboard.html", _ctx(request, db,
        comp_count=comp_count,
        job_count=job_count,
        member_count=member_count,
    ))


@app.get("/admin/competitions", response_class=HTMLResponse)
async def admin_competitions(
    request: Request,
    db: Session = Depends(get_db),
    bulk_added: int = 0,
    bulk_errors: int = 0,
):
    """공모전 관리 페이지"""
    if r := _privileged_redirect(request, db):
        return r
    competitions = _annotate(db.query(Competition).order_by(Competition.deadline.asc()).all())
    return _render(request, "admin/competitions.html", _ctx(request, db,
        competitions=competitions,
        today=_today(),
        bulk_added=bulk_added,
        bulk_errors=bulk_errors,
    ))


@app.post("/admin/competition/{comp_id}/toggle-featured")
async def admin_toggle_featured(
    comp_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """공모전 주목(⭐) 토글 — 관리자 대시보드에서 캐러셀 노출 여부 제어"""
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if comp:
        comp.is_featured = not bool(comp.is_featured)
        db.commit()
    return RedirectResponse(url="/admin/competitions", status_code=303)


@app.post("/admin/competition/{comp_id}/toggle-active")
async def admin_toggle_active(
    comp_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """공모전 활성/비활성 토글 — 비활성 상태면 일반 사용자에게 숨김"""
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if comp:
        comp.is_active = not bool(comp.is_active if comp.is_active is not None else True)
        db.commit()
    return RedirectResponse(url="/admin/competitions", status_code=303)


# ── 공모전 CRUD ───────────────────────────────────────────────────────────────

@app.get("/admin/add", response_class=HTMLResponse)
async def admin_add_page(request: Request, db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    return _render(request,
        "admin/form.html",
        _ctx(request, db, comp=None, tags=_get_tags(db), action="/admin/add", title="공모전 추가",
             review_dates_json="[]"),
    )


@app.post("/admin/add")
async def admin_add(
    request: Request,
    title: str = Form(...),
    organizer: str = Form(""),
    tags: List[str] = Form(default=[]),
    start_date: Optional[str] = Form(None),
    deadline: str = Form(...),
    announcement_date: Optional[str] = Form(None),
    award_date: Optional[str] = Form(None),
    review_dates_json: str = Form("[]"),
    prize: str = Form(""),
    link: str = Form(""),
    description: str = Form(""),
    is_featured: bool = Form(False),
    max_members: Optional[str] = Form(None),
    stage_override: Optional[str] = Form(None),
    submission_docs: List[str] = Form(default=[]),
    submission_docs_extra: str = Form(""),
    comp_image_path: Optional[str] = Form(None),
    comp_image: Optional[UploadFile] = File(None),
    files: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    if r := _privileged_redirect(request, db):
        return r
    _safe_path = Path(comp_image_path).name if comp_image_path else None
    image = await _save_image(comp_image) or _safe_path or None
    try:
        _review_dates = json.loads(review_dates_json or "[]")
        if not isinstance(_review_dates, list):
            _review_dates = []
    except Exception:
        _review_dates = []
    _extra_docs = [d.strip() for d in submission_docs_extra.split(",") if d.strip()]
    _all_docs = list(submission_docs) + [d for d in _extra_docs if d not in submission_docs]
    comp = Competition(
        title=title, organizer=organizer,
        tags=json.dumps(tags, ensure_ascii=False),
        start_date=date.fromisoformat(start_date) if start_date else None,
        deadline=date.fromisoformat(deadline),
        announcement_date=date.fromisoformat(announcement_date) if announcement_date else None,
        review_dates=json.dumps(_review_dates, ensure_ascii=False),
        award_date=date.fromisoformat(award_date) if award_date else None,
        prize=prize, link=link, description=description,
        image=image, max_members=_optional_int(max_members, "최대 팀 인원"), is_featured=is_featured,
        stage_override=stage_override.strip() if stage_override and stage_override.strip() else None,
        submission_docs=json.dumps(_all_docs, ensure_ascii=False),
        files=json.dumps(await _save_files(files), ensure_ascii=False),
    )
    db.add(comp)
    db.commit()
    return RedirectResponse(url=f"/admin/edit/{comp.id}", status_code=303)


@app.get("/admin/edit/{comp_id}", response_class=HTMLResponse)
async def admin_edit_page(request: Request, comp_id: int, db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)
    comp.tags_list = _from_json(comp.tags)
    comp.files_list = _from_json(comp.files)
    # review_dates JSON → 편집용 리스트
    review_dates_list = _from_json(comp.review_dates or "[]")
    # 구버전 호환: review_dates 비어있으면 review_1_date/review_2_date 에서 마이그레이션
    if not review_dates_list:
        if comp.review_1_date:
            review_dates_list.append({"label": "1차 심사", "date": comp.review_1_date.isoformat()})
        if comp.review_2_date:
            review_dates_list.append({"label": "2차 심사", "date": comp.review_2_date.isoformat()})
    comp.review_dates_json = json.dumps(review_dates_list, ensure_ascii=False)
    return _render(request,
        "admin/form.html",
        _ctx(request, db, comp=comp, tags=_get_tags(db), action=f"/admin/edit/{comp_id}", title="공모전 수정"),
    )


@app.post("/admin/edit/{comp_id}")
async def admin_edit(
    request: Request, comp_id: int,
    title: str = Form(...), organizer: str = Form(""),
    tags: List[str] = Form(default=[]),
    start_date: Optional[str] = Form(None), deadline: str = Form(...),
    announcement_date: Optional[str] = Form(None),
    award_date: Optional[str] = Form(None),
    review_dates_json: str = Form("[]"),
    prize: str = Form(""), link: str = Form(""), description: str = Form(""),
    is_featured: bool = Form(False), max_members: Optional[str] = Form(None),
    stage_override: Optional[str] = Form(None),
    submission_docs: List[str] = Form(default=[]),
    submission_docs_extra: str = Form(""),
    comp_image_path: Optional[str] = Form(None),
    comp_image: Optional[UploadFile] = File(None),
    # image_changed="yes" 일 때만 GPT 파싱 이미지 반영 (기본: 기존 이미지 보존)
    image_changed: str = Form("no"),
    delete_image: str = Form("no"),
    files: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)

    new_image = await _save_image(comp_image)
    _safe_path = Path(comp_image_path).name if comp_image_path and comp_image_path.strip() else None
    existing_files = _from_json(comp.files)
    comp.title = title; comp.organizer = organizer
    comp.tags = json.dumps(tags, ensure_ascii=False)
    comp.start_date = date.fromisoformat(start_date) if start_date else None
    comp.deadline = date.fromisoformat(deadline)
    comp.announcement_date = date.fromisoformat(announcement_date) if announcement_date else None
    comp.award_date = date.fromisoformat(award_date) if award_date else None
    try:
        _review_dates = json.loads(review_dates_json or "[]")
        if not isinstance(_review_dates, list):
            _review_dates = []
    except Exception:
        _review_dates = []
    comp.review_dates = json.dumps(_review_dates, ensure_ascii=False)
    comp.prize = prize; comp.link = link; comp.description = description
    comp.is_featured = is_featured; comp.max_members = _optional_int(max_members, "최대 팀 인원")
    comp.stage_override = stage_override.strip() if stage_override and stage_override.strip() else None
    # 필수 제출 서류: 체크박스 목록 + 직접 입력 합산
    _extra_docs = [d.strip() for d in submission_docs_extra.split(",") if d.strip()]
    _all_docs = list(submission_docs) + [d for d in _extra_docs if d not in submission_docs]
    comp.submission_docs = json.dumps(_all_docs, ensure_ascii=False)

    # ── 이미지 처리 우선순위 ────────────────────────────────────────────────
    # 1) 새 파일 직접 업로드 → 교체
    # 2) 이미지 삭제 체크박스 → None
    # 3) GPT 파싱 결과 (image_changed="yes") → 교체
    # 4) 그 외 → 기존 DB 값 반드시 유지 (hidden field 의존하지 않음)
    if new_image:
        _delete_upload(comp.image)
        comp.image = new_image
    elif delete_image == "yes":
        _delete_upload(comp.image)
        comp.image = None
    elif image_changed == "yes" and _safe_path:
        _delete_upload(comp.image)
        comp.image = _safe_path
    # else: comp.image 절대 건드리지 않음 (기존 DB 값 보존)

    comp.files = json.dumps(existing_files + await _save_files(files), ensure_ascii=False)
    db.commit()
    return RedirectResponse(url="/admin/competitions", status_code=303)


@app.post("/admin/delete/{comp_id}")
async def admin_delete(request: Request, comp_id: int, db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if comp:
        for item in _from_json(comp.files):
            try:
                _storage_delete(item["path"])
            except OSError:
                pass
        db.delete(comp)
        db.commit()
    return RedirectResponse(url="/admin/competitions", status_code=303)


@app.post("/admin/delete-bulk")
async def admin_delete_bulk(
    request: Request,
    ids: list[int] = Form(...),
    db: Session = Depends(get_db),
):
    """여러 공모전 일괄 삭제"""
    if r := _privileged_redirect(request, db):
        return r
    for comp_id in ids:
        comp = db.query(Competition).filter(Competition.id == comp_id).first()
        if not comp:
            continue
        for item in _from_json(comp.files):
            _storage_delete(item.get("path") or item.get("saved_name", ""))
        if comp.image:
            _storage_delete(comp.image)
        db.delete(comp)
    db.commit()
    return RedirectResponse(url="/admin/competitions", status_code=303)


@app.post("/admin/delete-file/{comp_id}")
async def delete_file(request: Request, comp_id: int, filename: str = Form(...), db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if comp:
        safe_name = Path(filename).name
        updated = [f for f in _from_json(comp.files) if f.get("path") != safe_name]
        comp.files = json.dumps(updated, ensure_ascii=False)
        db.commit()
        try:
            _storage_delete(safe_name)
        except OSError:
            pass
    return RedirectResponse(url=f"/admin/edit/{comp_id}", status_code=303)


# ── GPT 파싱 API ──────────────────────────────────────────────────────────────

@app.post("/admin/api/parse")
async def api_parse(request: Request, text: str = Form(...), db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    try:
        return JSONResponse(await parse_text(text))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/admin/api/parse-image")
async def api_parse_image(request: Request, image: UploadFile = File(...), db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    try:
        data = await image.read()
        if not data:
            raise HTTPException(status_code=400, detail="빈 파일입니다.")
        ext = Path(image.filename).suffix.lower() if image.filename else ".jpg"
        if ext not in ALLOWED_IMAGE_EXT:
            raise HTTPException(status_code=400, detail="이미지 파일만 업로드 가능합니다.")
        if not _is_valid_image_bytes(data):
            raise HTTPException(status_code=400, detail="유효하지 않은 이미지 파일입니다.")
        if len(data) > MAX_IMAGE_SIZE:
            raise HTTPException(status_code=413, detail=f"이미지 크기가 {MAX_IMAGE_SIZE // 1024 // 1024}MB를 초과했습니다.")
        stored_name = f"{uuid.uuid4().hex}{ext}"
        _storage_upload(data, stored_name, image.content_type or "image/jpeg")
        result = await parse_image_file(data, image.content_type)
        result["_image_path"] = stored_name
        return JSONResponse(result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/admin/api/parse-document")
async def api_parse_document(request: Request, document: UploadFile = File(...), db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    try:
        data = await document.read()
        result = await parse_document_file(data, document.filename or "file.pdf")
        return JSONResponse(result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── 회원 관리 ─────────────────────────────────────────────────────────────────

@app.get("/admin/members", response_class=HTMLResponse)
async def admin_members(request: Request, q: str = Query(default=""), db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    query = db.query(Member).order_by(Member.created_at.asc())
    if q.strip():
        term = f"%{q.strip()}%"
        query = query.filter(
            or_(Member.activity_name.ilike(term), Member.real_name.ilike(term))
        )
    members = query.all()

    # 초대코드 정보 조회 (삭제된 코드도 표시 위해 members의 invite_code_used 기준)
    all_invite_codes = {c.code: c for c in db.query(InviteCode).all()}

    # 그룹화
    groups_dict = defaultdict(list)
    for m in members:
        groups_dict[m.invite_code_used or ""].append(m)

    code_groups = []
    for code_val in sorted(groups_dict.keys(), key=lambda x: (x == "", x)):
        mlist = groups_dict[code_val]
        code_obj = all_invite_codes.get(code_val) if code_val else None
        if not code_val:
            label = "초대 코드 없음"
            note = ""
            code_exists = False
        elif code_obj:
            label = code_obj.note or code_val
            note = code_val
            code_exists = True
        else:
            label = f"삭제된 코드"
            note = code_val
            code_exists = False
        code_groups.append({
            "code": code_val,
            "label": label,
            "note": note,
            "exists": code_exists,
            "count": len(mlist),
            "members": mlist,
        })

    return _render(request, "admin/members.html", _ctx(request, db,
        members=members, code_groups=code_groups, query=q, now=_now(),
        admin_permissions=ADMIN_PERMISSIONS,
    ))


@app.post("/admin/members/set-generation")
async def admin_set_generation(
    request: Request,
    member_ids: List[int] = Form(default=[]),
    generation: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    """선택된 멤버 ID 목록에 기수를 일괄 지정"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    if member_ids:
        db.query(Member).filter(Member.id.in_(member_ids)).update(
            {Member.generation: generation}, synchronize_session=False
        )
        db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/set-role")
async def admin_set_role(request: Request, member_id: int, role: str = Form(...), db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m and role in ("member", "sub_admin"):
        m.role = role
        db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/edit-activity-name")
async def admin_edit_activity_name(
    request: Request, member_id: int,
    activity_name: str = Form(...),
    db: Session = Depends(get_db),
):
    """관리자 전용 — 회원 활동명(ID) 변경"""
    if r := _privileged_redirect(request, db):
        return r
    new_name = activity_name.strip()
    if not new_name or len(new_name) > 16:
        return RedirectResponse(url="/admin/members", status_code=303)
    m = db.query(Member).filter(Member.id == member_id).first()
    if m:
        duplicate = db.query(Member).filter(Member.activity_name == new_name, Member.id != member_id).first()
        if not duplicate:
            m.activity_name = new_name
            db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/set-permissions")
async def admin_set_permissions(
    request: Request, member_id: int,
    permissions: List[str] = Form(default=[]),
    db: Session = Depends(get_db),
):
    """메인 관리자 전용 — 중간관리자 권한 설정 (AJAX/폼 양쪽 지원)"""
    if r := _admin_redirect(request):
        # AJAX 요청이면 JSON 오류 반환
        accept = request.headers.get("accept", "")
        if "application/json" in accept or request.headers.get("x-requested-with") == "XMLHttpRequest":
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m and m.role == "sub_admin":
        valid_keys = {k for k, _ in ADMIN_PERMISSIONS}
        filtered = [p for p in permissions if p in valid_keys]
        m.permissions = json.dumps(filtered, ensure_ascii=False)
        db.commit()
    # AJAX 요청 감지 → JSON 응답
    accept = request.headers.get("accept", "")
    if "application/json" in accept or request.headers.get("x-requested-with") == "XMLHttpRequest":
        from fastapi.responses import JSONResponse
        return JSONResponse({"ok": True})
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/edit-name")
async def admin_edit_member_name(
    request: Request, member_id: int,
    real_name: str = Form(...),
    db: Session = Depends(get_db),
):
    if r := _privileged_redirect(request, db):
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m:
        m.real_name = real_name.strip()
        db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/delete")
async def admin_delete_member(request: Request, member_id: int, db: Session = Depends(get_db)):
    # 회원 삭제는 최고 관리자만 가능 (sub_admin 제외)
    if r := _admin_redirect(request):
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m:
        db.delete(m)
        db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/mute-comments")
async def admin_mute_member_comments(
    request: Request,
    member_id: int,
    duration_minutes: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    if r := _privileged_redirect(request, db):
        return r
    member = db.query(Member).filter(Member.id == member_id).first()
    if member:
        minutes = _optional_int(duration_minutes, "댓글 금지 시간")
        member.comment_muted_until = (_now() + timedelta(minutes=minutes)) if minutes and minutes > 0 else None
        db.commit()
    return RedirectResponse(url="/admin/members", status_code=303)


# ── 초대 코드 ─────────────────────────────────────────────────────────────────

@app.get("/admin/invite-codes", response_class=HTMLResponse)
async def admin_invite_codes(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    codes = db.query(InviteCode).order_by(InviteCode.created_at.desc()).all()
    code_ids = [c.id for c in codes]
    logs = (
        db.query(InviteCodeUseLog)
        .filter(InviteCodeUseLog.invite_code_id.in_(code_ids))
        .order_by(InviteCodeUseLog.used_at.desc())
        .all()
    ) if code_ids else []
    logs_by_code: dict[int, list[InviteCodeUseLog]] = defaultdict(list)
    for log in logs:
        logs_by_code[log.invite_code_id].append(log)
    used_ids = [c.used_by_member_id for c in codes if c.used_by_member_id] + [log.member_id for log in logs if log.member_id]
    members_map = {}
    if used_ids:
        for m in db.query(Member).filter(Member.id.in_(used_ids)).all():
            members_map[m.id] = m.activity_name
    return _render(request,
        "admin/invite_codes.html",
        _ctx(request, db, codes=codes, logs_by_code=logs_by_code, members_map=members_map, now=_now()),
    )


@app.post("/admin/invite-codes/create")
async def admin_create_invite_code(
    request: Request,
    note: str = Form(""),
    code_type: str = Form("personal"),
    max_uses: Optional[str] = Form(None),
    valid_days: Optional[str] = Form(None),
    expires_at: Optional[str] = Form(None),
    generation: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    code_type = code_type if code_type in ("personal", "group") else "personal"
    parsed_max_uses = 1 if code_type == "personal" else _optional_int(max_uses, "최대 사용 인원")
    if code_type == "group" and (not parsed_max_uses or parsed_max_uses < 1):
        raise HTTPException(status_code=400, detail="단체 초대 코드는 최대 사용 인원을 1명 이상으로 입력해야 합니다.")
    parsed_gen = _optional_int(generation, "기수") if generation and generation.strip() else None
    db.add(InviteCode(
        code=secrets.token_urlsafe(12),
        note=note.strip(),
        code_type=code_type,
        max_uses=parsed_max_uses,
        expires_at=_parse_expiry(valid_days, expires_at),
        use_count=0,
        is_active=True,
        generation=parsed_gen,
        created_at=_now(),
    ))
    db.commit()
    return RedirectResponse(url="/admin/invite-codes", status_code=303)


@app.post("/admin/invite-codes/delete/{code_id}")
async def admin_delete_invite_code(request: Request, code_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    code = db.query(InviteCode).filter(InviteCode.id == code_id).first()
    if code:
        db.delete(code)
        db.commit()
    return RedirectResponse(url="/admin/invite-codes", status_code=303)


@app.post("/admin/invite-codes/logs/{log_id}/kick")
async def admin_kick_invite_member(request: Request, log_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    log = db.query(InviteCodeUseLog).filter(InviteCodeUseLog.id == log_id).first()
    if not log or log.revoked_at:
        return RedirectResponse(url="/admin/invite-codes", status_code=303)
    code = db.query(InviteCode).filter(InviteCode.id == log.invite_code_id).first()
    member = db.query(Member).filter(Member.id == log.member_id).first() if log.member_id else None
    if member:
        db.delete(member)
    log.revoked_at = _now()
    log.revoked_by = "admin"
    if code and code.code_type == "group" and (code.use_count or 0) > 0:
        code.use_count -= 1
    db.commit()
    return RedirectResponse(url="/admin/invite-codes", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  회원 — 가입 / 로그인 / 로그아웃 / 프로필
# ════════════════════════════════════════════════════════════════════════════

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, db: Session = Depends(get_db)):
    if _current_member(request, db):
        return RedirectResponse(url="/", status_code=303)
    return _render(request, "register.html", _ctx(request, db, error=None))


@app.post("/register")
async def register(
    request: Request,
    invite_code: str = Form(...), activity_name: str = Form(...), real_name: str = Form(...),
    student_id: str = Form(""), phone: str = Form(""),
    password: str = Form(...), bio: str = Form(""),
    birthday_month: str = Form(""), birthday_day: str = Form(""),
    db: Session = Depends(get_db),
):
    def err(msg):
        return _render(request, "register.html", _ctx(request, db, error=msg), status_code=400)

    # with_for_update(): 동시 가입 시 같은 코드 중복 사용 방지 (row-level lock)
    code_obj = db.query(InviteCode).filter(InviteCode.code == invite_code.strip()).with_for_update().first()
    if not code_obj:
        return err("초대 코드가 올바르지 않습니다.")
    if not code_obj.is_active:
        return err("비활성화된 초대 코드입니다.")
    if code_obj.expires_at and _now() > code_obj.expires_at:
        return err("만료된 초대 코드입니다.")
    code_type = code_obj.code_type or "personal"
    if code_type == "personal" and code_obj.used_by_member_id:
        return err("이미 사용된 개인 초대 코드입니다.")
    if code_type == "group" and code_obj.max_uses and (code_obj.use_count or 0) >= code_obj.max_uses:
        return err("단체 초대 코드 사용 가능 인원이 모두 찼습니다.")
    if db.query(Member).filter(Member.activity_name == activity_name.strip()).first():
        return err("이미 사용 중인 활동명입니다.")
    if len(password) < 6:
        return err("비밀번호는 최소 6자 이상이어야 합니다.")

    # 생일 처리
    _birthday = None
    if birthday_month and birthday_day:
        try:
            _mm = int(birthday_month); _dd = int(birthday_day)
            if 1 <= _mm <= 12 and 1 <= _dd <= 31:
                _birthday = f"{_mm:02d}-{_dd:02d}"
        except ValueError:
            pass

    member = Member(
        activity_name=activity_name.strip(), real_name=real_name.strip(),
        student_id=student_id.strip(), phone=phone.strip(),
        password_hash=hash_password(password), bio=bio.strip(),
        invite_code_used=invite_code.strip(),
        generation=code_obj.generation if code_obj.generation else None,
        birthday=_birthday,
    )
    db.add(member)
    db.flush()
    db.add(InviteCodeUseLog(
        invite_code_id=code_obj.id,
        member_id=member.id,
        activity_name=member.activity_name,
        real_name=member.real_name,
        used_at=_now(),
    ))
    code_obj.use_count = (code_obj.use_count or 0) + 1
    if code_type == "personal":
        code_obj.used_by_member_id = member.id
        code_obj.is_active = False
    db.commit()

    resp = RedirectResponse(url="/", status_code=303)
    resp.set_cookie("member_token", create_member_token(member.id), httponly=True, max_age=604800, samesite="lax", secure=IS_PRODUCTION)
    return resp


@app.get("/member/login", response_class=HTMLResponse)
async def member_login_page(request: Request, next: str = "", db: Session = Depends(get_db)):
    if _current_member(request, db):
        return RedirectResponse(url="/", status_code=303)
    return _render(request, "member_login.html", _ctx(request, db, error=None, next=next))


@app.post("/member/login")
async def member_login(
    request: Request,
    activity_name: str = Form(...),
    password: str = Form(...),
    next: str = Form(""),
    db: Session = Depends(get_db),
):
    client_ip = request.client.host if request.client else "unknown"

    if _is_locked(_member_fail_count, client_ip):
        return _render(request, "member_login.html", _ctx(request, db, error="너무 많은 로그인 시도입니다. 잠시 후 다시 시도하세요.", next=next), status_code=429)

    m = db.query(Member).filter(Member.activity_name == activity_name.strip()).first()
    if not m or not verify_password(password, m.password_hash):
        _record_fail(_member_fail_count, client_ip)
        return _render(request,
            "member_login.html",
            _ctx(request, db, error="활동명 또는 비밀번호가 올바르지 않습니다.", next=next),
            status_code=401,
        )

    _member_fail_count.pop(client_ip, None)
    # next URL 검증: 같은 호스트의 상대 경로만 허용
    redirect_url = next if (next and next.startswith("/") and not next.startswith("//")) else "/"
    resp = RedirectResponse(url=redirect_url, status_code=303)
    resp.set_cookie("member_token", create_member_token(m.id), httponly=True, max_age=604800, samesite="lax", secure=IS_PRODUCTION)
    return resp


@app.get("/member/logout")
async def member_logout():
    resp = RedirectResponse(url="/", status_code=303)
    resp.delete_cookie("member_token")
    return resp


@app.get("/profile/me", response_class=HTMLResponse)
async def profile_me(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    return RedirectResponse(url=f"/profile/{cm.activity_name}", status_code=303)


@app.get("/profile/{activity_name}", response_class=HTMLResponse)
async def profile_view(request: Request, activity_name: str, db: Session = Depends(get_db)):
    target = db.query(Member).filter(Member.activity_name == activity_name).first()
    if not target:
        raise HTTPException(status_code=404, detail="회원을 찾을 수 없습니다.")
    cm = _current_member(request, db)

    # 포트폴리오: 이 회원이 참여한 팀원 레코드 (최신순)
    team_rows = (
        db.query(TeamMember)
        .filter(TeamMember.member_id == target.id)
        .order_by(TeamMember.created_at.desc())
        .all()
    )
    comp_ids = list({t.competition_id for t in team_rows})
    comps_map: dict = {}
    if comp_ids:
        for c in db.query(Competition).filter(Competition.id.in_(comp_ids)).all():
            comps_map[c.id] = c
    total     = len(team_rows)
    submitted = sum(1 for t in team_rows if t.is_participant)
    awarded   = sum(1 for t in team_rows if t.award_rank)

    # 단계 결과 맵 (team_id → list of results with label)
    team_ids_for_profile = [t.team_id for t in team_rows if t.team_id]
    stage_results_raw = (
        db.query(TeamResult).filter(TeamResult.team_id.in_(team_ids_for_profile)).all()
        if team_ids_for_profile else []
    )
    stage_label_map = {s[0]: s[2] for s in COMP_STAGES}
    stage_results_map: dict = {}
    for sr in stage_results_raw:
        sr.stage_label = stage_label_map.get(sr.stage, sr.stage)
        stage_results_map.setdefault(sr.team_id, []).append(sr)

    # 팔로우 상태
    follow_status = None   # None / "pending" / "approved" / "self"
    follow_obj = None
    if cm:
        if cm.id == target.id:
            follow_status = "self"
        else:
            fq = db.query(Follow).filter(
                Follow.follower_id == cm.id, Follow.following_id == target.id
            ).first()
            if fq:
                follow_status = fq.status
                follow_obj = fq

    # 팔로워/팔로잉 수
    follower_count  = db.query(Follow).filter(Follow.following_id == target.id, Follow.status == "approved").count()
    following_count = db.query(Follow).filter(Follow.follower_id == target.id, Follow.status == "approved").count()

    # 외부 이력
    external_achievements = (
        db.query(ExternalAchievement)
        .filter(ExternalAchievement.member_id == target.id)
        .order_by(ExternalAchievement.achieved_year.desc().nullslast(), ExternalAchievement.created_at.desc())
        .all()
    )

    # skills/links 파싱
    target_skills = _from_json(target.skills)
    target_links  = _from_json(target.links)

    # 참여 내역 공개 여부 반영 (본인이거나 공개 설정된 경우에만 노출)
    is_own = bool(cm and cm.id == target.id)
    show_participation = is_own or getattr(target, "show_participation_history", True)

    # 개인 갤러리 (공개 게시물만 — 본인이면 전체)
    posts_query = db.query(PersonalPost).filter(PersonalPost.member_id == target.id)
    if not is_own:
        posts_query = posts_query.filter(PersonalPost.is_public.is_(True))
    personal_posts = posts_query.order_by(PersonalPost.created_at.desc()).all()

    # 시간표: 공개 범위에 따라 노출
    tt_vis = getattr(target, "timetable_visibility", "members") or "members"
    can_see_tt = is_own or tt_vis == "public" or (tt_vis == "members" and cm)
    timetable_entries = (
        db.query(TimetableEntry).filter(TimetableEntry.member_id == target.id).all()
        if can_see_tt else []
    )

    return _render(request,
        "profile.html",
        _ctx(request, db, target=target, is_own=is_own,
             team_rows=team_rows if show_participation else [],
             comps_map=comps_map,
             stats={"total": total, "submitted": submitted, "awarded": awarded},
             stage_results_map=stage_results_map,
             follow_status=follow_status, follow_obj=follow_obj,
             follower_count=follower_count, following_count=following_count,
             external_achievements=external_achievements,
             target_skills=target_skills, target_links=target_links,
             show_participation=show_participation,
             personal_posts=personal_posts,
             timetable_entries=timetable_entries,
             timetable_visibility=tt_vis,
             can_see_tt=can_see_tt),
    )


_TT_COLORS = ["teal", "blue", "green", "purple", "orange", "pink"]
_TT_DAYS   = [("mon","월"),("tue","화"),("wed","수"),("thu","목"),("fri","금")]


@app.get("/timetable", response_class=HTMLResponse)
async def timetable_edit_page(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    entries = db.query(TimetableEntry).filter(TimetableEntry.member_id == cm.id).all()
    return _render(request, "timetable/edit.html",
                   _ctx(request, db, entries=entries, tt_days=_TT_DAYS,
                        tt_colors=_TT_COLORS,
                        tt_visibility=getattr(cm, "timetable_visibility", "members") or "members"))


@app.post("/timetable/add")
async def timetable_add(
    request: Request,
    subject_name: str = Form(...),
    day: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    location: str = Form(""),
    color: str = Form("teal"),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=403)
    if day not in {d for d, _ in _TT_DAYS}:
        raise HTTPException(status_code=400)
    if color not in _TT_COLORS:
        color = "teal"
    db.add(TimetableEntry(
        member_id=cm.id,
        subject_name=subject_name.strip()[:100],
        day=day,
        start_time=start_time,
        end_time=end_time,
        location=location.strip()[:100],
        color=color,
    ))
    db.commit()
    return RedirectResponse(url="/timetable", status_code=303)


@app.post("/timetable/{entry_id}/delete")
async def timetable_delete(request: Request, entry_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=403)
    entry = db.query(TimetableEntry).filter(
        TimetableEntry.id == entry_id, TimetableEntry.member_id == cm.id
    ).first()
    if entry:
        db.delete(entry)
        db.commit()
    return RedirectResponse(url="/timetable", status_code=303)


@app.get("/timetable/{entry_id}/edit", response_class=HTMLResponse)
async def timetable_entry_edit_form(request: Request, entry_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    entry = db.query(TimetableEntry).filter(
        TimetableEntry.id == entry_id, TimetableEntry.member_id == cm.id
    ).first()
    if not entry:
        raise HTTPException(status_code=404)
    return _render(request, "timetable/entry_edit.html",
                   _ctx(request, db, entry=entry, tt_days=_TT_DAYS, tt_colors=_TT_COLORS))


@app.post("/timetable/{entry_id}/edit")
async def timetable_entry_edit_submit(
    request: Request,
    entry_id: int,
    subject_name: str = Form(...),
    day: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    location: str = Form(""),
    color: str = Form("teal"),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=403)
    entry = db.query(TimetableEntry).filter(
        TimetableEntry.id == entry_id, TimetableEntry.member_id == cm.id
    ).first()
    if not entry:
        raise HTTPException(status_code=404)
    if day not in {d for d, _ in _TT_DAYS}:
        raise HTTPException(status_code=400)
    entry.subject_name = subject_name.strip()[:100]
    entry.day = day
    entry.start_time = start_time
    entry.end_time = end_time
    entry.location = location.strip()[:100]
    entry.color = color if color in _TT_COLORS else "teal"
    db.commit()
    return RedirectResponse(url="/timetable", status_code=303)


@app.post("/timetable/settings")
async def timetable_settings(
    request: Request,
    visibility: str = Form("members"),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=403)
    if visibility not in ("public", "members", "none"):
        visibility = "members"
    cm.timetable_visibility = visibility
    db.commit()
    return RedirectResponse(url="/timetable", status_code=303)


@app.get("/profile/edit/me", response_class=HTMLResponse)
async def profile_edit_page(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    external_achievements = (
        db.query(ExternalAchievement)
        .filter(ExternalAchievement.member_id == cm.id)
        .order_by(ExternalAchievement.created_at.desc())
        .all()
    )
    return _render(request, "profile_edit.html", _ctx(request, db,
        member=cm, error=None,
        external_achievements=external_achievements,
    ))


@app.post("/profile/edit/me")
async def profile_edit(
    request: Request,
    bio: str = Form(""), real_name: str = Form(...), phone: str = Form(""),
    activity_name: str = Form(...),
    new_password: str = Form(""), current_password: str = Form(...),
    profile_image: Optional[UploadFile] = File(None),
    intro_text: str = Form(""),
    skills_json: str = Form("[]"),
    links_json: str = Form("[]"),
    birthday_month: str = Form(""),
    birthday_day: str = Form(""),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    if not verify_password(current_password, cm.password_hash):
        return _render(request, "profile_edit.html", _ctx(request, db, member=cm, error="현재 비밀번호가 올바르지 않습니다."), status_code=400)

    # 활동명 변경 처리 (최대 16자)
    new_name = activity_name.strip()
    if new_name != cm.activity_name:
        if not new_name:
            return _render(request, "profile_edit.html", _ctx(request, db, member=cm, error="활동명을 입력해주세요."), status_code=400)
        if len(new_name) > 16:
            return _render(request, "profile_edit.html", _ctx(request, db, member=cm, error="활동명은 최대 16자까지 가능합니다."), status_code=400)
        duplicate = db.query(Member).filter(Member.activity_name == new_name, Member.id != cm.id).first()
        if duplicate:
            return _render(request, "profile_edit.html", _ctx(request, db, member=cm, error=f"'{new_name}'은(는) 이미 사용 중인 활동명입니다."), status_code=400)
        cm.activity_name = new_name

    cm.bio = bio.strip(); cm.real_name = real_name.strip(); cm.phone = phone.strip()
    cm.intro_text = intro_text.strip()
    # 생일 처리
    if birthday_month and birthday_day:
        try:
            mm = int(birthday_month); dd = int(birthday_day)
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                cm.birthday = f"{mm:02d}-{dd:02d}"
        except ValueError:
            pass
    elif not birthday_month and not birthday_day:
        cm.birthday = None  # 둘 다 비우면 삭제
    # skills/links: 클라이언트에서 JSON 문자열로 전송
    try:
        skills_list = json.loads(skills_json)
        if isinstance(skills_list, list):
            cm.skills = json.dumps(skills_list, ensure_ascii=False)
    except Exception:
        pass
    try:
        links_list = json.loads(links_json)
        if isinstance(links_list, list):
            cm.links = json.dumps(links_list, ensure_ascii=False)
    except Exception:
        pass
    new_img = await _save_image(profile_image)
    if new_img:
        _delete_upload(cm.profile_image)  # 구 프로필 이미지 삭제
        cm.profile_image = new_img
    if new_password:
        if len(new_password) < 6:
            return _render(request, "profile_edit.html", _ctx(request, db, member=cm, error="새 비밀번호는 최소 6자 이상이어야 합니다."), status_code=400)
        cm.password_hash = hash_password(new_password)
    db.commit()
    return RedirectResponse(url=f"/profile/{cm.activity_name}", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  캘린더 (행사 일정 + 생일 + 갤러리 연동)
# ════════════════════════════════════════════════════════════════════════════

def _cal_month_bounds(year: int, mon: int):
    """해당 월의 첫날·마지막날 반환"""
    import calendar as _c
    last_day = _c.monthrange(year, mon)[1]
    return date(year, mon, 1), date(year, mon, last_day)


@app.get("/calendar", response_class=HTMLResponse)
async def calendar_page(
    request: Request,
    month: str = "",
    db: Session = Depends(get_db),
):
    """행사 캘린더 + 생일 — 로그인한 회원만 접근"""
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    today = _today()
    if month:
        try:
            year, mon = map(int, month.split("-"))
            if not (1 <= mon <= 12 and 2000 <= year <= 2099):
                raise ValueError
        except (ValueError, AttributeError):
            year, mon = today.year, today.month
    else:
        year, mon = today.year, today.month

    month_start, month_end = _cal_month_bounds(year, mon)

    if mon == 1:
        prev_year, prev_mon = year - 1, 12
    else:
        prev_year, prev_mon = year, mon - 1
    if mon == 12:
        next_year, next_mon = year + 1, 1
    else:
        next_year, next_mon = year, mon + 1

    # ── 행사 일정 (달에 걸쳐있는 것 포함) ──────────────────────────────────
    events_raw = db.query(CalendarEvent).filter(
        or_(
            and_(CalendarEvent.start_date >= month_start, CalendarEvent.start_date <= month_end),
            and_(CalendarEvent.end_date   >= month_start, CalendarEvent.end_date   <= month_end),
            and_(CalendarEvent.start_date <= month_start, CalendarEvent.end_date   >= month_end),
        )
    ).order_by(CalendarEvent.start_date).all()

    # 이벤트 맵: {day: [event, ...]}  (기간 이벤트는 각 날짜에 중복 배치)
    event_map: dict[int, list] = {}
    for ev in events_raw:
        d = max(ev.start_date, month_start)
        end = min(ev.end_date or ev.start_date, month_end)
        cur = d
        while cur <= end:
            if cur.month == mon:
                event_map.setdefault(cur.day, []).append(ev)
            cur += timedelta(days=1)

    # ── 생일 ──────────────────────────────────────────────────────────────
    all_bd_members = db.query(Member).filter(
        Member.birthday.isnot(None), Member.birthday != ""
    ).order_by(Member.birthday).all()

    bd_map: dict[int, list] = {}
    for m in all_bd_members:
        try:
            mm, dd = map(int, m.birthday.split("-"))
            if mm == mon:
                bd_map.setdefault(dd, []).append(m)
        except Exception:
            pass

    # ── 갤러리 연동 ────────────────────────────────────────────────────────
    gallery_raw = db.query(GalleryPost).filter(
        GalleryPost.is_easter.is_(False),
        GalleryPost.is_public.isnot(False),
        GalleryPost.show_on_calendar.isnot(False),
        GalleryPost.event_date.isnot(None),
        GalleryPost.event_date >= month_start,
        GalleryPost.event_date <= month_end,
    ).order_by(GalleryPost.event_date).all()

    gallery_map: dict[int, list] = {}
    for gp in gallery_raw:
        gallery_map.setdefault(gp.event_date.day, []).append(gp)

    # ── 달력 그리드 ────────────────────────────────────────────────────────
    import calendar as _cal
    _cal.setfirstweekday(6)  # 일요일 시작
    cal_weeks = _cal.monthcalendar(year, mon)

    # ── 이달 생일 목록 ──────────────────────────────────────────────────────
    this_month_bdays = []
    for day in sorted(bd_map.keys()):
        for m in bd_map[day]:
            this_month_bdays.append({"day": day, "member": m})

    # ── 다음 30일 이내 생일 ─────────────────────────────────────────────────
    upcoming_bd = []
    for m in all_bd_members:
        try:
            mm, dd = map(int, m.birthday.split("-"))
            try:
                bd_this = date(today.year, mm, dd)
            except ValueError:
                continue
            if bd_this < today:
                bd_this = date(today.year + 1, mm, dd)
            delta = (bd_this - today).days
            if 0 <= delta <= 30:
                upcoming_bd.append({"days": delta, "date": bd_this, "member": m})
        except Exception:
            pass
    upcoming_bd.sort(key=lambda x: x["days"])

    # ── JSON 직렬화 (JS 클릭 패널용) ───────────────────────────────────────
    _ET_COLORS = {
        "정기모임": "#2563eb", "공모전": "#f59e0b",
        "행사": "#22c55e",     "MT": "#7c3aed",  "기타": "#64748b",
    }
    day_data: dict[str, dict] = {}
    all_days = set(event_map) | set(bd_map) | set(gallery_map)
    for d in all_days:
        evs = event_map.get(d, [])
        bds = bd_map.get(d, [])
        gps = gallery_map.get(d, [])
        day_data[str(d)] = {
            "events": [
                {
                    "id":    ev.id,
                    "title": ev.title,
                    "type":  ev.event_type,
                    "color": _ET_COLORS.get(ev.event_type, "#64748b"),
                    "desc":  ev.description or "",
                    "start": ev.start_date.isoformat(),
                    "end":   ev.end_date.isoformat() if ev.end_date else "",
                }
                for ev in evs
            ],
            "birthdays": [
                {
                    "name":     m.activity_name,
                    "url":      f"/profile/{m.activity_name}",
                    "img":      f"/uploads/{m.profile_image}" if m.profile_image else "",
                    "initial":  m.activity_name[0],
                    "gen":      m.generation or "",
                    "graduated": bool(m.is_graduated),
                }
                for m in bds
            ],
            "gallery": [
                {
                    "id":     gp.id,
                    "title":  gp.title,
                    "thumb":  json.loads(gp.images)[0] if gp.images and json.loads(gp.images) else "",
                    "count":  len(json.loads(gp.images)) if gp.images else 0,
                    "is_easter": bool(gp.is_easter),
                    "show":   bool(gp.show_on_calendar) if gp.show_on_calendar is not None else True,
                }
                for gp in gps
            ],
        }

    month_names = ["1월","2월","3월","4월","5월","6월","7월","8월","9월","10월","11월","12월"]

    return _render(request, "calendar.html", _ctx(request, db,
        year=year, mon=mon,
        cal_weeks=cal_weeks,
        event_map=event_map,
        bd_map=bd_map,
        gallery_map=gallery_map,
        today=today,
        prev_month=f"{prev_year:04d}-{prev_mon:02d}",
        next_month=f"{next_year:04d}-{next_mon:02d}",
        month_name=month_names[mon - 1],
        this_month_bdays=this_month_bdays,
        upcoming_bd=upcoming_bd,
        day_data_json=json.dumps(day_data, ensure_ascii=False),
        et_colors=_ET_COLORS,
        is_cal_admin=_has_perm(request, db, "calendar"),
    ))


# ── 관리자 캘린더 일정 CRUD ──────────────────────────────────────────────────

@app.post("/admin/calendar/add")
async def admin_calendar_add(
    request: Request,
    title:      str = Form(...),
    event_type: str = Form("기타"),
    start_date: str = Form(...),
    end_date:   str = Form(""),
    description: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _has_perm(request, db, "calendar"):
        return RedirectResponse(url="/member/login", status_code=303)
    try:
        sd = date.fromisoformat(start_date)
        ed = date.fromisoformat(end_date) if end_date.strip() else None
    except ValueError:
        raise HTTPException(status_code=400, detail="날짜 형식이 올바르지 않습니다.")
    cm = _current_member(request, db)
    ev = CalendarEvent(
        title=title.strip(), event_type=event_type,
        start_date=sd, end_date=ed,
        description=description.strip(),
        created_by_id=cm.id if cm else None,
        created_at=_now(),
    )
    db.add(ev); db.commit()
    back = f"/calendar?month={sd.year:04d}-{sd.month:02d}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/calendar/{event_id}/edit")
async def admin_calendar_edit(
    request: Request,
    event_id:   int,
    title:      str = Form(...),
    event_type: str = Form("기타"),
    start_date: str = Form(...),
    end_date:   str = Form(""),
    description: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _has_perm(request, db, "calendar"):
        return RedirectResponse(url="/member/login", status_code=303)
    ev = db.query(CalendarEvent).filter(CalendarEvent.id == event_id).first()
    if not ev:
        raise HTTPException(status_code=404)
    try:
        ev.start_date = date.fromisoformat(start_date)
        ev.end_date   = date.fromisoformat(end_date) if end_date.strip() else None
    except ValueError:
        raise HTTPException(status_code=400, detail="날짜 형식이 올바르지 않습니다.")
    ev.title = title.strip(); ev.event_type = event_type; ev.description = description.strip()
    db.commit()
    back = f"/calendar?month={ev.start_date.year:04d}-{ev.start_date.month:02d}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/calendar/{event_id}/delete")
async def admin_calendar_delete(
    request: Request,
    event_id: int,
    redirect_month: str = Form(""),
    db: Session = Depends(get_db),
):
    if not _has_perm(request, db, "calendar"):
        return RedirectResponse(url="/member/login", status_code=303)
    ev = db.query(CalendarEvent).filter(CalendarEvent.id == event_id).first()
    if ev:
        back_month = redirect_month or f"{ev.start_date.year:04d}-{ev.start_date.month:02d}"
        db.delete(ev); db.commit()
    else:
        back_month = redirect_month
    return RedirectResponse(url=f"/calendar?month={back_month}", status_code=303)


@app.post("/admin/gallery-post/{post_id}/toggle-calendar")
async def admin_toggle_gallery_calendar(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
):
    """갤러리 포스트의 캘린더 노출 여부 토글"""
    if r := _privileged_redirect(request, db):
        return r
    gp = db.query(GalleryPost).filter(GalleryPost.id == post_id).first()
    if gp:
        gp.show_on_calendar = not bool(gp.show_on_calendar if gp.show_on_calendar is not None else True)
        db.commit()
    referer = request.headers.get("referer", "/calendar")
    return RedirectResponse(url=referer, status_code=303)


# ── 관리자 회원 생일 편집 ────────────────────────────────────────────────────

@app.post("/admin/member/{member_id}/set-birthday")
async def admin_set_member_birthday(
    request: Request,
    member_id: int,
    birthday_month: str = Form(""),
    birthday_day:   str = Form(""),
    db: Session = Depends(get_db),
):
    """관리자가 회원 생일 직접 설정"""
    if r := _privileged_redirect(request, db):
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m:
        if birthday_month and birthday_day:
            try:
                mm = int(birthday_month); dd = int(birthday_day)
                if 1 <= mm <= 12 and 1 <= dd <= 31:
                    m.birthday = f"{mm:02d}-{dd:02d}"
            except ValueError:
                pass
        else:
            m.birthday = None
        db.commit()
    referer = request.headers.get("referer", "/admin/members")
    return RedirectResponse(url=referer, status_code=303)


@app.post("/admin/member/{member_id}/toggle-graduated")
async def admin_toggle_graduated(
    member_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """졸업 여부 토글 — 졸업 시 캘린더에서 흐리게 표시"""
    if r := _privileged_redirect(request, db):
        return r
    m = db.query(Member).filter(Member.id == member_id).first()
    if m:
        m.is_graduated = not bool(m.is_graduated)
        db.commit()
    referer = request.headers.get("referer", "/admin/members")
    return RedirectResponse(url=referer, status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  PWA 푸시 알림 라우트
# ════════════════════════════════════════════════════════════════════════════

from fastapi.responses import FileResponse as _FileResponse

@app.get("/sw.js")
async def service_worker():
    """서비스 워커를 루트 스코프로 서빙"""
    return _FileResponse(
        BASE_DIR / "static" / "sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )


@app.get("/push/vapid-public-key")
async def get_vapid_public_key():
    """프론트엔드에서 구독 시 필요한 VAPID 공개키 반환"""
    return JSONResponse({"publicKey": _vapid_public_key})


@app.post("/push/subscribe")
async def push_subscribe(request: Request, db: Session = Depends(get_db)):
    """푸시 구독 정보 저장"""
    try:
        data = await request.json()
        endpoint = data.get("endpoint", "")
        p256dh   = data.get("keys", {}).get("p256dh", "")
        auth     = data.get("keys", {}).get("auth", "")
        if not endpoint or not p256dh or not auth:
            return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
        cm = _current_member(request, db)
        existing = db.query(PushSubscription).filter(PushSubscription.endpoint == endpoint).first()
        if existing:
            existing.member_id = cm.id if cm else None
            existing.p256dh = p256dh; existing.auth = auth
        else:
            db.add(PushSubscription(
                member_id=cm.id if cm else None,
                endpoint=endpoint, p256dh=p256dh, auth=auth,
            ))
        db.commit()
        return JSONResponse({"ok": True})
    except Exception as _e:
        _log.warning("push_subscribe 오류: %s", _e)
        return JSONResponse({"ok": False}, status_code=500)


@app.post("/push/unsubscribe")
async def push_unsubscribe(request: Request, db: Session = Depends(get_db)):
    """푸시 구독 취소"""
    try:
        data = await request.json()
        endpoint = data.get("endpoint", "")
        db.query(PushSubscription).filter(PushSubscription.endpoint == endpoint).delete()
        db.commit()
        return JSONResponse({"ok": True})
    except Exception:
        return JSONResponse({"ok": False}, status_code=500)


# ── 사이트 배너 관리 ──────────────────────────────────────────────────────────

@app.get("/admin/banners", response_class=HTMLResponse)
async def admin_banners_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    banners = db.query(SiteBanner).order_by(SiteBanner.created_at.desc()).all()
    return _render(request, "admin/banners.html", _ctx(request, db, banners=banners))


@app.post("/admin/banners/create")
async def admin_banner_create(
    request: Request,
    message: str = Form(...),
    link_url: str = Form(""),
    link_label: str = Form("자세히"),
    color: str = Form("blue"),
    expires_at: str = Form(""),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    from datetime import datetime as _dt
    exp = None
    if expires_at:
        try:
            exp = _dt.fromisoformat(expires_at)
        except ValueError:
            pass
    banner = SiteBanner(
        message=message.strip(),
        link_url=link_url.strip(),
        link_label=link_label.strip() or "자세히",
        color=color if color in ("blue","red","green","yellow","purple") else "blue",
        is_active=True,
        expires_at=exp,
    )
    db.add(banner)
    db.commit()
    return RedirectResponse(url="/admin/banners", status_code=303)


@app.post("/admin/banners/{banner_id}/toggle")
async def admin_banner_toggle(request: Request, banner_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    banner = db.query(SiteBanner).filter(SiteBanner.id == banner_id).first()
    if banner:
        banner.is_active = not banner.is_active
        db.commit()
    return RedirectResponse(url="/admin/banners", status_code=303)


@app.post("/admin/banners/{banner_id}/delete")
async def admin_banner_delete(request: Request, banner_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    banner = db.query(SiteBanner).filter(SiteBanner.id == banner_id).first()
    if banner:
        db.delete(banner)
        db.commit()
    return RedirectResponse(url="/admin/banners", status_code=303)


@app.get("/admin/push", response_class=HTMLResponse)
async def admin_push_page(request: Request, db: Session = Depends(get_db)):
    """관리자 푸시 방송 페이지"""
    if r := _admin_redirect(request):
        return r
    sub_count = db.query(func.count(PushSubscription.id)).scalar() or 0
    return _render(request, "admin/push.html", _ctx(request, db,
        sub_count=sub_count,
        vapid_ok=bool(_vapid_public_key),
    ))


@app.post("/admin/push/send")
async def admin_push_send(
    request: Request,
    title: str = Form(...),
    body:  str = Form(...),
    url:   str = Form("/"),
    db: Session = Depends(get_db),
):
    """관리자가 전체 구독자에게 푸시 방송"""
    if r := _admin_redirect(request):
        return r
    sent = _broadcast_push(db, title=title.strip(), body=body.strip(),
                           url=url.strip() or "/", tag="admin-broadcast")
    return RedirectResponse(url=f"/admin/push?sent={sent}", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  팔로우 시스템
# ════════════════════════════════════════════════════════════════════════════

def _dm_thread_key(a: int, b: int) -> str:
    return f"{min(a,b)}:{max(a,b)}"


def _create_notification(db: Session, member_id: int, type_: str,
                          actor_id: Optional[int], ref_id: Optional[int], message: str):
    db.add(Notification(
        member_id=member_id, type=type_,
        actor_id=actor_id, ref_id=ref_id, message=message,
    ))


@app.post("/follow/{target_id}")
async def send_follow_request(request: Request, target_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if cm.id == target_id:
        raise HTTPException(status_code=400, detail="자신을 팔로우할 수 없습니다.")
    target = db.query(Member).filter(Member.id == target_id).first()
    if not target:
        raise HTTPException(status_code=404)
    existing = db.query(Follow).filter(
        Follow.follower_id == cm.id, Follow.following_id == target_id
    ).first()
    if existing:
        # 이미 요청이 있으면 취소(삭제)
        db.delete(existing)
        db.commit()
        return RedirectResponse(url=f"/profile/{target.activity_name}", status_code=303)
    follow = Follow(follower_id=cm.id, following_id=target_id)
    db.add(follow)
    db.flush()
    _create_notification(db, target_id, "follow_request", cm.id, follow.id,
                          f"{cm.activity_name}님이 팔로우를 요청했습니다.")
    db.commit()
    return RedirectResponse(url=f"/profile/{target.activity_name}", status_code=303)


@app.post("/follow/{follow_id}/approve")
async def approve_follow(request: Request, follow_id: int,
                         next: str = Form("/my/follows"),
                         db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    follow = db.query(Follow).filter(Follow.id == follow_id, Follow.following_id == cm.id).first()
    if not follow:
        raise HTTPException(status_code=404)
    follow.status = "approved"
    follow.approved_at = _now()
    # 관련 알림 읽음 처리
    db.query(Notification).filter(
        Notification.member_id == cm.id,
        Notification.type == "follow_request",
        Notification.ref_id == follow_id,
    ).update({"is_read": True})
    _create_notification(db, follow.follower_id, "follow_approved", cm.id, follow.id,
                          f"{cm.activity_name}님이 팔로우 요청을 수락했습니다.")
    db.commit()
    return RedirectResponse(url=next, status_code=303)


@app.post("/follow/{follow_id}/reject")
async def reject_follow(request: Request, follow_id: int,
                        next: str = Form("/my/follows"),
                        db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    follow = db.query(Follow).filter(
        Follow.id == follow_id,
        or_(Follow.following_id == cm.id, Follow.follower_id == cm.id)
    ).first()
    if follow:
        # 관련 알림 읽음 처리
        db.query(Notification).filter(
            Notification.member_id == cm.id,
            Notification.type == "follow_request",
            Notification.ref_id == follow_id,
        ).update({"is_read": True})
        db.delete(follow)
        db.commit()
    return RedirectResponse(url=next, status_code=303)


@app.get("/my/follows", response_class=HTMLResponse)
async def my_follows(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    # 나에게 온 팔로우 요청 (pending)
    pending_follows = db.query(Follow).filter(
        Follow.following_id == cm.id, Follow.status == "pending"
    ).order_by(Follow.created_at.desc()).all()
    pf_actor_ids = [f.follower_id for f in pending_follows]
    pf_members = _member_map(db, pf_actor_ids)
    for f in pending_follows:
        f.actor = pf_members.get(f.follower_id)

    # 내가 팔로우하는 사람 (approved)
    following = db.query(Follow).filter(
        Follow.follower_id == cm.id, Follow.status == "approved"
    ).all()
    following_ids = [f.following_id for f in following]
    following_members = _member_map(db, following_ids)
    for f in following:
        f.target = following_members.get(f.following_id)

    # 나를 팔로우하는 사람 (approved)
    followers = db.query(Follow).filter(
        Follow.following_id == cm.id, Follow.status == "approved"
    ).all()
    follower_ids = [f.follower_id for f in followers]
    follower_members = _member_map(db, follower_ids)
    for f in followers:
        f.actor = follower_members.get(f.follower_id)

    return _render(request, "follows.html", _ctx(request, db,
        pending_follows=pending_follows,
        following=following,
        followers=followers,
    ))


# ════════════════════════════════════════════════════════════════════════════
#  DM (1:1 메시지)
# ════════════════════════════════════════════════════════════════════════════

def _can_dm(db: Session, a_id: int, b_id: int) -> bool:
    """a→b DM 가능 여부: 수신자(b)의 dm_allowed_from 설정 + 팔로우 상태 확인"""
    receiver = db.query(Member).filter(Member.id == b_id).first()
    if not receiver:
        return False
    dm_setting = getattr(receiver, "dm_allowed_from", "all") or "all"
    if dm_setting == "none":
        return False  # 수신 거부
    mutual_follow = bool(db.query(Follow).filter(
        or_(
            and_(Follow.follower_id == a_id, Follow.following_id == b_id),
            and_(Follow.follower_id == b_id, Follow.following_id == a_id),
        ),
        Follow.status == "approved",
    ).first())
    if dm_setting == "followers":
        return mutual_follow  # 팔로워만 허용
    # dm_setting == "all": 팔로우 관계 있으면 허용 (기존 동작 유지)
    return mutual_follow


@app.get("/dm", response_class=HTMLResponse)
async def dm_list(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    # 최신 메시지 기준으로 대화 목록
    sent_keys = [r[0] for r in db.query(DirectMessage.thread_key).filter(
        DirectMessage.sender_id == cm.id).distinct().all()]
    recv_keys = [r[0] for r in db.query(DirectMessage.thread_key).filter(
        DirectMessage.receiver_id == cm.id).distinct().all()]
    all_keys = list(set(sent_keys + recv_keys))

    # thread_key별 마지막 메시지 + 미읽음 수를 한 번에 조회
    last_msgs: dict = {}
    unread_counts: dict = {}
    if all_keys:
        # 마지막 메시지: thread_key별 max(id)로 서브쿼리 없이 Python에서 처리
        all_msgs = (
            db.query(DirectMessage)
            .filter(DirectMessage.thread_key.in_(all_keys))
            .order_by(DirectMessage.created_at.desc())
            .all()
        )
        for msg in all_msgs:
            if msg.thread_key not in last_msgs:
                last_msgs[msg.thread_key] = msg
            if msg.receiver_id == cm.id and not msg.is_read:
                unread_counts[msg.thread_key] = unread_counts.get(msg.thread_key, 0) + 1

    # 파트너 ID 목록 수집 후 일괄 조회
    partner_id_map: dict = {}
    for key, msg in last_msgs.items():
        partner_id_map[key] = msg.receiver_id if msg.sender_id == cm.id else msg.sender_id
    all_partner_ids = list(set(partner_id_map.values()))
    partners = _member_map(db, all_partner_ids)

    threads = []
    for key in all_keys:
        last_msg = last_msgs.get(key)
        if not last_msg:
            continue
        pid = partner_id_map.get(key)
        threads.append({
            "key": key,
            "partner": partners.get(pid),
            "last_msg": last_msg,
            "unread": unread_counts.get(key, 0),
        })

    threads.sort(key=lambda t: t["last_msg"].created_at, reverse=True)
    return _render(request, "dm/list.html", _ctx(request, db, threads=threads))


@app.get("/dm/{partner_id}", response_class=HTMLResponse)
async def dm_thread(request: Request, partner_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    partner = db.query(Member).filter(Member.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404)
    if not _can_dm(db, cm.id, partner_id):
        raise HTTPException(status_code=403, detail="팔로우 관계인 회원과만 DM 가능합니다.")

    key = _dm_thread_key(cm.id, partner_id)
    messages = db.query(DirectMessage).filter(
        DirectMessage.thread_key == key
    ).order_by(DirectMessage.created_at.asc()).all()

    # 읽음 처리
    db.query(DirectMessage).filter(
        DirectMessage.thread_key == key,
        DirectMessage.receiver_id == cm.id,
        DirectMessage.is_read.is_(False),
    ).update({"is_read": True})
    db.commit()

    return _render(request, "dm/thread.html", _ctx(request, db,
        partner=partner, messages=messages, thread_key=key,
    ))


@app.post("/dm/{partner_id}/send")
async def dm_send(
    request: Request,
    partner_id: int,
    content: str = Form(...),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    if not _can_dm(db, cm.id, partner_id):
        raise HTTPException(status_code=403)
    content = content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="메시지를 입력하세요.")
    if len(content) > 2000:
        raise HTTPException(status_code=400, detail="메시지는 2,000자 이하로 입력하세요.")
    key = _dm_thread_key(cm.id, partner_id)
    db.add(DirectMessage(
        thread_key=key, sender_id=cm.id, receiver_id=partner_id, content=content,
    ))
    db.commit()
    return RedirectResponse(url=f"/dm/{partner_id}", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  알림
# ════════════════════════════════════════════════════════════════════════════

@app.post("/notifications/{notif_id}/read")
async def mark_notification_read(request: Request, notif_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    notif = db.query(Notification).filter(
        Notification.id == notif_id, Notification.member_id == cm.id
    ).first()
    if notif:
        notif.is_read = True
        db.commit()
    return RedirectResponse(url=_safe_referer(request, "/my"), status_code=303)


@app.post("/notifications/read-all")
async def mark_all_read(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    db.query(Notification).filter(
        Notification.member_id == cm.id, Notification.is_read.is_(False)
    ).update({"is_read": True})
    db.commit()
    return RedirectResponse(url=_safe_referer(request, "/my"), status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  프로필 확장 — 외부 이력
# ════════════════════════════════════════════════════════════════════════════

@app.post("/profile/external/add")
async def add_external_achievement(
    request: Request,
    title: str = Form(...),
    organizer: str = Form(""),
    result: str = Form(""),
    achieved_year: Optional[str] = Form(None),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    if not title.strip():
        raise HTTPException(status_code=400, detail="이력 제목을 입력하세요.")
    year = _optional_int(achieved_year, "연도")
    db.add(ExternalAchievement(
        member_id=cm.id, title=title.strip(), organizer=organizer.strip(),
        result=result.strip(), achieved_year=year, note=note.strip(),
    ))
    db.commit()
    return RedirectResponse(url="/profile/edit/me#external", status_code=303)


@app.post("/profile/external/{ach_id}/delete")
async def delete_external_achievement(
    request: Request, ach_id: int, db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    ach = db.query(ExternalAchievement).filter(
        ExternalAchievement.id == ach_id, ExternalAchievement.member_id == cm.id
    ).first()
    if ach:
        db.delete(ach)
        db.commit()
    return RedirectResponse(url="/profile/edit/me#external", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  팀 구성
# ════════════════════════════════════════════════════════════════════════════

@app.post("/competition/{comp_id}/team/create")
async def create_team(
    request: Request, comp_id: int,
    team_name: str = Form(...),
    team_desc: str = Form(""),
    team_requirements: str = Form(""),
    nickname: str = Form(...),
    real_name: str = Form(...),
    student_id: str = Form(...),
    password: str = Form(...),
    role: str = Form("기타"),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)
    if _today() > comp.deadline:
        raise HTTPException(status_code=400, detail="마감된 공모전입니다.")
    team_name = team_name.strip()
    if not team_name:
        raise HTTPException(status_code=400, detail="팀 이름을 입력하세요.")
    if not real_name.strip():
        raise HTTPException(status_code=400, detail="본명을 입력하세요.")
    if not student_id.strip():
        raise HTTPException(status_code=400, detail="학번을 입력하세요.")
    if db.query(Team).filter(Team.competition_id == comp_id, Team.name == team_name).first():
        raise HTTPException(status_code=400, detail="같은 이름의 팀이 이미 있습니다.")
    try:
        team = Team(
            competition_id=comp_id,
            name=team_name,
            description=(team_desc or "").strip(),
            requirements=(team_requirements or "").strip(),
        )
        db.add(team)
        db.flush()
        cm = _current_member(request, db)
        leader = TeamMember(
            team_id=team.id,
            competition_id=comp_id,
            nickname=nickname.strip(),
            real_name=real_name.strip(),
            student_id=student_id.strip(),
            password_hash=hash_password(password),
            role=role if role in ROLES else "기타",
            memo=(memo or "").strip(),
            is_leader=True,
            status="approved",
            member_id=cm.id if cm else None,
        )
        db.add(leader)
        db.flush()
        # 팔로워 알림
        if cm:
            for fr in db.query(Follow).filter(Follow.following_id == cm.id, Follow.status == "approved").all():
                _create_notification(
                    db, fr.follower_id, "team_recruit", cm.id, team.id,
                    f"{cm.activity_name}님이 '{comp.title}' 팀 '{team_name}'을 모집합니다.",
                )
        db.commit()
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="팀 생성 중 오류가 발생했습니다.")
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/join")
async def team_join(
    request: Request, comp_id: int, team_id: int,
    student_id: str = Form(...),
    real_name: str = Form(...),
    site_password: str = Form(""),   # 비로그인 시 필수
    role: str = Form("기타"),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    """팀원 참여 신청 — 반드시 사이트 회원이어야 하며 본인 인증 필요"""
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)
    if _today() > comp.deadline:
        raise HTTPException(status_code=400, detail="마감된 공모전입니다.")
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)

    # ── 본인 인증: 로그인 세션 우선, 아니면 학번+본명+비밀번호로 조회 ──
    cm = _current_member(request, db)
    if cm:
        # 로그인 상태: 입력한 학번·본명이 본인 것인지 확인
        if cm.student_id.strip() != student_id.strip() or cm.real_name.strip() != real_name.strip():
            raise HTTPException(
                status_code=400,
                detail="입력한 학번 또는 본명이 회원 정보와 일치하지 않습니다."
            )
        target_member = cm
    else:
        # 비로그인: 학번+본명+비밀번호로 회원 확인
        if not student_id.strip() or not real_name.strip() or not site_password:
            raise HTTPException(status_code=400, detail="학번, 본명, 비밀번호를 모두 입력하세요.")
        target_member = db.query(Member).filter(
            Member.student_id == student_id.strip(),
            Member.real_name  == real_name.strip(),
        ).first()
        if not target_member or not verify_password(site_password, target_member.password_hash):
            raise HTTPException(
                status_code=400,
                detail="회원 정보를 찾을 수 없거나 비밀번호가 올바르지 않습니다."
            )

    # ── 인원 제한: 승인된 팀원(비팀장) 기준 ──
    approved_count = db.query(func.count(TeamMember.id)).filter(
        TeamMember.team_id == team_id,
        TeamMember.is_leader.is_(False),
        TeamMember.status == "approved",
    ).scalar() or 0
    if comp.max_members and approved_count >= comp.max_members:
        raise HTTPException(status_code=400, detail="팀 인원이 가득 찼습니다.")

    # ── 팀장은 팀원 신청 불가 ──
    if db.query(TeamMember).filter(
        TeamMember.team_id == team_id,
        TeamMember.member_id == target_member.id,
        TeamMember.is_leader.is_(True),
    ).first():
        raise HTTPException(status_code=400, detail="팀장은 별도로 팀원 신청을 할 수 없습니다.")

    # ── 중복 신청 확인 (같은 회원) ──
    if db.query(TeamMember).filter(
        TeamMember.team_id == team_id,
        TeamMember.member_id == target_member.id,
        TeamMember.is_leader.is_(False),
    ).first():
        raise HTTPException(status_code=400, detail="이미 이 팀에 신청하셨습니다.")

    db.add(TeamMember(
        team_id=team_id,
        competition_id=comp_id,
        nickname=target_member.activity_name,   # 사이트 닉네임(활동명) 사용
        real_name=target_member.real_name,
        student_id=target_member.student_id,
        password_hash=None,
        role=role if role in ROLES else "기타",
        memo=(memo or "").strip(),
        is_leader=False,
        status="pending",
        member_id=target_member.id,
    ))

    # ── 팀장에게 알림 ──
    leader_tm = db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)
    ).first()
    if leader_tm and leader_tm.member_id:
        _create_notification(
            db, leader_tm.member_id, "team_recruit", target_member.id, team_id,
            f"'{team.name}' 팀에 '{target_member.activity_name}'({target_member.real_name})님이 참여 신청했습니다.",
        )
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/admin-add-member")
async def admin_add_team_member(
    request: Request, comp_id: int, team_id: int,
    member_id: int = Form(...),
    role: str = Form("기타"),
    db: Session = Depends(get_db),
):
    """관리자 전용 — 카르텔 멤버를 팀에 직접 추가 (즉시 승인)"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    m = db.query(Member).filter(Member.id == member_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="회원을 찾을 수 없습니다.")
    existing = db.query(TeamMember).filter(
        TeamMember.team_id == team_id,
        TeamMember.member_id == member_id,
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"'{m.activity_name}'은 이미 이 팀에 있습니다.")
    db.add(TeamMember(
        team_id=team_id,
        competition_id=comp_id,
        nickname=m.activity_name,
        real_name=m.real_name,
        student_id=m.student_id or "",
        is_leader=False,
        status="approved",
        member_id=m.id,
        role=(role.strip() or "기타")[:30],
    ))
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/admin-create")
async def admin_create_team(
    request: Request, comp_id: int,
    team_name: str = Form(...),
    team_desc: str = Form(""),
    leader_member_id: int = Form(...),
    role: str = Form("기타"),
    db: Session = Depends(get_db),
):
    """관리자 전용 — 팀장 멤버를 지정해 팀 생성"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)
    team_name = team_name.strip()
    if not team_name:
        raise HTTPException(status_code=400, detail="팀 이름을 입력하세요.")
    if db.query(Team).filter(Team.competition_id == comp_id, Team.name == team_name).first():
        raise HTTPException(status_code=400, detail="같은 이름의 팀이 이미 있습니다.")
    leader_m = db.query(Member).filter(Member.id == leader_member_id).first()
    if not leader_m:
        raise HTTPException(status_code=404, detail="선택한 회원을 찾을 수 없습니다.")
    team = Team(
        competition_id=comp_id,
        name=team_name,
        description=team_desc.strip(),
    )
    db.add(team)
    db.flush()
    db.add(TeamMember(
        team_id=team.id,
        competition_id=comp_id,
        nickname=leader_m.activity_name,
        real_name=leader_m.real_name,
        student_id=leader_m.student_id or "",
        is_leader=True,
        status="approved",
        member_id=leader_m.id,
        role=(role.strip() or "기타")[:30],
    ))
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/member/{tm_id}/approve")
async def approve_member(
    request: Request, comp_id: int, team_id: int, tm_id: int,
    leader_password: str = Form(""),
    db: Session = Depends(get_db),
):
    """팀장 또는 관리자가 신청을 승인"""
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    applicant = db.query(TeamMember).filter(TeamMember.id == tm_id, TeamMember.team_id == team_id).first()
    if not applicant:
        raise HTTPException(status_code=404)

    # 권한 확인: 관리자 OR 로그인된 팀장 OR 팀장 비밀번호 입력
    cm = _current_member(request, db)
    leader_tm = db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)
    ).first()
    is_admin_user = _is_privileged(request, db)
    is_logged_leader = cm and leader_tm and leader_tm.member_id == cm.id
    is_pwd_leader = (leader_password and leader_tm and leader_tm.password_hash
                     and verify_team_password(leader_password, leader_tm.password_hash))
    if not (is_admin_user or is_logged_leader or is_pwd_leader):
        raise HTTPException(status_code=403, detail="팀장 권한이 필요합니다.")

    applicant.status = "approved"
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team-{team_id}", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/member/{tm_id}/reject")
async def reject_member(
    request: Request, comp_id: int, team_id: int, tm_id: int,
    leader_password: str = Form(""),
    db: Session = Depends(get_db),
):
    """팀장 또는 관리자가 신청을 거절(삭제)"""
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    applicant = db.query(TeamMember).filter(TeamMember.id == tm_id, TeamMember.team_id == team_id).first()
    if not applicant:
        raise HTTPException(status_code=404)

    cm = _current_member(request, db)
    leader_tm = db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)
    ).first()
    is_admin_user = _is_privileged(request, db)
    is_logged_leader = cm and leader_tm and leader_tm.member_id == cm.id
    is_pwd_leader = (leader_password and leader_tm and leader_tm.password_hash
                     and verify_team_password(leader_password, leader_tm.password_hash))
    if not (is_admin_user or is_logged_leader or is_pwd_leader):
        raise HTTPException(status_code=403, detail="팀장 권한이 필요합니다.")

    db.delete(applicant)
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team-{team_id}", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/leave/{member_id}")
async def team_leave(
    request: Request, comp_id: int, team_id: int, member_id: int,
    nickname: str = Form(""),
    verify_field: str = Form(""),
    kick_reason: str = Form(""),
    db: Session = Depends(get_db),
):
    team_check = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team_check:
        raise HTTPException(status_code=404)
    member = db.query(TeamMember).filter(
        TeamMember.id == member_id, TeamMember.team_id == team_id,
    ).first()
    if not member:
        raise HTTPException(status_code=404)

    is_priv = _is_privileged(request, db)
    cm = _current_member(request, db)

    if not is_priv:
        if member.nickname != nickname:
            raise HTTPException(status_code=400, detail="닉네임이 올바르지 않습니다.")
        if member.is_leader:
            if not (member.password_hash and verify_team_password(verify_field, member.password_hash)):
                raise HTTPException(status_code=400, detail="비밀번호가 올바르지 않습니다.")
        else:
            if member.student_id != verify_field.strip():
                raise HTTPException(status_code=400, detail="학번이 올바르지 않습니다.")

    # ── 팀장이 타 팀원을 강퇴하는 경우, 접수 이후엔 관리자 승인 필요 ──
    if not is_priv and cm:
        is_requester_leader = bool(db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.is_leader.is_(True),
            or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
        ).first())
        is_kicking_other = (member.member_id != cm.id)
        comp = db.query(Competition).filter(Competition.id == comp_id).first()
        after_start = bool(comp and comp.start_date and _today() >= comp.start_date)

        if is_requester_leader and is_kicking_other and after_start:
            if not kick_reason.strip():
                raise HTTPException(status_code=400, detail="접수 이후 강퇴는 사유를 입력해야 합니다.")
            db.add(TeamKickRequest(
                team_id=team_id,
                competition_id=comp_id,
                team_member_id=member_id,
                requested_by_id=cm.id,
                reason=kick_reason.strip(),
            ))
            db.commit()
            return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)

    was_leader = member.is_leader
    db.delete(member)
    db.flush()

    remaining = db.query(TeamMember).filter(TeamMember.team_id == team_id).order_by(TeamMember.created_at.asc()).all()
    if not remaining:
        team = db.query(Team).filter(Team.id == team_id).first()
        if team:
            db.delete(team)
    elif was_leader:
        next_leader = next((r for r in remaining if not r.is_leader and r.status == "approved"), remaining[0])
        next_leader.is_leader = True

    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/kick-request/{req_id}/approve")
async def approve_kick_request(request: Request, comp_id: int, req_id: int, db: Session = Depends(get_db)):
    """관리자: 강퇴 요청 승인 → 팀원 즉시 삭제"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    req = db.query(TeamKickRequest).filter(TeamKickRequest.id == req_id).first()
    if not req:
        raise HTTPException(status_code=404)
    member = db.query(TeamMember).filter(TeamMember.id == req.team_member_id).first()
    if member:
        db.delete(member)
        remaining = db.query(TeamMember).filter(TeamMember.team_id == req.team_id).all()
        if not remaining:
            team = db.query(Team).filter(Team.id == req.team_id).first()
            if team:
                db.delete(team)
    db.delete(req)
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/kick-request/{req_id}/deny")
async def deny_kick_request(request: Request, comp_id: int, req_id: int, db: Session = Depends(get_db)):
    """관리자: 강퇴 요청 거절 → 요청 삭제"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    req = db.query(TeamKickRequest).filter(TeamKickRequest.id == req_id).first()
    if req:
        db.delete(req)
        db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/admin/competition/{comp_id}/team/{team_id}/set-leader/{member_id}")
async def set_leader(request: Request, comp_id: int, team_id: int, member_id: int, db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    db.query(TeamMember).filter(TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)).update({"is_leader": False})
    m = db.query(TeamMember).filter(TeamMember.id == member_id).first()
    if m:
        m.is_leader = True
        m.status = "approved"
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/force-dissolve")
async def force_dissolve_team(
    request: Request, comp_id: int, team_id: int,
    db: Session = Depends(get_db),
):
    """관리자 전용 — 동의 없이 즉시 팀 해체"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    db.delete(team)
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/request-dissolution")
async def request_dissolution(request: Request, comp_id: int, team_id: int, db: Session = Depends(get_db)):
    """팀장이 해체 요청 시작 — 자신의 동의를 포함해 과반수 달성 시 즉시 해체"""
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    is_leader = bool(db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True),
        or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
    ).first())
    if not is_leader:
        raise HTTPException(status_code=403, detail="팀장만 해체 요청을 시작할 수 있습니다.")

    total = db.query(func.count(TeamMember.id)).filter(
        TeamMember.team_id == team_id, TeamMember.status == "approved"
    ).scalar() or 1

    votes = [int(cm.id)] if cm.id else []
    team.dissolution_requested = True
    team.dissolution_requested_at = _now()
    team.dissolution_votes = json.dumps(votes, ensure_ascii=False)

    # 과반수 달성(팀장 혼자인 경우) 즉시 해체
    if len(votes) * 2 > total:
        db.delete(team)
        db.commit()
        return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)

    # 팀원들에게 알림
    for tm in db.query(TeamMember).filter(
        TeamMember.team_id == team_id,
        TeamMember.is_leader.is_(False),
        TeamMember.status == "approved",
    ).all():
        if tm.member_id:
            _create_notification(
                db, tm.member_id, "team_recruit", cm.id, team_id,
                f"'{team.name}' 팀 해체 요청이 시작됐습니다. 동의하시면 투표해 주세요.",
            )
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/vote-dissolution")
async def vote_dissolution(request: Request, comp_id: int, team_id: int, db: Session = Depends(get_db)):
    """팀원이 해체에 동의 투표 — 과반수 달성 시 자동 해체"""
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team or not team.dissolution_requested:
        raise HTTPException(status_code=400, detail="진행 중인 해체 요청이 없습니다.")

    tm = db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.status == "approved",
        or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
    ).first()
    if not tm:
        raise HTTPException(status_code=403, detail="이 팀의 멤버가 아닙니다.")

    votes = [int(v) for v in _from_json(team.dissolution_votes or "[]") if v]
    if cm.id in votes:
        raise HTTPException(status_code=400, detail="이미 동의하셨습니다.")

    votes.append(cm.id)
    team.dissolution_votes = json.dumps(votes, ensure_ascii=False)

    total = db.query(func.count(TeamMember.id)).filter(
        TeamMember.team_id == team_id, TeamMember.status == "approved"
    ).scalar() or 1

    # 과반수 달성 시 해체
    if len(votes) * 2 > total:
        db.delete(team)
        db.commit()
        return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)

    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/cancel-dissolution")
async def cancel_dissolution(request: Request, comp_id: int, team_id: int, db: Session = Depends(get_db)):
    """팀장 또는 관리자가 해체 요청 취소"""
    cm = _current_member(request, db)
    is_priv = _is_privileged(request, db)
    if not is_priv:
        if not cm:
            raise HTTPException(status_code=401)
        is_leader = bool(db.query(TeamMember).filter(
            TeamMember.team_id == team_id, TeamMember.is_leader.is_(True),
            or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
        ).first())
        if not is_leader:
            raise HTTPException(status_code=403)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    team.dissolution_requested = False
    team.dissolution_requested_at = None
    team.dissolution_votes = "[]"
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/transfer-leader")
async def transfer_leader(
    request: Request, comp_id: int, team_id: int,
    new_leader_tm_id: int = Form(...),
    db: Session = Depends(get_db),
):
    """팀장 또는 관리자가 팀장 권한을 다른 팀원에게 양도"""
    # 팀이 해당 공모전 소속인지 먼저 검증
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)

    cm = _current_member(request, db)
    is_priv = _is_privileged(request, db)

    # 현재 팀장인지 확인
    is_team_leader = False
    if cm:
        is_team_leader = bool(db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.is_leader.is_(True),
            or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
        ).first())

    if not is_priv and not is_team_leader:
        raise HTTPException(status_code=403, detail="팀장 또는 관리자만 팀장을 변경할 수 있습니다.")

    # 새 팀장이 같은 팀의 승인된 팀원인지 검증
    new_ldr = db.query(TeamMember).filter(
        TeamMember.id == new_leader_tm_id,
        TeamMember.team_id == team_id,
        TeamMember.is_leader.is_(False),
        TeamMember.status == "approved",
    ).first()
    if not new_ldr:
        raise HTTPException(status_code=400, detail="올바른 팀원을 선택해주세요.")

    db.query(TeamMember).filter(
        TeamMember.team_id == team_id, TeamMember.is_leader.is_(True)
    ).update({"is_leader": False})
    new_ldr.is_leader = True
    new_ldr.status = "approved"
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


@app.post("/admin/competition/{comp_id}/team/{team_id}/member/add")
async def admin_add_member(
    request: Request, comp_id: int, team_id: int,
    real_name: str = Form(...),
    student_id: str = Form(...),
    role: str = Form("기타"),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    """관리자가 학번+본명으로 회원을 팀에 직접 추가 (즉시 승인)"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    if not real_name.strip() or not student_id.strip():
        raise HTTPException(status_code=400, detail="본명과 학번을 모두 입력하세요.")

    # 학번+본명으로 회원 조회 (반드시 회원이어야 함)
    target_member = db.query(Member).filter(
        Member.student_id == student_id.strip(),
        Member.real_name  == real_name.strip(),
    ).first()
    if not target_member:
        raise HTTPException(
            status_code=400,
            detail=f"학번 '{student_id.strip()}'·본명 '{real_name.strip()}'에 해당하는 회원을 찾을 수 없습니다."
        )

    # 이미 팀에 있는지 확인
    if db.query(TeamMember).filter(
        TeamMember.team_id == team_id,
        TeamMember.member_id == target_member.id,
    ).first():
        raise HTTPException(status_code=400, detail="이미 이 팀에 속한 회원입니다.")

    db.add(TeamMember(
        team_id=team_id,
        competition_id=comp_id,
        nickname=target_member.activity_name,
        real_name=target_member.real_name,
        student_id=target_member.student_id,
        password_hash=None,
        role=role if role in ROLES else "기타",
        memo=(memo or "").strip(),
        is_leader=False,
        status="approved",
        member_id=target_member.id,
    ))
    db.commit()
    return RedirectResponse(url=f"/admin/competition/{comp_id}/members", status_code=303)


# ── 어드민: 공모전별 참여자·수상 관리 ─────────────────────────────────────────────

@app.get("/admin/competition/{comp_id}/members", response_class=HTMLResponse)
async def admin_comp_members(request: Request, comp_id: int, db: Session = Depends(get_db)):
    if r := _privileged_redirect(request, db):
        return r
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    if not comp:
        raise HTTPException(status_code=404)
    teams = db.query(Team).filter(Team.competition_id == comp_id).order_by(Team.created_at.asc()).all()
    team_ids = [t.id for t in teams]
    all_tm = db.query(TeamMember).filter(TeamMember.team_id.in_(team_ids)).order_by(TeamMember.created_at.asc()).all() if team_ids else []
    tm_by_team: dict = {}
    for tm in all_tm:
        tm_by_team.setdefault(tm.team_id, []).append(tm)
    for t in teams:
        t.members = tm_by_team.get(t.id, [])
    member_ids = [tm.member_id for tm in all_tm if tm.member_id]
    members_map = {}
    if member_ids:
        for m in db.query(Member).filter(Member.id.in_(member_ids)).all():
            members_map[m.id] = m
    return _render(request,
        "admin/comp_members.html",
        _ctx(request, db, comp=comp, teams=teams, members_map=members_map, award_ranks=AWARD_RANKS),
    )


@app.post("/admin/competition/{comp_id}/members/{tm_id}/award")
async def admin_set_award(
    request: Request, comp_id: int, tm_id: int,
    award_rank: str = Form(""),
    award_prize: str = Form(""),
    award_note: str = Form(""),
    db: Session = Depends(get_db),
):
    """팀원 한 명의 수상 정보 저장"""
    if r := _privileged_redirect(request, db):
        return r
    tm = db.query(TeamMember).filter(TeamMember.id == tm_id).first()
    if not tm:
        raise HTTPException(status_code=404)
    tm.award_rank  = award_rank if award_rank in AWARD_RANKS else None
    tm.award_prize = award_prize.strip()
    tm.award_note  = award_note.strip()
    db.commit()
    return RedirectResponse(url=f"/admin/competition/{comp_id}/members", status_code=303)


@app.post("/competition/{comp_id}/team/{team_id}/submit")
async def record_submission(
    request: Request, comp_id: int, team_id: int,
    participant_ids: List[int] = Form(default=[]),
    submitted_docs: List[str] = Form(default=[]),
    custom_doc: str = Form(""),
    submission_files: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    # 관리자 또는 해당 팀의 팀장만 허용
    cm = _current_member(request, db)
    is_team_leader = False
    if cm:
        ldr = db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.is_leader == True,
            or_(TeamMember.member_id == cm.id, TeamMember.nickname == cm.activity_name),
        ).first()
        is_team_leader = bool(ldr)
    if not (_is_privileged(request, db) or is_team_leader):
        raise HTTPException(status_code=403)

    team = db.query(Team).filter(Team.id == team_id, Team.competition_id == comp_id).first()
    if not team:
        raise HTTPException(status_code=404)
    comp = db.query(Competition).filter(Competition.id == comp_id).first()
    today = _today()
    if today > comp.deadline + timedelta(days=30):
        raise HTTPException(status_code=400, detail="제출 기록 기간이 지났습니다. (마감 후 30일 초과)")

    db.query(TeamMember).filter(TeamMember.team_id == team_id).update({"is_participant": False})
    if participant_ids:
        db.query(TeamMember).filter(
            TeamMember.team_id == team_id, TeamMember.id.in_(participant_ids)
        ).update({"is_participant": True})

    # 제출 서류 체크 목록
    docs = list(submitted_docs)
    if custom_doc.strip():
        docs.append(custom_doc.strip())
    team.submitted_docs = json.dumps(docs, ensure_ascii=False)

    # 제출 파일 업로드 (기존 파일 유지 + 새 파일 추가)
    existing_files = _from_json(team.submission_files or "[]")
    new_files = await _save_files(submission_files)
    team.submission_files = json.dumps(existing_files + new_files, ensure_ascii=False)

    team.submitted = True
    team.submitted_at = _now()
    db.commit()
    return RedirectResponse(url=f"/competition/{comp_id}#team", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  게시판
# ════════════════════════════════════════════════════════════════════════════

@app.get("/board/{board}", response_class=HTMLResponse)
async def board_list(
    request: Request, board: str,
    page: int = Query(1, ge=1),
    q: str = "",
    db: Session = Depends(get_db),
):
    if board not in BOARDS:
        raise HTTPException(status_code=404)

    page_size = 20
    post_query = db.query(Post).filter(Post.board == board)
    if q:
        compact_q = _compact_text(q)
        post_query = post_query.filter(
            or_(
                _compact_column(Post.title).contains(compact_q),
                _compact_column(Post.content).contains(compact_q),
            )
        )
    total = post_query.with_entities(func.count(Post.id)).scalar()

    # 고정 글을 맨 위로 (공지 게시판), 그 외는 최신순
    posts = (
        post_query
        .order_by(Post.is_pinned.desc(), Post.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # 작성자 매핑
    author_ids = list({p.author_id for p in posts})
    authors = _member_map(db, author_ids)

    # 댓글 수 / 좋아요 수
    post_ids = [p.id for p in posts]
    comment_counts = dict(
        db.query(Comment.post_id, func.count(Comment.id))
        .filter(Comment.post_id.in_(post_ids), Comment.parent_id.is_(None))
        .group_by(Comment.post_id).all()
    ) if post_ids else {}
    like_counts = dict(
        db.query(PostLike.post_id, func.count(PostLike.id))
        .filter(PostLike.post_id.in_(post_ids))
        .group_by(PostLike.post_id).all()
    ) if post_ids else {}

    for p in posts:
        p.author = authors.get(p.author_id)
        p.comment_count = comment_counts.get(p.id, 0)
        p.like_count = like_counts.get(p.id, 0)

    total_pages = max(1, (total + page_size - 1) // page_size)

    return _render(request,
        "board/list.html",
        _ctx(request, db,
             board=board, board_name=BOARDS[board],
             posts=posts, page=page, total_pages=total_pages, query=q),
    )


@app.get("/board/{board}/new", response_class=HTMLResponse)
async def board_new_page(request: Request, board: str, db: Session = Depends(get_db)):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    if board in NOTICE_ONLY_BOARDS and not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="공지사항은 관리자/중간관리자만 작성할 수 있습니다.")
    return _render(request,
        "board/post_new.html",
        _ctx(request, db, board=board, board_name=BOARDS[board], error=None),
    )


@app.post("/board/{board}/new")
async def board_new_post(
    request: Request, board: str,
    title: str = Form(...), content: str = Form(""),
    images: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    if board in NOTICE_ONLY_BOARDS and not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="공지사항은 관리자/중간관리자만 작성할 수 있습니다.")

    if len(title.strip()) > 200:
        raise HTTPException(status_code=400, detail="제목은 200자를 초과할 수 없습니다.")
    if len(content) > 10000:
        raise HTTPException(status_code=400, detail="본문은 10,000자를 초과할 수 없습니다.")

    saved_images = await _save_images(images)
    post = Post(
        board=board, title=title.strip(), content=content,
        author_id=cm.id,
        images=json.dumps(saved_images, ensure_ascii=False),
    )
    db.add(post)
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{post.id}", status_code=303)


@app.get("/board/{board}/post/{post_id}", response_class=HTMLResponse)
async def board_post_detail(request: Request, board: str, post_id: int, db: Session = Depends(get_db)):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)

    db.execute(
        _sa_update(Post).where(Post.id == post_id).values(view_count=Post.view_count + 1)
    )
    db.commit()
    db.refresh(post)

    # 작성자 정보
    author = db.query(Member).filter(Member.id == post.author_id).first()

    # 댓글 (계층 구조)
    all_comments = db.query(Comment).filter(Comment.post_id == post_id).order_by(Comment.created_at.asc()).all()

    # 댓글 작성자 정보
    c_author_ids = list({c.author_id for c in all_comments})
    c_authors = _member_map(db, c_author_ids)

    # 좋아요
    cm = _current_member(request, db)
    post_likes = db.query(func.count(PostLike.id)).filter(PostLike.post_id == post_id).scalar()
    user_liked_post = bool(cm and db.query(PostLike).filter(PostLike.post_id == post_id, PostLike.member_id == cm.id).first())

    # 댓글 좋아요
    c_ids = [c.id for c in all_comments]
    c_like_counts = dict(
        db.query(CommentLike.comment_id, func.count(CommentLike.id))
        .filter(CommentLike.comment_id.in_(c_ids))
        .group_by(CommentLike.comment_id).all()
    ) if c_ids else {}
    user_liked_comments = set()
    if cm and c_ids:
        liked = db.query(CommentLike.comment_id).filter(CommentLike.member_id == cm.id, CommentLike.comment_id.in_(c_ids)).all()
        user_liked_comments = {r[0] for r in liked}

    for c in all_comments:
        c.author = c_authors.get(c.author_id)
        c.like_count = c_like_counts.get(c.id, 0)
        c.user_liked = c.id in user_liked_comments

    # 계층 정리: top_comments + replies 매핑
    top_comments = [c for c in all_comments if c.parent_id is None]
    replies = defaultdict(list)
    for c in all_comments:
        if c.parent_id is not None:
            replies[c.parent_id].append(c)

    total_comments = len(all_comments)

    return _render(request,
        "board/post_detail.html",
        _ctx(request, db,
             board=board, board_name=BOARDS[board],
             post=post, author=author,
             images=_from_json(post.images),
             top_comments=top_comments, replies=dict(replies),
             post_likes=post_likes, user_liked_post=user_liked_post,
             total_comments=total_comments),
    )


@app.get("/board/{board}/post/{post_id}/edit", response_class=HTMLResponse)
async def board_edit_page(request: Request, board: str, post_id: int, db: Session = Depends(get_db)):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    if not cm or (cm.id != post.author_id and not _is_privileged(request, db)):
        raise HTTPException(status_code=403, detail="수정 권한이 없습니다.")
    return _render(request, "board/post_edit.html", _ctx(request, db,
        board=board, board_name=BOARDS[board], post=post, error=None,
    ))


@app.post("/board/{board}/post/{post_id}/edit")
async def board_edit_post(
    request: Request, board: str, post_id: int,
    title: str = Form(...), content: str = Form(""),
    images: List[UploadFile] = File(default=[]),
    delete_images: List[str] = Form(default=[]),
    db: Session = Depends(get_db),
):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    if not cm or (cm.id != post.author_id and not _is_privileged(request, db)):
        raise HTTPException(status_code=403, detail="수정 권한이 없습니다.")
    if len(title.strip()) > 200:
        raise HTTPException(status_code=400, detail="제목은 200자를 초과할 수 없습니다.")
    if len(content) > 10000:
        raise HTTPException(status_code=400, detail="본문은 10,000자를 초과할 수 없습니다.")
    post.title = title.strip()
    post.content = content
    # 기존 이미지에서 삭제 요청 처리
    existing = _from_json(post.images)
    for del_img in delete_images:
        if del_img in existing:
            _storage_delete(del_img)
            existing.remove(del_img)
    # 새 이미지 추가
    new_imgs = await _save_images(images)
    existing.extend(new_imgs)
    post.images = json.dumps(existing, ensure_ascii=False)
    post.updated_at = _now()
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{post_id}", status_code=303)


@app.post("/board/{board}/post/{post_id}/delete")
async def board_delete_post(request: Request, board: str, post_id: int, db: Session = Depends(get_db)):
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    is_author = cm and cm.id == post.author_id
    if not is_author and not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    # 이미지 파일 삭제
    for img in _from_json(post.images):
        try:
            _storage_delete(img)
        except OSError:
            pass
    db.delete(post)
    db.commit()
    return RedirectResponse(url=f"/board/{board}", status_code=303)


@app.post("/board/{board}/post/{post_id}/pin")
async def board_pin_post(request: Request, board: str, post_id: int, db: Session = Depends(get_db)):
    """관리자/중간관리자 전용: 게시글 핀(고정) 토글"""
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    post.is_pinned = not post.is_pinned
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{post_id}", status_code=303)


@app.post("/board/{board}/post/{post_id}/like")
async def board_like_post(request: Request, board: str, post_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    existing = db.query(PostLike).filter(PostLike.post_id == post_id, PostLike.member_id == cm.id).first()
    if existing:
        db.delete(existing)
    else:
        db.add(PostLike(post_id=post_id, member_id=cm.id))
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{post_id}", status_code=303)


@app.post("/board/{board}/post/{post_id}/comment")
async def board_add_comment(
    request: Request, board: str, post_id: int,
    content: str = Form(...), parent_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    if board not in BOARDS:
        raise HTTPException(status_code=404)
    post = db.query(Post).filter(Post.id == post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    if _is_comment_muted(cm):
        raise HTTPException(status_code=403, detail=f"댓글 작성이 {cm.comment_muted_until.strftime('%Y.%m.%d %H:%M')}까지 제한되었습니다.")
    if not content.strip():
        raise HTTPException(status_code=400, detail="댓글 내용을 입력하세요.")
    if len(content.strip()) > 2000:
        raise HTTPException(status_code=400, detail="댓글은 2,000자를 초과할 수 없습니다.")
    db.add(Comment(post_id=post_id, parent_id=parent_id, author_id=cm.id, content=content.strip()))
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{post_id}#comments", status_code=303)


@app.post("/board/{board}/comment/{comment_id}/delete")
async def board_delete_comment(request: Request, board: str, comment_id: int, db: Session = Depends(get_db)):
    comment = db.query(Comment).filter(Comment.id == comment_id).first()
    if not comment:
        raise HTTPException(status_code=404)
    # 댓글이 해당 게시판의 게시글에 속하는지 확인
    post = db.query(Post).filter(Post.id == comment.post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    cm = _current_member(request, db)
    is_author = cm and cm.id == comment.author_id
    if not is_author and not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    db.delete(comment)
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{comment.post_id}#comments", status_code=303)


@app.post("/board/{board}/comment/{comment_id}/like")
async def board_like_comment(request: Request, board: str, comment_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    comment = db.query(Comment).filter(Comment.id == comment_id).first()
    if not comment:
        raise HTTPException(status_code=404)
    # 댓글이 해당 게시판의 게시글에 속하는지 확인
    post = db.query(Post).filter(Post.id == comment.post_id, Post.board == board).first()
    if not post:
        raise HTTPException(status_code=404)
    existing = db.query(CommentLike).filter(CommentLike.comment_id == comment_id, CommentLike.member_id == cm.id).first()
    if existing:
        db.delete(existing)
    else:
        db.add(CommentLike(comment_id=comment_id, member_id=cm.id))
    db.commit()
    return RedirectResponse(url=f"/board/{board}/post/{comment.post_id}#comments", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  채팅
# ════════════════════════════════════════════════════════════════════════════

class _RoomManager:
    def __init__(self):
        self.connections: dict[int, dict] = defaultdict(dict)

    async def join(self, room_id: int, ws: WebSocket, member: Member):
        self.connections[room_id][ws] = {
            "id": member.id,
            "name": member.activity_name,
            "profile_image": member.profile_image,
        }

    def leave(self, room_id: int, ws: WebSocket):
        self.connections[room_id].pop(ws, None)

    def online(self, room_id: int) -> list[dict]:
        seen = {}
        for item in self.connections.get(room_id, {}).values():
            seen[item["id"]] = item
        return sorted(seen.values(), key=lambda row: row["name"])

    async def broadcast(self, room_id: int, msg: dict):
        dead = set()
        for ws in list(self.connections.get(room_id, {}).keys()):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.connections[room_id].pop(ws, None)


async def _broadcast_room_state(room_id: int, db: Session):
    members_payload = []
    for row in _room_members(db, room_id):
        if row.member:
            members_payload.append({
                "id": row.member_id,
                "name": row.member.activity_name,
                "role": row.role,
                "muted_until": row.muted_until.strftime("%Y.%m.%d %H:%M") if row.muted_until else "",
            })
    await _room_mgr.broadcast(room_id, {
        "type": "presence",
        "online": _room_mgr.online(room_id),
        "members": members_payload,
    })


_room_mgr = _RoomManager()


@app.get("/chat", response_class=HTMLResponse)
async def chat_list(
    request: Request,
    q: str = "",
    sort: str = "created",
    order: str = "desc",
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    query = db.query(ChatRoom)
    if q:
        compact_q = _compact_text(q)
        query = query.filter(
            or_(
                _compact_column(ChatRoom.name).contains(compact_q),
                _compact_column(ChatRoom.description).contains(compact_q),
            )
        )
    rooms = query.all()
    room_ids = [room.id for room in rooms]
    member_counts = dict(
        db.query(ChatRoomMember.room_id, func.count(ChatRoomMember.id))
        .filter(ChatRoomMember.room_id.in_(room_ids))
        .group_by(ChatRoomMember.room_id)
        .all()
    ) if room_ids else {}
    creator_ids = list({r.created_by_id for r in rooms})
    creators = _member_map(db, creator_ids)
    for r in rooms:
        r.creator = creators.get(r.created_by_id)
        r.online_count = len(_room_mgr.online(r.id))
        r.member_count = member_counts.get(r.id, 0)
        r.has_password = bool(r.password_hash)

    reverse = order != "asc"
    if sort == "name":
        rooms.sort(key=lambda room: room.name.lower(), reverse=reverse)
    elif sort == "members":
        rooms.sort(key=lambda room: (room.member_count, room.name.lower()), reverse=reverse)
    else:
        rooms.sort(key=lambda room: room.created_at, reverse=reverse)

    return _render(request,
        "chat/list.html",
        _ctx(request, db, rooms=rooms, query=q, current_sort=sort, current_order=order),
    )


@app.post("/chat/create")
async def chat_create(
    request: Request,
    name: str = Form(...), description: str = Form(""), password: str = Form(""),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    room = ChatRoom(
        name=name.strip(), description=description.strip(),
        password_hash=hash_password(password) if password else None,
        created_by_id=cm.id,
    )
    db.add(room)
    db.flush()
    db.add(ChatRoomMember(room_id=room.id, member_id=cm.id, role="owner"))
    db.commit()
    return RedirectResponse(url=f"/chat/{room.id}", status_code=303)


@app.post("/chat/{room_id}/delete")
async def chat_delete_room(request: Request, room_id: int, db: Session = Depends(get_db)):
    if not _is_privileged(request, db):
        raise HTTPException(status_code=403)
    room = db.query(ChatRoom).filter(ChatRoom.id == room_id).first()
    if room:
        db.delete(room)
        db.commit()
    return RedirectResponse(url="/chat", status_code=303)


@app.post("/chat/{room_id}/leave")
async def chat_leave_room(request: Request, room_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    room = db.query(ChatRoom).filter(ChatRoom.id == room_id).first()
    row = _chat_member(db, room_id, cm.id)
    if not room or not row:
        return RedirectResponse(url="/chat", status_code=303)
    was_owner = row.role == "owner"
    db.delete(row)
    db.flush()
    if was_owner:
        next_owner = (
            db.query(ChatRoomMember)
            .filter(ChatRoomMember.room_id == room_id, ChatRoomMember.role == "co_owner")
            .order_by(ChatRoomMember.joined_at.asc())
            .first()
        ) or (
            db.query(ChatRoomMember)
            .filter(ChatRoomMember.room_id == room_id)
            .order_by(ChatRoomMember.joined_at.asc())
            .first()
        )
        if next_owner:
            next_owner.role = "owner"
            room.created_by_id = next_owner.member_id
        else:
            db.delete(room)
    db.commit()
    return RedirectResponse(url="/chat", status_code=303)


@app.post("/chat/{room_id}/members/{member_id}/role")
async def chat_set_member_role(
    request: Request,
    room_id: int,
    member_id: int,
    role: str = Form(...),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    actor = _chat_member(db, room_id, cm.id) if cm else None
    if not actor or actor.role != "owner":
        raise HTTPException(status_code=403)
    target = _chat_member(db, room_id, member_id)
    room = db.query(ChatRoom).filter(ChatRoom.id == room_id).first()
    if not target or not room:
        raise HTTPException(status_code=404)
    if role == "transfer_owner":
        actor.role = "member"
        target.role = "owner"
        room.created_by_id = target.member_id
    elif role in ("co_owner", "member") and target.role != "owner":
        target.role = role
    db.commit()
    return RedirectResponse(url=f"/chat/{room_id}", status_code=303)


@app.post("/chat/{room_id}/members/{member_id}/mute")
async def chat_mute_member(
    request: Request,
    room_id: int,
    member_id: int,
    duration_minutes: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    actor = _chat_member(db, room_id, cm.id) if cm else None
    if not _is_privileged(request, db) and not (actor and actor.role in ("owner", "co_owner")):
        raise HTTPException(status_code=403)
    target = _chat_member(db, room_id, member_id)
    if not target:
        raise HTTPException(status_code=404)
    if target.role == "owner" and not _is_privileged(request, db):
        raise HTTPException(status_code=403, detail="방장은 채팅 제한할 수 없습니다.")
    minutes = _optional_int(duration_minutes, "채팅 금지 시간")
    target.muted_until = (_now() + timedelta(minutes=minutes)) if minutes and minutes > 0 else None
    db.commit()
    return RedirectResponse(url=f"/chat/{room_id}", status_code=303)


@app.get("/chat/{room_id}", response_class=HTMLResponse)
async def chat_room(request: Request, room_id: int, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)

    room = db.query(ChatRoom).filter(ChatRoom.id == room_id).first()
    if not room:
        raise HTTPException(status_code=404)
    room_member = _chat_member(db, room.id, cm.id)
    if not room.password_hash:
        room_member = _ensure_chat_member(db, room, cm)

    # 최근 메시지 100개
    messages = (
        db.query(ChatMessage)
        .filter(ChatMessage.room_id == room_id)
        .order_by(ChatMessage.created_at.asc())
        .limit(100)
        .all()
    )
    author_ids = list({m.author_id for m in messages})
    authors = _member_map(db, author_ids)
    for msg in messages:
        msg.author = authors.get(msg.author_id)

    resp = _render(request,
        "chat/room.html",
        _ctx(
            request,
            db,
            room=room,
            messages=messages,
            has_password=bool(room.password_hash and not room_member),
            room_member=room_member,
            room_members=_room_members(db, room.id),
            online_members=_room_mgr.online(room.id),
            can_manage_room=_can_manage_room(room_member, request, db),
        ),
    )
    # ws_token: non-httpOnly, JS에서 읽어 첫 메시지 인증에 사용
    ws_tok = create_member_token(cm.id)
    resp.set_cookie("ws_token", ws_tok, httponly=False, max_age=3600, samesite="lax", secure=IS_PRODUCTION)
    return resp


@app.get("/chat/{room_id}/history")
async def chat_history(
    request: Request, room_id: int,
    before_id: int = Query(0),
    db: Session = Depends(get_db),
):
    """채팅 이전 메시지 페이지네이션 API (JSON 반환)"""
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    room_member = _chat_member(db, room_id, cm.id)
    if not room_member:
        raise HTTPException(status_code=403, detail="채팅방 멤버가 아닙니다.")
    query = db.query(ChatMessage).filter(ChatMessage.room_id == room_id)
    if before_id:
        query = query.filter(ChatMessage.id < before_id)
    msgs = query.order_by(ChatMessage.created_at.desc()).limit(50).all()
    author_ids = list({m.author_id for m in msgs})
    authors = _member_map(db, author_ids)
    result = []
    for msg in reversed(msgs):   # 오래된 순으로 반환
        author = authors.get(msg.author_id)
        result.append({
            "id": msg.id,
            "author": author.activity_name if author else "",
            "profile_image": author.profile_image if author else None,
            "content": msg.content,
            "time": msg.created_at.strftime("%H:%M"),
            "is_mine": msg.author_id == cm.id,
        })
    return JSONResponse({"messages": result, "has_more": len(msgs) == 50})


@app.websocket("/ws/chat/{room_id}")
async def ws_chat(ws: WebSocket, room_id: int):
    await ws.accept()
    db = SessionLocal()
    try:
        # 첫 메시지에서 인증 정보 수신 (타임아웃 10초)
        try:
            auth_text = await asyncio.wait_for(ws.receive_text(), timeout=10.0)
            auth_data = json.loads(auth_text)
        except Exception:
            await ws.close(code=4001, reason="Auth required")
            return

        token = auth_data.get("token", "")
        password = auth_data.get("password", "")

        mid = verify_member_token(token)
        if not mid:
            await ws.close(code=4001, reason="Unauthorized")
            return

        room = db.query(ChatRoom).filter(ChatRoom.id == room_id).first()
        if not room:
            await ws.close(code=4004, reason="Room not found")
            return

        member = db.query(Member).filter(Member.id == mid).first()
        if not member:
            await ws.close(code=4001, reason="Member not found")
            return
        room_member = _chat_member(db, room_id, member.id)
        if room.password_hash and not room_member:
            if not password or not verify_team_password(password, room.password_hash):
                await ws.close(code=4003, reason="Wrong password")
                return
        room_member = _ensure_chat_member(db, room, member)

        await _room_mgr.join(room_id, ws, member)
        await _broadcast_room_state(room_id, db)
        await _room_mgr.broadcast(room_id, {
            "type": "system",
            "message": f"{member.activity_name}님이 입장했습니다.",
        })

        try:
            while True:
                text = await ws.receive_text()
                content = text.strip()[:2000]
                if not content:
                    continue
                db.refresh(room_member)
                if room_member.muted_until and room_member.muted_until > _now():
                    await ws.send_json({
                        "type": "error",
                        "message": f"{room_member.muted_until.strftime('%Y.%m.%d %H:%M')}까지 채팅이 제한되었습니다.",
                    })
                    continue
                msg = ChatMessage(room_id=room_id, author_id=mid, content=content)
                db.add(msg)
                db.commit()
                db.refresh(msg)
                await _room_mgr.broadcast(room_id, {
                    "type": "message",
                    "id": msg.id,
                    "author": member.activity_name,
                    "profile_image": member.profile_image,
                    "content": content,
                    "time": msg.created_at.strftime("%H:%M"),
                })
        except WebSocketDisconnect:
            pass
        finally:
            _room_mgr.leave(room_id, ws)
            await _broadcast_room_state(room_id, db)
            await _room_mgr.broadcast(room_id, {
                "type": "system",
                "message": f"{member.activity_name}님이 퇴장했습니다.",
            })
    finally:
        db.close()


# ════════════════════════════════════════════════════════════════════════════
#  관리자 — 공모전 자동 크롤링 (Phase 3)
# ════════════════════════════════════════════════════════════════════════════

# 크롤 결과를 서버 인스턴스 메모리에 캐시 (재크롤 전까지 유지, add/gpt 라우트용)
_crawl_cache: dict = {"items": [], "errors": [], "counts": {}, "crawled_at": None}


def _get_enabled_sources(db: Session) -> list:
    """AppSetting에서 활성 크롤링 소스 목록 로드. 없으면 전체 반환."""
    try:
        row = db.query(AppSetting).filter(AppSetting.key == "enabled_crawl_sources").first()
        if row and row.value:
            parsed = json.loads(row.value)
            if isinstance(parsed, list):
                return [s for s in parsed if s in CRAWL_SOURCES]
    except Exception:
        pass
    return list(CRAWL_SOURCES.keys())


def _save_enabled_sources(db: Session, sources: list) -> None:
    """활성 크롤링 소스 AppSetting에 저장."""
    val = json.dumps(sources, ensure_ascii=False)
    row = db.query(AppSetting).filter(AppSetting.key == "enabled_crawl_sources").first()
    if row:
        row.value = val
        row.updated_at = _now()
    else:
        db.add(AppSetting(key="enabled_crawl_sources", value=val))
    db.commit()


def _load_crawl_history(db: Session) -> list:
    """DB에서 크롤 세션 이력 로드 (최신순 최대 30개). 각 항목은 dict 형태."""
    try:
        rows = (
            db.query(CrawlSession)
            .order_by(CrawlSession.crawled_at.desc())
            .limit(30)
            .all()
        )
        history = []
        for row in rows:
            history.append({
                "id": row.id,
                "items": json.loads(row.items or "[]"),
                "errors": json.loads(row.errors or "[]"),
                "counts": json.loads(row.counts or "{}"),
                "sources": json.loads(row.sources or "[]"),
                "skipped": row.skipped_count,
                "item_count": row.item_count,
                "crawled_at": (row.crawled_at + timedelta(hours=9)).strftime("%Y년 %m월 %d일 %H:%M") if row.crawled_at else "",
            })
        return history
    except Exception:
        return []


def _save_crawl_session(db: Session, result: dict, sources: list) -> int:
    """크롤 결과를 DB CrawlSession에 저장. 저장된 session id 반환."""
    items = result.get("items", [])
    sess = CrawlSession(
        sources=json.dumps(sources, ensure_ascii=False),
        items=json.dumps(items, ensure_ascii=False),
        errors=json.dumps(result.get("errors", []), ensure_ascii=False),
        counts=json.dumps(result.get("counts", {}), ensure_ascii=False),
        item_count=len(items),
        skipped_count=result.get("skipped", 0),
    )
    db.add(sess)
    db.commit()
    db.refresh(sess)
    return sess.id


@app.get("/admin/crawl", response_class=HTMLResponse)
async def admin_crawl_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    # 최신 캐시에서 새로 추가된 항목 재필터 (메모리 캐시 기준)
    cache = dict(_crawl_cache)
    if cache.get("items"):
        refreshed, extra_skipped = _dedup_crawl_items(cache["items"], db)
        if extra_skipped:
            cache["items"] = refreshed
            _crawl_cache["items"] = refreshed
            cache["skipped"] = cache.get("skipped", 0) + extra_skipped
    history = _load_crawl_history(db)
    return _render(request,
        "admin/crawl.html",
        _ctx(request, db,
             cache=cache,
             history=history,
             all_tags=_get_tags(db),
             all_sources=CRAWL_SOURCES,
             enabled_sources=_get_enabled_sources(db)),
    )


def _norm_url(url: str) -> str:
    """URL 정규화 — http/https·www·끝슬래시·쿼리파라미터 순서 차이 무시"""
    if not url:
        return ""
    url = url.strip().lower()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.rstrip('/')
    if '?' in url:
        from urllib.parse import parse_qsl, urlencode as _enc
        path, qs = url.split('?', 1)
        try:
            params = sorted(parse_qsl(qs, keep_blank_values=True))
            url = path + ('?' + _enc(params) if params else '')
        except Exception:
            pass
    url = url.split('#')[0]
    return url


# 제목에서 제거할 일반 접미어 (공모전명 핵심 추출용)
_TITLE_SUFFIX_RE = re.compile(
    r'(공모전|공모|콘테스트|어워드|어워즈|경진대회|대회|모집|접수|안내'
    r'|참가자|신청|참여|온라인|오프라인|공고|발표|선정|지원|구인)+$'
)


def _norm_title(title: str) -> str:
    """제목 정규화 — 공백·특수문자·대소문자 차이 무시"""
    if not title:
        return ""
    title = re.sub(r'\s+', ' ', title).strip()
    title = re.sub(r'[^\w가-힣]', '', title)
    return title.lower()


def _core_title(nt: str) -> str:
    """정규화 제목에서 일반 접미어를 제거한 핵심 부분 반환 (유사도 비교용)"""
    return _TITLE_SUFFIX_RE.sub('', nt).strip()


def _is_similar_title(nt: str, seen_list: list[str], threshold: float = 0.82) -> bool:
    """정규화된 제목이 기존 제목 목록과 threshold 이상 유사하면 True.
    difflib.SequenceMatcher 기반 문자 유사도 비교.
    """
    from difflib import SequenceMatcher
    if len(nt) < 6:          # 너무 짧으면 퍼지 체크 무의미
        return False
    nc = _core_title(nt)     # 핵심 부분으로 비교 (접미어 차이 무시)
    for st in seen_list:
        # 길이 차이가 60% 이상이면 스킵 (최적화)
        if abs(len(nc) - len(st)) > max(len(nc), len(st)) * 0.6:
            continue
        if SequenceMatcher(None, nc, _core_title(st)).ratio() >= threshold:
            return True
    return False


def _dedup_crawl_items(items: list, db: Session) -> tuple[list, int]:
    """크롤링 결과 중복 제거.
    ① URL 정규화 exact match
    ② 제목 정규화 exact match
    ③ 제목 유사도 퍼지 매칭 (다른 사이트의 같은 공모전 감지)
    반환: (중복 제거된 items, 제거된 개수)
    """
    seen_urls:   set[str]  = set()
    seen_titles: set[str]  = set()
    seen_title_list: list[str] = []   # 퍼지 비교용 리스트

    def _add(link: str, title: str):
        nu = _norm_url(link)
        nt = _norm_title(title)
        if nu: seen_urls.add(nu)
        if nt and nt not in seen_titles:
            seen_titles.add(nt)
            seen_title_list.append(nt)

    # ① 공모전 DB 기존 항목
    for row in db.query(Competition.link, Competition.title).all():
        _add(row.link or "", row.title or "")

    # ② 이전 크롤 세션 항목
    for (raw_items,) in db.query(CrawlSession.items).all():
        try:
            for it in json.loads(raw_items or "[]"):
                _add(it.get("link") or "", it.get("title") or "")
        except Exception:
            continue

    filtered, skipped = [], 0
    for item in items:
        nu = _norm_url(item.get("link") or "")
        nt = _norm_title(item.get("title") or "")

        is_dup = (
            (nu and nu in seen_urls)           # URL 일치
            or (nt and nt in seen_titles)      # 제목 완전 일치
            or (nt and _is_similar_title(nt, seen_title_list))  # 제목 유사 (다른 사이트)
        )

        if is_dup:
            skipped += 1
        else:
            filtered.append(item)
            if nu: seen_urls.add(nu)
            if nt and nt not in seen_titles:
                seen_titles.add(nt)
                seen_title_list.append(nt)
    return filtered, skipped


@app.post("/admin/crawl/run")
async def admin_crawl_run(
    request: Request,
    sources: List[str] = Form(default=[]),
    db: Session = Depends(get_db),
):
    """크롤링 실행 — 완료까지 기다린 후 결과 페이지로 이동"""
    if r := _admin_redirect(request):
        return r
    global _crawl_cache

    # 선택 소스 저장 (비어있으면 전체)
    selected = [s for s in sources if s in CRAWL_SOURCES] or list(CRAWL_SOURCES.keys())
    _save_enabled_sources(db, selected)

    try:
        result = await _do_crawl_all(sources=selected)
    except Exception as exc:
        result = {"items": [], "errors": [f"크롤링 전체 실패: {type(exc).__name__}: {exc}"], "counts": {}}

    # ① 비공모전 항목 필터 (강의·할인·채용 등 제목 키워드 차단)
    raw_items = result.get("items", [])
    from crawler import _is_contest_title as _ict
    non_contest = [it for it in raw_items if not _ict(it.get("title", ""))]
    raw_items   = [it for it in raw_items if     _ict(it.get("title", ""))]
    if non_contest:
        result.setdefault("errors", []).extend(
            [f"비공모전 항목 제외: {it.get('title','?')}" for it in non_contest]
        )

    # ② 중복 제거 (URL·제목 정규화 + 유사도 퍼지 매칭)
    filtered_items, skipped = _dedup_crawl_items(raw_items, db)

    # 선택한 분야 외 항목 제외
    # - 원래 태그가 있는데 하나도 안 맞으면 → 완전 제외
    # - 원래 태그가 없는 항목(분류 불가) → 기타로 유지
    active_tags = set(_get_tags(db))
    tag_filtered = []
    for item in filtered_items:
        original_tags = item.get("tags", [])
        matching = [t for t in original_tags if t in active_tags]
        if original_tags and not matching:
            # 분류됐지만 선택한 분야에 없음 → 제외
            continue
        item["tags"] = matching
        tag_filtered.append(item)
    filtered_items = tag_filtered

    result["items"]             = filtered_items
    result["skipped"]           = skipped
    result["total_before_dedup"] = len(raw_items)
    result["crawled_at"]        = _now().strftime("%Y년 %m월 %d일 %H:%M")

    _crawl_cache = result
    # DB에 세션 저장 (영속)
    _save_crawl_session(db, result, selected)

    return RedirectResponse(url="/admin/crawl", status_code=303)


@app.post("/admin/crawl/session/{session_id}/delete")
async def admin_crawl_session_delete(
    session_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """크롤 세션 이력 삭제"""
    if r := _admin_redirect(request):
        return r
    sess = db.query(CrawlSession).filter(CrawlSession.id == session_id).first()
    if sess:
        db.delete(sess)
        db.commit()
    return RedirectResponse(url="/admin/crawl", status_code=303)


def _latest_crawl_items(db: Session) -> list:
    """최신 CrawlSession의 items 반환 (메모리 캐시 → DB 순으로 폴백)"""
    items = _crawl_cache.get("items", [])
    if items:
        return items
    # 메모리 캐시 없으면 DB 최신 세션에서 로드
    latest = db.query(CrawlSession).order_by(CrawlSession.id.desc()).first()
    if latest:
        return json.loads(latest.items or "[]")
    return []


@app.post("/admin/crawl/add")
async def admin_crawl_add(
    request: Request,
    idx: int = Form(...),
    db: Session = Depends(get_db),
):
    """크롤 결과 한 항목을 공모전으로 즉시 등록 (기본 정보만)"""
    if r := _admin_redirect(request):
        return r

    items = _latest_crawl_items(db)
    if idx < 0 or idx >= len(items):
        raise HTTPException(status_code=400, detail="잘못된 인덱스입니다. 페이지를 새로고침하거나 다시 크롤링해주세요.")

    item = items[idx]
    deadline_str = item.get("deadline")
    if not deadline_str:
        # 마감일 없는 경우 오늘+30일로 임시 설정
        deadline_str = (_today() + timedelta(days=30)).isoformat()

    comp = Competition(
        title=item.get("title", ""),
        organizer=item.get("organizer", ""),
        tags=json.dumps(item.get("tags", []), ensure_ascii=False),
        deadline=date.fromisoformat(deadline_str),
        prize=item.get("prize", ""),
        link=item.get("link", ""),
        description=f"[{item.get('source_label', '')}에서 자동 수집]\n\n원문 링크: {item.get('link', '')}",
        is_active=False,  # 자동 추가: 관리자 검토 후 활성화 필요
    )
    db.add(comp)
    db.commit()
    return RedirectResponse(url=f"/admin/edit/{comp.id}", status_code=303)


@app.post("/admin/crawl/add-bulk")
async def admin_crawl_add_bulk(
    request: Request,
    idxs: list[int] = Form(...),
    db: Session = Depends(get_db),
):
    """체크된 크롤 항목들을 기본 정보로 일괄 등록"""
    if r := _admin_redirect(request):
        return r

    items = _latest_crawl_items(db)
    added = 0
    for idx in idxs:
        if idx < 0 or idx >= len(items):
            continue
        item = items[idx]
        deadline_str = item.get("deadline") or (_today() + timedelta(days=30)).isoformat()
        try:
            dl = date.fromisoformat(deadline_str)
        except (ValueError, TypeError):
            dl = _today() + timedelta(days=30)
        comp = Competition(
            title=item.get("title", ""),
            organizer=item.get("organizer", ""),
            tags=json.dumps(item.get("tags", []), ensure_ascii=False),
            deadline=dl,
            prize=item.get("prize", ""),
            link=item.get("link", ""),
            description=f"[{item.get('source_label', '')}에서 자동 수집]\n\n원문 링크: {item.get('link', '')}",
            is_active=False,  # 자동 추가: 관리자 검토 후 활성화 필요
        )
        db.add(comp)
        added += 1
    db.commit()
    return RedirectResponse(url=f"/admin/competitions?bulk_added={added}", status_code=303)


@app.post("/admin/crawl/add-bulk-gpt")
async def admin_crawl_add_bulk_gpt(
    request: Request,
    idxs: list[int] = Form(...),
    db: Session = Depends(get_db),
):
    """체크된 크롤 항목들을 GPT로 파싱해 일괄 등록"""
    if r := _admin_redirect(request):
        return r
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(status_code=400, detail="OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")

    items = _latest_crawl_items(db)
    added = 0
    errors = []

    for idx in idxs:
        if idx < 0 or idx >= len(items):
            continue
        item = items[idx]
        try:
            await _gpt_process_item(item, db)
            added += 1
        except Exception as e:
            errors.append(f"{item.get('title','?')} — {str(e)[:80]}")

    return RedirectResponse(
        url=f"/admin/competitions?bulk_added={added}&bulk_errors={len(errors)}",
        status_code=303,
    )


async def _gpt_process_item(item: dict, db: Session) -> int:
    """
    크롤 항목 하나를 GPT로 파싱해 Competition을 생성하고 comp.id를 반환.
    단일 추가·일괄 추가 모두 이 함수를 공유한다.
    """
    link = item.get("link", "").strip()
    if not link:
        raise ValueError("URL이 없는 항목입니다.")

    # contestkorea 상대경로 복원

    # contestkorea 상대경로 복원: /sub/ 없이 저장된 캐시 URL 교정
    # 예) https://www.contestkorea.com/view.php?... → https://www.contestkorea.com/sub/view.php?...
    if "contestkorea.com/view.php" in link:
        link = link.replace("contestkorea.com/view.php", "contestkorea.com/sub/view.php")
    # www 없는 도메인 정규화
    link = link.replace("://contestkorea.com/", "://www.contestkorea.com/")

    from urllib.parse import urljoin as _urljoin
    from bs4 import BeautifulSoup as _BS

    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    _FETCH_HEADERS = {
        "User-Agent": _UA,
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.contestkorea.com/",
    }
    _FILE_EXTS = {"pdf", "hwp", "hwpx", "docx", "pptx", "xlsx", "zip"}

    # ── 결과 변수 초기화 ──────────────────────────────────────────────────────
    saved_image:        Optional[str] = None
    saved_files:        list          = []
    page_text:          str           = ""
    og_image_bytes:     Optional[bytes] = None
    og_image_ctype:     str           = "image/jpeg"
    content_img_list:   list          = []   # [(bytes, ctype), ...] 비전 분석용 이미지들
    attach_texts:       list[str]     = []   # PDF/HWP에서 추출한 텍스트
    parsed:             dict          = {}
    fetch_error:        str           = ""
    gpt_text_error:     str           = ""
    gpt_vision_error:   str           = ""

    # ── 1. 페이지 fetch ───────────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=5),
            follow_redirects=True,
            headers=_FETCH_HEADERS,
        ) as _cli:
            resp = await _cli.get(link)
            if resp.status_code >= 400:
                raise HTTPException(
                    status_code=502,
                    detail=f"페이지 응답 오류: HTTP {resp.status_code} — {link}",
                )

            # ── 1-a. HTML 파싱 ───────────────────────────────────────────────
            try:
                soup = _BS(resp.text, "lxml")
            except Exception:
                soup = _BS(resp.text, "html.parser")

            # ── 1-b. og:image URL + og:description 추출 ──────────────────────
            og_image_url = ""
            for _a in [
                {"property": "og:image"},
                {"name": "og:image"},
                {"property": "twitter:image"},
                {"name": "twitter:image"},
            ]:
                _m = soup.find("meta", attrs=_a)
                if _m and str(_m.get("content", "")).strip():
                    og_image_url = str(_m["content"]).strip()
                    break
            # og:image가 없으면 가장 큰 img src를 후보로
            if not og_image_url:
                for _img in soup.find_all("img", src=True):
                    _s = str(_img.get("src", "")).strip()
                    if _s and not _s.endswith(".gif") and len(_s) > 10:
                        og_image_url = _urljoin(link, _s)
                        break

            # og:description / meta description 추출 → 본문 텍스트 보강용
            og_description = ""
            for _a in [
                {"property": "og:description"},
                {"name": "og:description"},
                {"name": "description"},
                {"property": "twitter:description"},
            ]:
                _m = soup.find("meta", attrs=_a)
                if _m and len(str(_m.get("content", "")).strip()) > 20:
                    og_description = str(_m["content"]).strip()
                    break

            # 본문 영역 내 콘텐츠 이미지 수집 (최대 3개, 비전 분석용)
            _content_img_urls: list[str] = []
            if og_image_url:
                _content_img_urls.append(og_image_url)
            # 메인 영역의 큰 이미지도 후보로 추가 (공모요강 포스터 이미지 대응)
            for _img in soup.find_all("img", src=True):
                _s = str(_img.get("src", "")).strip()
                if not _s or _s.endswith(".gif"):
                    continue
                _full_s = _urljoin(link, _s)
                if _full_s not in _content_img_urls:
                    _content_img_urls.append(_full_s)
                if len(_content_img_urls) >= 4:
                    break

            # ── 1-c. 첨부파일 링크 수집 ─────────────────────────────────────
            _seen: set = set()
            attach_links: list = []
            for _a in soup.find_all("a", href=True):
                _href = str(_a.get("href", "")).strip()
                if not _href or _href.startswith("javascript"):
                    continue
                _link_text = _a.get_text(strip=True)
                _url_path  = _href.lower().split("?")[0]
                _ext_from_url  = _url_path.rsplit(".", 1)[-1] if "." in _url_path else ""
                # 링크 텍스트에서 확장자 추출 (file_dn.php 같은 동적 URL 대응)
                _ext_from_text = ""
                if _link_text:
                    _m = re.search(r"\.([a-z0-9]{2,5})$", _link_text.lower())
                    if _m:
                        _ext_from_text = _m.group(1)
                _ext = _ext_from_url if _ext_from_url in _FILE_EXTS else _ext_from_text
                # 공모전코리아 다운로드 URL 패턴도 포함 (file_dn.php)
                _is_download = "file_dn.php" in _url_path or "download" in _url_path
                if _ext in _FILE_EXTS or (_is_download and _ext_from_text in _FILE_EXTS):
                    _full = _urljoin(link, _href)
                    if _full not in _seen:
                        _seen.add(_full)
                        # 링크 텍스트에 파일명이 있으면 사용, 없으면 URL 마지막 경로
                        _name = _link_text or _href.rsplit("/", 1)[-1]
                        attach_links.append({
                            "name": _name,
                            "url":  _full,
                            "ext":  _ext or _ext_from_text,
                        })

            # ── 1-d. 본문 텍스트 추출 ────────────────────────────────────────
            for _tag in soup(["script", "style", "noscript"]):
                _tag.decompose()

            # 공모전코리아 정보 테이블 구조화 추출 (접수기간·시상내역 등 라벨:값 형식)
            # 공모전코리아 정보 테이블 구조화 추출 (접수기간·시상내역 등 라벨:값)
            _info_lines: list[str] = []
            _SKIP_KEYS = {"SNS", "오류", "특전", "접수하기", "홈페이지", "콘코"}
            _info_tbl = soup.select_one(".txt_area table") or soup.select_one(".view_top_area table")
            if _info_tbl:
                for _tr in _info_tbl.select("tr"):
                    _th = _tr.select_one("th")
                    _td = _tr.select_one("td")
                    if _th and _td:
                        _k = re.sub(r"\s+", " ", _th.get_text()).strip()
                        _v = re.sub(r"\s+", " ", _td.get_text()).strip()
                        # 불필요한 행 제외
                        if not _k or not _v or len(_v) > 300:
                            continue
                        if any(skip in _k for skip in _SKIP_KEYS):
                            continue
                        # 날짜 범위 레이블 명확화 (GPT 오독 방지)
                        if "접수기간" in _k:
                            # "2026.05.18 ~ 2026.07.13" → deadline 명시
                            _parts = _v.split("~")
                            if len(_parts) == 2:
                                _info_lines.append(f"접수 시작일: {_parts[0].strip()}")
                                _info_lines.append(f"접수 마감일(deadline): {_parts[1].strip()}")
                                continue
                        elif "심사기간" in _k:
                            _parts = _v.split("~")
                            if len(_parts) == 2:
                                _info_lines.append(f"심사 시작일: {_parts[0].strip()}")
                                _info_lines.append(f"심사 종료일: {_parts[1].strip()}")
                                continue
                        _info_lines.append(f"{_k}: {_v}")

            _info_text = ""
            if _info_lines:
                _info_text = (
                    "【공모전 정보 - 아래 데이터를 최우선으로 사용하세요】\n"
                    + "\n".join(_info_lines)
                    + "\n\n"
                )

            # 메인 본문: 공모전코리아 전용 선택자 → 일반 선택자 순
            _MAIN_SELECTORS = [
                ".view_cont_area",    # 공모전코리아 전체 뷰 컨테이너
                ".view_detail_area",  # 세부요강 영역
                ".tab_cont",          # 탭 콘텐츠
                ".view_area",
                ".view_cont",
                ".contest_view",
                ".board_view_wrap",
                ".sub_content",
                "#content .inner",
                "article",
                "main",
                ".content",
                "#content",
                ".board_view",
            ]
            _main_el = None
            for _sel in _MAIN_SELECTORS:
                _main_el = soup.select_one(_sel)
                if _main_el:
                    break

            if _main_el:
                for _t in _main_el(["nav", "footer", "header", "aside"]):
                    _t.decompose()
                _raw = _main_el.get_text(separator="\n")
            else:
                for _t in soup(["nav", "footer", "header", "aside"]):
                    _t.decompose()
                _raw = (soup.body or soup).get_text(separator="\n")

            _body_text = re.sub(r"[ \t]+", " ", _raw)
            _body_text = re.sub(r"\n{3,}", "\n\n", _body_text).strip()

            # og:description이 있으면 본문 앞에 추가 (JS 렌더링 페이지 보완)
            _og_prefix = f"【페이지 요약】\n{og_description}\n\n" if og_description else ""
            # 정보 테이블을 앞에 붙여 GPT가 날짜·시상내역을 먼저 인식하게 함
            page_text = (_og_prefix + _info_text + _body_text)[:12000]

            _log.info("GPT추가 page_text %d자, 첨부%d개, og_image=%s — %s",
                      len(page_text), len(attach_links), bool(og_image_url), link)

            # ── 1-e. og:image + 콘텐츠 이미지 다운로드 ─────────────────────
            if og_image_url:
                try:
                    _ir = await _cli.get(og_image_url, headers={"User-Agent": _UA})
                    if _ir.status_code == 200 and _ir.content:
                        _ct = _ir.headers.get("content-type", "image/jpeg").split(";")[0].strip()
                        og_image_bytes  = _ir.content
                        og_image_ctype  = _ct
                        if _is_valid_image_bytes(_ir.content) and len(_ir.content) <= MAX_IMAGE_SIZE:
                            _ie = og_image_url.split("?")[0].rsplit(".", 1)[-1].lower()
                            _ie = _ie if _ie in ("jpg", "jpeg", "png", "gif", "webp") else "jpg"
                            _ifn = f"{uuid.uuid4().hex}.{_ie}"
                            _storage_upload(_ir.content, _ifn, _ct or "image/jpeg")
                            saved_image = _ifn
                            content_img_list.append((_ir.content, _ct))
                except Exception as _e:
                    _log.warning("og:image 다운로드 실패: %s", _e)

            # 추가 콘텐츠 이미지 다운로드 (공모요강 포스터 등 이미지 안의 텍스트 추출용)
            for _cimg_url in _content_img_urls[1:4]:  # og:image 제외한 나머지, 최대 3장
                if len(content_img_list) >= 3:
                    break
                try:
                    _cr = await _cli.get(_cimg_url, headers={"User-Agent": _UA}, timeout=10)
                    if _cr.status_code == 200 and _cr.content and _is_valid_image_bytes(_cr.content):
                        _cct = _cr.headers.get("content-type", "image/jpeg").split(";")[0].strip()
                        if len(_cr.content) >= 20000:  # 20KB 이상인 이미지만 (포스터급)
                            content_img_list.append((_cr.content, _cct))
                except Exception:
                    pass

            # ── 1-f. 첨부파일 다운로드 + 텍스트 추출 ────────────────────────
            for _att in attach_links[:5]:
                try:
                    _fr = await _cli.get(_att["url"], headers={"User-Agent": _UA, "Referer": link})
                    if _fr.status_code != 200 or not _fr.content:
                        continue
                    if len(_fr.content) > MAX_FILE_SIZE:
                        continue

                    # Content-Disposition 에서 실제 파일명·확장자 추출 (file_dn.php 등 동적 URL 대응)
                    _cd = _fr.headers.get("content-disposition", "")
                    _cd_match = re.search(r'filename[^=]*=\s*["\']?([^"\';\r\n]+)', _cd, re.IGNORECASE)
                    if _cd_match:
                        _cd_name = _cd_match.group(1).strip().strip("\"'")
                        # URL 인코딩·EUC-KR 디코딩 처리
                        try:
                            from urllib.parse import unquote
                            _cd_name = unquote(_cd_name, encoding="utf-8")
                        except Exception:
                            pass
                        if _cd_name:
                            _att["name"] = _cd_name
                            _cd_ext = _cd_name.rsplit(".", 1)[-1].lower()
                            if _cd_ext in _FILE_EXTS:
                                _att["ext"] = _cd_ext

                    # 확장자가 여전히 없으면 Content-Type으로 추론
                    if not _att.get("ext") or _att["ext"] not in _FILE_EXTS:
                        _ct_map = {
                            "application/pdf": "pdf",
                            "application/haansofthwp": "hwp",
                            "application/x-hwp": "hwp",
                        }
                        _mime = _fr.headers.get("content-type", "").split(";")[0].strip()
                        _att["ext"] = _ct_map.get(_mime, _att.get("ext", "bin"))

                    _ffn = f"{uuid.uuid4().hex}.{_att['ext']}"
                    _storage_upload(_fr.content, _ffn, _fr.headers.get("content-type", "application/octet-stream"))
                    saved_files.append({"name": _att["name"], "path": _ffn})

                    # PDF·HWP 텍스트 추출
                    if _att["ext"] in ("pdf", "hwp", "hwpx"):
                        try:
                            from ai_parser import (
                                _extract_pdf_text, _extract_hwp_text, _extract_hwpx_text,
                            )
                            _ft = {
                                "pdf":  lambda b: _extract_pdf_text(b),
                                "hwp":  lambda b: _extract_hwp_text(b),
                                "hwpx": lambda b: _extract_hwpx_text(b),
                            }[_att["ext"]](_fr.content)
                            if _ft.strip():
                                attach_texts.append(
                                    f"[첨부: {_att['name']}]\n{_ft.strip()[:4000]}"
                                )
                        except Exception as _e:
                            _log.warning("첨부파일 텍스트 추출 실패(%s): %s", _att["name"], _e)
                except Exception as _e:
                    _log.warning("첨부파일 다운로드 실패(%s): %s", _att["url"], _e)
                    continue

    except HTTPException:
        raise
    except Exception as _e:
        fetch_error = f"{type(_e).__name__}: {_e}"
        _log.error("GPT추가 fetch 실패: %s — %s", fetch_error, link)

    # ── 2. GPT 텍스트 파싱 ────────────────────────────────────────────────────
    # 페이지 텍스트 + 첨부파일 텍스트 합산 (최대 14000자)
    _combined = page_text
    if attach_texts:
        _combined = page_text + "\n\n" + "\n\n".join(attach_texts)
    _combined = _combined[:14000].strip()

    if _combined:
        try:
            parsed = await parse_text(_combined)
            _log.info("GPT텍스트 파싱 성공: title=%s deadline=%s",
                      parsed.get("title", "?"), parsed.get("deadline", "?"))
        except Exception as _e:
            gpt_text_error = f"{type(_e).__name__}: {_e}"
            _log.warning("GPT텍스트 파싱 실패: %s", gpt_text_error)
    else:
        gpt_text_error = f"추출된 텍스트 없음 (fetch 오류: {fetch_error})"
        _log.warning("GPT추가 - 텍스트 없음, 비전으로 fallback: %s", link)

    # ── 3. GPT 비전 파싱 (텍스트 파싱 불완전/설명 부족 시 이미지로 보완) ──────
    # 설명이 200자 미만이거나 제목·마감일이 없으면 이미지 분석 실행
    _desc_too_short = len(parsed.get("description", "")) < 200
    _needs_vision = bool(content_img_list) and (
        not parsed.get("title")
        or not parsed.get("deadline")
        or _desc_too_short
    )
    if _needs_vision:
        for _img_bytes, _img_ctype in content_img_list[:2]:  # 최대 2장 분석
            try:
                _vp = await parse_image_file(_img_bytes, _img_ctype)
                _log.info("GPT비전 파싱 성공: title=%s deadline=%s desc=%d자",
                          _vp.get("title", "?"), _vp.get("deadline", "?"),
                          len(_vp.get("description", "")))
                # 텍스트 파싱 결과를 비전으로 보완
                for _k in ["title", "organizer", "deadline", "start_date", "announcement_date",
                           "review_dates", "prize", "tags", "link"]:
                    if not parsed.get(_k) and _vp.get(_k):
                        parsed[_k] = _vp[_k]
                # description은 더 긴 쪽을 사용
                _vis_desc = _vp.get("description", "")
                if len(_vis_desc) > len(parsed.get("description", "")):
                    parsed["description"] = _vis_desc
                    _log.info("GPT비전 description 채택 (%d자)", len(_vis_desc))
                # 제목·마감일·설명이 충분해지면 중단
                if parsed.get("title") and parsed.get("deadline") and len(parsed.get("description", "")) >= 200:
                    break
            except Exception as _e:
                gpt_vision_error = f"{type(_e).__name__}: {_e}"
                _log.warning("GPT비전 파싱 실패: %s", gpt_vision_error)

    # ── 4. 파싱 완전 실패 시 오류 표시 ───────────────────────────────────────
    if not parsed.get("title") and not parsed.get("deadline"):
        _errs = " / ".join(filter(None, [fetch_error, gpt_text_error, gpt_vision_error]))
        raise HTTPException(
            status_code=502,
            detail=f"GPT 파싱 실패 — {_errs or '원인 불명'}\n\n링크: {link}",
        )

    # ── 5. 크롤 캐시 기본값으로 보완 ─────────────────────────────────────────
    if not parsed.get("title"):
        parsed["title"] = item.get("title", "")
    if not parsed.get("organizer"):
        parsed["organizer"] = item.get("organizer", "")
    if not parsed.get("deadline"):
        parsed["deadline"] = item.get("deadline")
    if not parsed.get("tags"):
        parsed["tags"] = item.get("tags", [])

    # ── 6. Competition 생성 ───────────────────────────────────────────────────
    deadline_str = parsed.get("deadline") or (_today() + timedelta(days=30)).isoformat()

    _rd = parsed.get("review_dates") or []
    if not isinstance(_rd, list):
        _rd = []

    # review_dates의 "결과 발표/시상" 항목 날짜가 YYYY-MM-DD이면 announcement_date 자동 동기화
    if not parsed.get("announcement_date"):
        _ANNOUNCE_KWS = ("발표", "결과", "시상", "당선")
        for _rdi in _rd:
            if any(kw in str(_rdi.get("label", "")) for kw in _ANNOUNCE_KWS):
                _rdi_date = str(_rdi.get("date", "")).strip()
                try:
                    date.fromisoformat(_rdi_date)   # YYYY-MM-DD 검증
                    parsed["announcement_date"] = _rdi_date
                    _log.info("GPT추가 announcement_date 자동 동기화: %s ← %s", _rdi_date, _rdi.get("label"))
                    break
                except (ValueError, TypeError):
                    pass  # 텍스트 날짜("8월 말")는 무시

    def _safe_date(val) -> Optional[date]:
        try:
            return date.fromisoformat(str(val)) if val else None
        except (ValueError, TypeError):
            return None

    comp = Competition(
        title        = parsed.get("title", ""),
        organizer    = parsed.get("organizer", ""),
        tags         = json.dumps(parsed.get("tags", []), ensure_ascii=False),
        start_date   = _safe_date(parsed.get("start_date")),
        deadline     = date.fromisoformat(deadline_str),
        announcement_date = _safe_date(parsed.get("announcement_date")),
        review_dates = json.dumps(_rd, ensure_ascii=False),
        prize        = parsed.get("prize", "") or item.get("prize", ""),
        link         = link,
        description  = parsed.get("description", ""),
        image        = saved_image,
        files        = json.dumps(saved_files, ensure_ascii=False),
        is_active    = False,  # 자동 추가(GPT): 관리자 검토 후 활성화 필요
    )
    db.add(comp)
    db.commit()
    return comp.id   # ← 라우트에서 redirect 처리


@app.post("/admin/crawl/add-from-url")
async def admin_crawl_add_from_url(
    request: Request,
    url: str = Form(...),
    db: Session = Depends(get_db),
):
    """URL 직접 입력으로 GPT 파싱 후 공모전 등록"""
    if r := _admin_redirect(request):
        return r
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(status_code=400, detail="OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="올바른 URL을 입력해주세요. (http:// 또는 https://로 시작)")
    # link만 있는 최소 item 딕셔너리로 _gpt_process_item 호출
    item = {"link": url, "title": "", "organizer": "", "deadline": None, "prize": "", "tags": []}
    comp_id = await _gpt_process_item(item, db)
    return RedirectResponse(url=f"/admin/edit/{comp_id}", status_code=303)


@app.post("/admin/crawl/add-with-gpt")
async def admin_crawl_add_with_gpt(
    request: Request,
    idx: int = Form(...),
    db: Session = Depends(get_db),
):
    """크롤 결과를 GPT로 파싱해 공모전 등록 (단일)"""
    if r := _admin_redirect(request):
        return r
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(status_code=400, detail="OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
    items = _latest_crawl_items(db)
    if idx < 0 or idx >= len(items):
        raise HTTPException(status_code=400, detail="잘못된 인덱스입니다. 페이지를 새로고침하거나 다시 크롤링해주세요.")
    comp_id = await _gpt_process_item(items[idx], db)
    return RedirectResponse(url=f"/admin/edit/{comp_id}", status_code=303)


# ── 관리자 설정 ──────────────────────────────────────────────────────────────

@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    tags = _get_tags(db)
    return _render(request,
        "admin/settings.html",
        _ctx(request, db,
             tags=tags,
             tags_json=json.dumps(tags, ensure_ascii=False),
             all_cats=_CONTESTKOREA_CATS),
    )


@app.post("/admin/settings/tags")
async def admin_settings_tags(
    request: Request,
    tags_json: str = Form("[]"),
    db: Session = Depends(get_db),
):
    """분야 태그 목록 저장"""
    if r := _admin_redirect(request):
        return r
    try:
        new_tags = json.loads(tags_json)
        if not isinstance(new_tags, list):
            raise ValueError
        new_tags = [str(t).strip() for t in new_tags if str(t).strip()]
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="태그 형식이 올바르지 않습니다.")

    if not new_tags:
        raise HTTPException(status_code=400, detail="태그를 최소 1개 이상 입력하세요.")

    row = db.query(AppSetting).filter(AppSetting.key == "tags").first()
    if row:
        row.value = json.dumps(new_tags, ensure_ascii=False)
        row.updated_at = _now()
    else:
        db.add(AppSetting(key="tags", value=json.dumps(new_tags, ensure_ascii=False)))
    db.commit()
    return RedirectResponse(url="/admin/settings?saved=1", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  취업 게시판
# ════════════════════════════════════════════════════════════════════════════

# 선택 가능한 전체 분야 목록 (관리자가 이 중 활성화할 분야를 선택)
_ALL_JOB_TYPES = ["인턴", "채용", "서포터즈", "대외활동", "봉사활동", "교육/강연", "기타"]
JOB_TYPES = _ALL_JOB_TYPES  # fallback (DB 접근 전 사용)


def _get_job_types(db: Session) -> list[str]:
    """AppSetting에서 활성 취업 분야 목록을 로드. 설정 없으면 기본값 반환."""
    try:
        row = db.query(AppSetting).filter(AppSetting.key == "job_types").first()
        if row and row.value:
            parsed = json.loads(row.value)
            if isinstance(parsed, list) and parsed:
                return parsed
    except Exception:
        pass
    return list(_ALL_JOB_TYPES)


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page(
    request: Request,
    q: str = Query(""),
    job_type: str = Query("all"),
    sort: str = Query("deadline"),
    page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    today = _today()

    # ── 피처드: 마감 임박 8개 + 조회수 높은 10개 → 최대 15개 ──
    _by_deadline = (
        db.query(JobPosting)
        .filter(JobPosting.deadline != None, JobPosting.deadline >= today)  # noqa: E711
        .order_by(JobPosting.deadline.asc())
        .limit(8).all()
    )
    _by_views = (
        db.query(JobPosting)
        .filter(or_(JobPosting.deadline == None, JobPosting.deadline >= today))  # noqa: E711
        .order_by(JobPosting.view_count.desc())
        .limit(10).all()
    )
    _seen: set = set()
    featured_jobs: list = []
    for _j in _by_deadline + _by_views:
        if _j.id not in _seen and len(featured_jobs) < 15:
            _seen.add(_j.id)
            featured_jobs.append(_j)

    # ── 메인 그리드 쿼리 ──
    query = db.query(JobPosting)

    if q:
        q_like = f"%{q}%"
        query = query.filter(
            or_(JobPosting.title.ilike(q_like), JobPosting.company.ilike(q_like))
        )
    active_job_types = _get_job_types(db)
    if job_type != "all" and job_type in active_job_types:
        query = query.filter(JobPosting.job_type == job_type)

    if sort == "views":
        query = query.order_by(JobPosting.view_count.desc())
    elif sort == "newest":
        query = query.order_by(JobPosting.created_at.desc())
    else:
        query = query.order_by(
            case(
                (JobPosting.deadline == None, 1),  # noqa: E711
                else_=case(
                    (JobPosting.deadline >= today, 0),
                    else_=2,
                )
            ),
            JobPosting.deadline.asc().nulls_last(),
            JobPosting.created_at.desc(),
        )

    all_postings = query.all()
    _JOB_PAGE_SIZE = 12
    total_count  = len(all_postings)
    total_pages  = max(1, (total_count + _JOB_PAGE_SIZE - 1) // _JOB_PAGE_SIZE)
    page         = max(1, min(page, total_pages))
    postings     = all_postings[(_JOB_PAGE_SIZE * (page - 1)):(_JOB_PAGE_SIZE * page)]

    return _render(request, "jobs.html", _ctx(request, db,
        featured_jobs=featured_jobs,
        postings=postings,
        query=q,
        current_job_type=job_type,
        current_sort=sort,
        job_types=active_job_types,
        today=today,
        page=page, total_pages=total_pages, total_count=total_count,
    ))


@app.get("/job/{job_id}")
async def job_detail(job_id: int, db: Session = Depends(get_db)):
    posting = db.query(JobPosting).filter(JobPosting.id == job_id).first()
    if not posting:
        raise HTTPException(status_code=404, detail="공고를 찾을 수 없습니다.")
    posting.view_count = (posting.view_count or 0) + 1
    db.commit()
    return RedirectResponse(url=posting.link, status_code=302)


# ── 관리자 취업 ──────────────────────────────────────────────────────────────

# 메모리 캐시 (재배포 시 소멸 → DB로 복원)
_job_crawl_cache: list = []


def _latest_job_crawl_items(db: Session) -> list:
    """메모리 캐시가 비어 있으면 DB 최신 JobCrawlSession에서 복원"""
    if _job_crawl_cache:
        return _job_crawl_cache
    try:
        sess = (
            db.query(JobCrawlSession)
            .order_by(JobCrawlSession.id.desc())
            .first()
        )
        if sess:
            return json.loads(sess.items or "[]")
    except Exception:
        pass
    return []


def _load_job_crawl_history(db: Session) -> list:
    """DB에서 취업 크롤 히스토리 목록을 로드 (최신 20개)
    이미 추가된 공고는 already_added=True 로 표시하고 목록 하단으로 정렬.
    """
    try:
        # 현재 DB에 등록된 공고 링크 세트
        existing_links: set = {
            row.link for row in db.query(JobPosting.link).all() if row.link
        }

        rows = (
            db.query(JobCrawlSession)
            .order_by(JobCrawlSession.id.desc())
            .limit(20)
            .all()
        )
        history = []
        for row in rows:
            items = json.loads(row.items or "[]")

            # 이미 추가된 항목 표시
            for item in items:
                item["already_added"] = bool(
                    item.get("link") and item["link"] in existing_links
                )

            # 미추가 항목 먼저, 이미 추가된 항목 뒤로
            items.sort(key=lambda x: x.get("already_added", False))

            history.append({
                "id":         row.id,
                "items":      items,
                "errors":     json.loads(row.errors or "[]"),
                "counts":     json.loads(row.counts or "{}"),
                "sources":    json.loads(row.sources or "[]"),
                "item_count": row.item_count,
                "crawled_at": (
                    (row.crawled_at + timedelta(hours=9)).strftime("%Y년 %m월 %d일 %H:%M")
                    if row.crawled_at else ""
                ),
            })
        return history
    except Exception:
        return []


def _save_job_crawl_session(db: Session, result: dict, sources: list) -> None:
    """취업 크롤 결과를 DB에 저장"""
    items = result.get("items", [])
    sess = JobCrawlSession(
        sources=json.dumps(sources, ensure_ascii=False),
        items=json.dumps(items, ensure_ascii=False),
        errors=json.dumps(result.get("errors", []), ensure_ascii=False),
        counts=json.dumps(result.get("counts", {}), ensure_ascii=False),
        item_count=len(items),
    )
    db.add(sess)
    db.commit()


def _job_item_to_posting(item: dict) -> JobPosting:
    """크롤 결과 dict → JobPosting 모델"""
    deadline_val = None
    if item.get("deadline"):
        try:
            deadline_val = date.fromisoformat(item["deadline"])
        except (ValueError, TypeError):
            pass
    return JobPosting(
        title=item.get("title", "")[:500],
        company=item.get("company", "")[:200],
        job_type=item.get("job_type", "인턴")[:50],
        location=item.get("location", "")[:200],
        deadline=deadline_val,
        link=item.get("link", "")[:1000],
        source=item.get("source", "")[:50],
        source_label=item.get("source_label", "")[:100],
    )


@app.get("/admin/jobs", response_class=HTMLResponse)
async def admin_jobs_list(
    request: Request,
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    today = _today()
    postings = db.query(JobPosting).order_by(JobPosting.created_at.desc()).all()
    bulk_deleted = request.query_params.get("bulk_deleted")
    return _render(request, "admin/jobs.html", _ctx(request, db,
        postings=postings,
        today=today,
        job_types=_get_job_types(db),
        bulk_deleted=int(bulk_deleted) if bulk_deleted else None,
    ))


# ── Career 분야 설정 ──────────────────────────────────────────────────────────

@app.get("/admin/jobs/settings", response_class=HTMLResponse)
async def admin_jobs_settings_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    active = _get_job_types(db)
    return _render(request, "admin/jobs_settings.html", _ctx(request, db,
        active_job_types=active,
        active_json=json.dumps(active, ensure_ascii=False),
        all_job_types=_ALL_JOB_TYPES,
    ))


@app.post("/admin/jobs/settings/types")
async def admin_jobs_settings_types(
    request: Request,
    types_json: str = Form("[]"),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    try:
        new_types = [t for t in json.loads(types_json) if isinstance(t, str) and t.strip()]
    except Exception:
        new_types = []
    if not new_types:
        raise HTTPException(status_code=400, detail="분야를 최소 1개 이상 선택하세요.")
    row = db.query(AppSetting).filter(AppSetting.key == "job_types").first()
    if row:
        row.value = json.dumps(new_types, ensure_ascii=False)
        row.updated_at = _now()
    else:
        db.add(AppSetting(key="job_types", value=json.dumps(new_types, ensure_ascii=False)))
    db.commit()
    return RedirectResponse(url="/admin/jobs/settings?saved=1", status_code=303)


@app.get("/admin/jobs/crawl", response_class=HTMLResponse)
async def admin_jobs_crawl_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    history = _load_job_crawl_history(db)
    return _render(request, "admin/jobs_crawl.html", _ctx(request, db,
        job_sources=JOB_SOURCES,
        history=history,
    ))


@app.post("/admin/jobs/crawl/run")
async def admin_jobs_crawl_run(
    request: Request,
    sources: List[str] = Form(default=[]),
    db: Session = Depends(get_db),
):
    global _job_crawl_cache
    if r := _admin_redirect(request):
        return r

    selected = [s for s in sources if s in JOB_SOURCES] or list(JOB_SOURCES.keys())
    result = await _do_crawl_jobs(selected)

    items = result.get("items", [])
    _job_crawl_cache = items
    _save_job_crawl_session(db, result, selected)

    history = _load_job_crawl_history(db)
    return _render(request, "admin/jobs_crawl.html", _ctx(request, db,
        job_sources=JOB_SOURCES,
        history=history,
    ))


@app.post("/admin/jobs/add")
async def admin_jobs_add_single(
    request: Request,
    idx: int = Form(...),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    items = _latest_job_crawl_items(db)
    if idx < 0 or idx >= len(items):
        return RedirectResponse(url="/admin/jobs/crawl?err=invalid_idx", status_code=303)
    try:
        posting = _job_item_to_posting(items[idx])
        db.add(posting)
        db.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/jobs/crawl?added=1", status_code=303)


@app.post("/admin/jobs/add-bulk")
async def admin_jobs_add_bulk(
    request: Request,
    indices: List[int] = Form(default=[]),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    items = _latest_job_crawl_items(db)
    added, errors = 0, 0
    for idx in indices:
        if idx < 0 or idx >= len(items):
            errors += 1
            continue
        try:
            posting = _job_item_to_posting(items[idx])
            db.add(posting)
            added += 1
        except Exception:
            errors += 1
    db.commit()
    return RedirectResponse(url=f"/admin/jobs/crawl?bulk_added={added}&bulk_errors={errors}", status_code=303)


@app.post("/admin/jobs/crawl/session/{sess_id}/delete")
async def admin_jobs_crawl_session_delete(
    request: Request,
    sess_id: int,
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    sess = db.query(JobCrawlSession).filter(JobCrawlSession.id == sess_id).first()
    if sess:
        db.delete(sess)
        db.commit()
    return RedirectResponse(url="/admin/jobs/crawl", status_code=303)


@app.post("/admin/jobs/delete/{job_id}")
async def admin_jobs_delete(request: Request, job_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    posting = db.query(JobPosting).filter(JobPosting.id == job_id).first()
    if posting:
        db.delete(posting)
        db.commit()
    referer = request.headers.get("referer", "")
    if "/admin/jobs" in referer and "crawl" not in referer:
        return RedirectResponse(url="/admin/jobs", status_code=303)
    return RedirectResponse(url="/jobs", status_code=303)


@app.post("/admin/jobs/delete-bulk")
async def admin_jobs_delete_bulk(
    request: Request,
    job_ids: List[int] = Form(default=[]),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    count = 0
    if job_ids:
        count = db.query(JobPosting).filter(JobPosting.id.in_(job_ids)).delete(synchronize_session=False)
        db.commit()
    return RedirectResponse(url=f"/admin/jobs?bulk_deleted={count}", status_code=303)


# ══════════════════════════════════════════════════════════════════════════════
# 교과목 게시판  /course
# ══════════════════════════════════════════════════════════════════════════════

_SEMESTERS = [("1", "1학기"), ("2", "2학기"), ("여름", "여름학기"), ("겨울", "겨울학기")]


@app.get("/course", response_class=HTMLResponse)
async def course_list(
    request: Request,
    subject: str = "",   # 교과목명 검색
    prof: str = "",      # 교수명 검색
    grade: str = "",     # 학년 필터 (1~4)
    semester: str = "",  # 학기 필터 (1/2/여름/겨울)
    db: Session = Depends(get_db),
):
    from sqlalchemy import func
    query = db.query(CourseEntry)
    if subject:
        query = query.filter(CourseEntry.subject_name.ilike(f"%{subject}%"))
    if prof:
        query = query.filter(CourseEntry.professor.ilike(f"%{prof}%"))
    if grade and grade.isdigit():
        query = query.filter(CourseEntry.grade == int(grade))
    if semester:
        query = query.filter(CourseEntry.semester == semester)
    entries = query.order_by(CourseEntry.subject_name).all()

    # 글 수 집계
    ids = [e.id for e in entries]
    review_counts, exam_counts = {}, {}
    if ids:
        rows = (
            db.query(CoursePost.course_id, CoursePost.post_type, func.count())
            .filter(CoursePost.course_id.in_(ids))
            .group_by(CoursePost.course_id, CoursePost.post_type)
            .all()
        )
        for cid, ptype, cnt in rows:
            if ptype == "review":
                review_counts[cid] = cnt
            else:
                exam_counts[cid] = cnt

    # 학년별 그룹핑: {1: [...], 2: [...], 3: [...], 4: [...], None: [...]}
    from collections import defaultdict
    grouped: dict = defaultdict(list)
    for e in entries:
        grouped[e.grade].append(e)
    # 정렬: 1~4학년 순서, 마지막에 전학년(None)
    grade_order = [1, 2, 3, 4, None]
    grouped_list = [(g, grouped[g]) for g in grade_order if grouped[g]]

    ctx = _ctx(request, db,
               grouped_list=grouped_list,
               review_counts=review_counts,
               exam_counts=exam_counts,
               subject=subject,
               prof=prof,
               grade_filter=grade,
               semester_filter=semester,
               semesters=_SEMESTERS)
    return _render(request, "course/list.html", ctx)


@app.get("/course/new", response_class=HTMLResponse)
async def course_new_form(request: Request, db: Session = Depends(get_db)):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    return _render(request, "course/new.html", _ctx(request, db))


@app.post("/course/new")
async def course_new_submit(
    request: Request,
    subject_name: str = Form(...),
    grade: str = Form(""),
    semester: str = Form(""),
    professor: str = Form(""),
    department: str = Form(""),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    subject_name = subject_name.strip()
    if not subject_name:
        return _render(request, "course/new.html", _ctx(request, db, error="교과목명을 입력해주세요."))
    grade_int = int(grade) if grade and grade.isdigit() and 1 <= int(grade) <= 4 else None
    valid_sems = {v for v, _ in _SEMESTERS}
    entry = CourseEntry(
        subject_name=subject_name,
        grade=grade_int,
        semester=semester if semester in valid_sems else "",
        professor=professor.strip(),
        department=department.strip(),
        created_by=cm.id,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return RedirectResponse(url=f"/course/{entry.id}", status_code=303)


@app.get("/course/{course_id}", response_class=HTMLResponse)
async def course_detail(
    request: Request,
    course_id: int,
    tab: str = "review",
    year: str = "",
    sem: str = "",
    db: Session = Depends(get_db),
):
    entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="교과목을 찾을 수 없습니다.")

    post_type = "review" if tab != "exam" else "exam"
    base_q = db.query(CoursePost).filter(
        CoursePost.course_id == course_id, CoursePost.post_type == post_type
    )

    # 필터용 전체 목록에서 연도·학기 목록 수집
    all_posts = base_q.all()
    avail_years = sorted({p.year for p in all_posts if p.year}, reverse=True)
    avail_sems = sorted({p.semester for p in all_posts if p.semester},
                        key=lambda s: {"1": 0, "2": 1, "여름": 2, "겨울": 3}.get(s, 9))

    # 필터 적용
    q = base_q
    if year and year.isdigit():
        q = q.filter(CoursePost.year == int(year))
    if sem:
        q = q.filter(CoursePost.semester == sem)
    posts = q.order_by(CoursePost.year.desc().nulls_last(), CoursePost.created_at.desc()).all()

    # 작성자 정보 (익명 제외)
    author_ids = {p.author_id for p in posts if not p.is_anonymous and p.author_id}
    authors = {}
    if author_ids:
        for m in db.query(Member).filter(Member.id.in_(author_ids)).all():
            authors[m.id] = m

    review_count = db.query(CoursePost).filter(
        CoursePost.course_id == course_id, CoursePost.post_type == "review"
    ).count()
    exam_count = db.query(CoursePost).filter(
        CoursePost.course_id == course_id, CoursePost.post_type == "exam"
    ).count()

    creator = db.query(Member).filter(Member.id == entry.created_by).first() if entry.created_by else None

    # 첨부 파일 (post_id → files 맵)
    post_ids = [p.id for p in posts]
    files_map: dict = {}
    if post_ids:
        cm_now = _current_member(request, db)
        is_priv = _is_privileged(request, db)
        all_cfiles = db.query(CourseFile).filter(CourseFile.course_post_id.in_(post_ids)).all()
        for cf in all_cfiles:
            visible = (cf.is_approved is True) or is_priv or (cm_now and cm_now.id == cf.uploaded_by)
            if visible:
                files_map.setdefault(cf.course_post_id, []).append(cf)

    ctx = _ctx(request, db,
               entry=entry,
               posts=posts,
               authors=authors,
               files_map=files_map,
               tab=tab,
               year_filter=year,
               sem_filter=sem,
               avail_years=avail_years,
               avail_sems=avail_sems,
               review_count=review_count,
               exam_count=exam_count,
               creator=creator,
               semesters=_SEMESTERS)
    return _render(request, "course/detail.html", ctx)


@app.get("/course/{course_id}/write", response_class=HTMLResponse)
async def course_write_form(
    request: Request,
    course_id: int,
    tab: str = "review",
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
    if not entry:
        raise HTTPException(status_code=404)
    return _render(request, "course/write.html",
                   _ctx(request, db, entry=entry, tab=tab, semesters=_SEMESTERS))


_COURSE_FILE_EXTS = {"hwp", "hwpx", "html", "htm", "pdf", "zip"}
_COURSE_FILE_MAX  = 50 * 1024 * 1024   # 50 MB


@app.post("/course/{course_id}/write")
async def course_write_submit(
    request: Request,
    course_id: int,
    post_type: str = Form(...),
    content: str = Form(...),
    year: str = Form(""),
    semester: str = Form(""),
    is_anonymous: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
    if not entry:
        raise HTTPException(status_code=404)

    content = content.strip()
    if not content:
        return _render(request, "course/write.html",
                       _ctx(request, db, entry=entry, tab=post_type,
                            semesters=_SEMESTERS, error="내용을 입력해주세요."))

    valid_semesters = {s for s, _ in _SEMESTERS}
    ptype = "review" if post_type == "review" else "exam"
    cp = CoursePost(
        course_id=course_id,
        post_type=ptype,
        content=content,
        year=int(year) if year and year.isdigit() else None,
        semester=semester if semester in valid_semesters else "",
        is_anonymous=bool(is_anonymous),
        author_id=cm.id,
    )
    db.add(cp)
    db.flush()   # cp.id 확보

    # 파일 업로드 (시험정보만)
    if ptype == "exam" and files:
        for uf in files:
            if not uf.filename:
                continue
            ext = Path(uf.filename).suffix.lstrip(".").lower()
            if ext not in _COURSE_FILE_EXTS:
                continue
            raw = await uf.read()
            if len(raw) > _COURSE_FILE_MAX:
                continue
            safe_name = f"cf_{uuid.uuid4().hex}{Path(uf.filename).suffix}"
            _storage_upload(raw, safe_name, uf.content_type or "application/octet-stream")
            db.add(CourseFile(
                course_post_id=cp.id,
                course_id=course_id,
                filename=safe_name,
                original_name=uf.filename,
                file_size=len(raw),
                uploaded_by=cm.id,
                is_approved=None,
            ))

    db.commit()
    return RedirectResponse(url=f"/course/{course_id}?tab={ptype}", status_code=303)


@app.get("/course/{course_id}/post/{post_id}/edit", response_class=HTMLResponse)
async def course_post_edit_form(
    request: Request,
    course_id: int,
    post_id: int,
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
    if not entry:
        raise HTTPException(status_code=404)
    cp = db.query(CoursePost).filter(CoursePost.id == post_id, CoursePost.course_id == course_id).first()
    if not cp:
        raise HTTPException(status_code=404)
    is_priv = _is_privileged(request, db)
    if cm.id != cp.author_id and not is_priv:
        raise HTTPException(status_code=403)
    existing_files = db.query(CourseFile).filter(CourseFile.course_post_id == post_id).all()
    return _render(request, "course/edit.html",
                   _ctx(request, db, entry=entry, post=cp, tab=cp.post_type,
                        semesters=_SEMESTERS, existing_files=existing_files))


@app.post("/course/{course_id}/post/{post_id}/edit")
async def course_post_edit_submit(
    request: Request,
    course_id: int,
    post_id: int,
    content: str = Form(...),
    year: str = Form(""),
    semester: str = Form(""),
    is_anonymous: str = Form(""),
    delete_file_ids: List[str] = Form(default=[]),
    files: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        return RedirectResponse(url="/member/login", status_code=303)
    cp = db.query(CoursePost).filter(CoursePost.id == post_id, CoursePost.course_id == course_id).first()
    if not cp:
        raise HTTPException(status_code=404)
    is_priv = _is_privileged(request, db)
    if cm.id != cp.author_id and not is_priv:
        raise HTTPException(status_code=403)
    content = content.strip()
    if not content:
        entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
        existing_files = db.query(CourseFile).filter(CourseFile.course_post_id == post_id).all()
        return _render(request, "course/edit.html",
                       _ctx(request, db, entry=entry, post=cp, tab=cp.post_type,
                            semesters=_SEMESTERS, existing_files=existing_files,
                            error="내용을 입력해주세요."))
    valid_semesters = {s for s, _ in _SEMESTERS}
    cp.content = content
    cp.year = int(year) if year and year.isdigit() else None
    cp.semester = semester if semester in valid_semesters else ""
    cp.is_anonymous = bool(is_anonymous)

    # 기존 파일 삭제
    for fid_str in delete_file_ids:
        try:
            fid = int(fid_str)
        except ValueError:
            continue
        cf = db.query(CourseFile).filter(CourseFile.id == fid, CourseFile.course_post_id == post_id).first()
        if cf:
            _storage_delete(cf.filename)
            db.delete(cf)

    # 새 파일 업로드 (시험정보만)
    if cp.post_type == "exam" and files:
        for uf in files:
            if not uf.filename:
                continue
            ext = Path(uf.filename).suffix.lstrip(".").lower()
            if ext not in _COURSE_FILE_EXTS:
                continue
            raw = await uf.read()
            if len(raw) > _COURSE_FILE_MAX:
                continue
            safe_name = f"cf_{uuid.uuid4().hex}{Path(uf.filename).suffix}"
            _storage_upload(raw, safe_name, uf.content_type or "application/octet-stream")
            db.add(CourseFile(
                course_post_id=cp.id,
                course_id=course_id,
                filename=safe_name,
                original_name=uf.filename,
                file_size=len(raw),
                uploaded_by=cm.id,
                is_approved=None,
            ))

    db.commit()
    return RedirectResponse(url=f"/course/{course_id}?tab={cp.post_type}", status_code=303)


@app.post("/course/{course_id}/post/{post_id}/delete")
async def course_post_delete(
    request: Request,
    course_id: int,
    post_id: int,
    db: Session = Depends(get_db),
):
    cm = _current_member(request, db)
    if not cm:
        raise HTTPException(status_code=401)
    cp = db.query(CoursePost).filter(
        CoursePost.id == post_id, CoursePost.course_id == course_id
    ).first()
    if not cp:
        raise HTTPException(status_code=404)
    is_priv = _is_privileged(request, db)
    if cp.author_id != cm.id and not is_priv:
        raise HTTPException(status_code=403)
    tab = cp.post_type
    # 첨부 파일도 삭제
    for cf in db.query(CourseFile).filter(CourseFile.course_post_id == post_id).all():
        _storage_delete(cf.filename)
        db.delete(cf)
    db.delete(cp)
    db.commit()
    return RedirectResponse(url=f"/course/{course_id}?tab={tab}", status_code=303)


@app.get("/course/file/{file_id}/download")
async def course_file_download(request: Request, file_id: int, db: Session = Depends(get_db)):
    cf = db.query(CourseFile).filter(CourseFile.id == file_id).first()
    if not cf:
        raise HTTPException(status_code=404)
    if cf.is_approved is not True:
        cm = _current_member(request, db)
        if not cm:
            raise HTTPException(status_code=403)
        is_priv = _is_privileged(request, db)
        if not is_priv and cm.id != cf.uploaded_by:
            raise HTTPException(status_code=403, detail="관리자 승인 후 다운로드 가능합니다.")
    path = UPLOAD_DIR / cf.filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")
    from fastapi.responses import FileResponse
    return FileResponse(path, filename=cf.original_name, media_type="application/octet-stream")


@app.post("/course/{course_id}/delete")
async def course_entry_delete(
    request: Request,
    course_id: int,
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    entry = db.query(CourseEntry).filter(CourseEntry.id == course_id).first()
    if not entry:
        raise HTTPException(status_code=404)
    for cf in db.query(CourseFile).filter(CourseFile.course_id == course_id).all():
        _storage_delete(cf.filename)
        db.delete(cf)
    db.query(CoursePost).filter(CoursePost.course_id == course_id).delete()
    db.delete(entry)
    db.commit()
    return RedirectResponse(url="/course", status_code=303)


# ── 관리자: 교과목 파일 승인 ────────────────────────────────────────────────

@app.get("/admin/course-files", response_class=HTMLResponse)
async def admin_course_files(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    pending = db.query(CourseFile).filter(CourseFile.is_approved.is_(None)).order_by(CourseFile.created_at).all()
    approved = db.query(CourseFile).filter(CourseFile.is_approved.is_(True)).order_by(CourseFile.created_at.desc()).limit(50).all()
    rejected = db.query(CourseFile).filter(CourseFile.is_approved.is_(False)).order_by(CourseFile.created_at.desc()).limit(30).all()

    post_ids = {cf.course_post_id for cf in pending + approved + rejected}
    posts_map = {p.id: p for p in db.query(CoursePost).filter(CoursePost.id.in_(post_ids)).all()} if post_ids else {}
    course_ids = {cf.course_id for cf in pending + approved + rejected}
    courses_map = {e.id: e for e in db.query(CourseEntry).filter(CourseEntry.id.in_(course_ids)).all()} if course_ids else {}
    uploader_ids = {cf.uploaded_by for cf in pending + approved + rejected if cf.uploaded_by}
    uploaders_map = {m.id: m for m in db.query(Member).filter(Member.id.in_(uploader_ids)).all()} if uploader_ids else {}

    return _render(request, "admin/course_files.html", _ctx(request, db,
        pending=pending, approved=approved, rejected=rejected,
        posts_map=posts_map, courses_map=courses_map, uploaders_map=uploaders_map))


@app.post("/admin/course-files/{file_id}/approve")
async def admin_course_file_approve(request: Request, file_id: int, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    cf = db.query(CourseFile).filter(CourseFile.id == file_id).first()
    if cf:
        cm = _current_member(request, db)
        cf.is_approved = True
        cf.approved_by = cm.id if cm else None
        cf.approved_at = _now()
        db.commit()
    return RedirectResponse(url="/admin/course-files", status_code=303)


@app.post("/admin/course-files/{file_id}/reject")
async def admin_course_file_reject(
    request: Request, file_id: int,
    reason: str = Form(""),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    cf = db.query(CourseFile).filter(CourseFile.id == file_id).first()
    if cf:
        cf.is_approved = False
        cf.reject_reason = reason.strip()
        db.commit()
    return RedirectResponse(url="/admin/course-files", status_code=303)


# ════════════════════════════════════════════════════════════════════════════
#  CNU 보드 — 공개 & 관리자
# ════════════════════════════════════════════════════════════════════════════

# 백그라운드 GPT 요약 진행 상황 (메모리, 서버 재시작 시 초기화)
_cnu_jobs: dict = {}   # job_id -> {total, done, status, errors}


async def _bg_summarize_all(job_id: str, item_ids: list):
    """백그라운드에서 CNU 항목 전체 요약 — _cnu_jobs에 진행률 업데이트"""
    from database import SessionLocal
    db = SessionLocal()
    try:
        for i, iid in enumerate(item_ids):
            try:
                item = db.query(CnuItem).filter(CnuItem.id == iid).first()
                if item:
                    item.summary = await _cnu_summarize(item.title, item.link)
                    db.commit()
            except Exception:
                _cnu_jobs[job_id]["errors"] += 1
            _cnu_jobs[job_id]["done"] = i + 1
        _cnu_jobs[job_id]["status"] = "done"
    except Exception as e:
        _cnu_jobs[job_id]["status"] = "error"
        _cnu_jobs[job_id]["msg"] = str(e)
    finally:
        db.close()

# ── 공개 보드 ────────────────────────────────────────────────────────────────

@app.get("/cnu", response_class=HTMLResponse)
async def cnu_board(
    request: Request,
    tab: str = "scholarship",
    db: Session = Depends(get_db),
):
    from datetime import date as _date
    today_str = _date.today().isoformat()
    valid_tabs = ("scholarship", "job_contest", "program")
    if tab not in valid_tabs:
        tab = "scholarship"
    items = (
        db.query(CnuItem)
        .filter(CnuItem.board_type == tab)
        .order_by(CnuItem.created_at.desc())
        .all()
    )
    counts = {
        bt: db.query(CnuItem).filter(CnuItem.board_type == bt).count()
        for bt in valid_tabs
    }
    return _render(request, "cnu/board.html",
                   _ctx(request, db, tab=tab, items=items, counts=counts, today=today_str))


# ── 관리자: CNU 크롤 ─────────────────────────────────────────────────────────

@app.get("/admin/cnu", response_class=HTMLResponse)
async def admin_cnu_page(request: Request, db: Session = Depends(get_db)):
    if r := _admin_redirect(request):
        return r
    from datetime import date as _date
    today_str = _date.today().isoformat()
    all_items = (
        db.query(CnuItem)
        .order_by(CnuItem.created_at.desc())
        .limit(200)
        .all()
    )
    return _render(request, "admin/cnu.html",
                   _ctx(request, db, items=all_items, cnu_sources=CNU_SOURCES,
                        today=today_str))


@app.post("/admin/cnu/crawl")
async def admin_cnu_crawl(
    request: Request,
    sources: List[str] = Form(default=[]),
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r

    selected = [s for s in sources if s in CNU_SOURCES] or list(CNU_SOURCES.keys())
    result = await _do_crawl_cnu(sources=selected)

    items  = result.get("items", [])
    errors = result.get("errors", [])
    added  = 0

    for it in items:
        # 중복 체크 (URL 기준)
        exists = db.query(CnuItem).filter(CnuItem.link == it["link"]).first()
        if exists:
            continue
        db.add(CnuItem(
            board_type   = it["board_type"],
            source       = it["source"],
            source_label = it["source_label"],
            title        = it["title"][:500],
            link         = it["link"][:1000],
            posted_date  = it.get("posted_date", "")[:30],
            deadline     = (it.get("deadline") or "")[:30] or None,
            summary      = "",
        ))
        added += 1

    db.commit()
    msg = f"크롤 완료 — 신규 {added}건 추가"
    if errors:
        msg += f" / 오류 {len(errors)}건"
    return RedirectResponse(url=f"/admin/cnu?msg={msg}", status_code=303)


@app.post("/admin/cnu/{item_id}/summarize")
async def admin_cnu_summarize(
    request: Request,
    item_id: int,
    db: Session = Depends(get_db),
):
    """GPT로 항목 요약 생성"""
    if r := _admin_redirect(request):
        return r
    item = db.query(CnuItem).filter(CnuItem.id == item_id).first()
    if item:
        item.summary = await _cnu_summarize(item.title, item.link)
        db.commit()
    return RedirectResponse(url="/admin/cnu", status_code=303)


@app.post("/admin/cnu/summarize-all")
async def admin_cnu_summarize_all(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """요약 없는 항목 전부 GPT 요약 — 백그라운드로 실행, job_id 반환"""
    if r := _admin_redirect(request):
        return r
    pending = db.query(CnuItem).filter(
        (CnuItem.summary == None) | (CnuItem.summary == "")
    ).all()
    if not pending:
        return JSONResponse({"job_id": None, "total": 0, "msg": "요약할 항목이 없습니다"})
    job_id = uuid.uuid4().hex[:10]
    _cnu_jobs[job_id] = {"total": len(pending), "done": 0, "status": "running", "errors": 0}
    background_tasks.add_task(_bg_summarize_all, job_id, [it.id for it in pending])
    return JSONResponse({"job_id": job_id, "total": len(pending)})


@app.get("/admin/cnu/job/{job_id}")
async def admin_cnu_job_status(request: Request, job_id: str):
    """백그라운드 요약 진행 상황 폴링 엔드포인트"""
    if r := _admin_redirect(request):
        return r
    job = _cnu_jobs.get(job_id)
    if not job:
        return JSONResponse({"status": "not_found"})
    return JSONResponse(job)


@app.post("/admin/cnu/{item_id}/delete")
async def admin_cnu_delete(
    request: Request,
    item_id: int,
    db: Session = Depends(get_db),
):
    if r := _admin_redirect(request):
        return r
    item = db.query(CnuItem).filter(CnuItem.id == item_id).first()
    if item:
        db.delete(item)
        db.commit()
    return RedirectResponse(url="/admin/cnu", status_code=303)


@app.post("/admin/cnu/purge-expired")
async def admin_cnu_purge_expired(
    request: Request,
    db: Session = Depends(get_db),
):
    """마감 지난 항목 일괄 삭제"""
    if r := _admin_redirect(request):
        return r
    from datetime import date as _date
    today = _date.today().isoformat()
    deleted = db.query(CnuItem).filter(
        CnuItem.deadline != None,
        CnuItem.deadline < today,
    ).delete()
    db.commit()
    return RedirectResponse(url=f"/admin/cnu?msg=만료 항목 {deleted}건 삭제", status_code=303)
