from datetime import datetime, timedelta, timezone

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Text, UniqueConstraint, and_, or_
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# KST 기본값 함수 — SQLAlchemy default= 에 사용
_KST = timezone(timedelta(hours=9))
def _kst_now():
    """현재 KST 시각을 naive datetime으로 반환"""
    return datetime.now(_KST).replace(tzinfo=None)

BOARDS = {
    "free":    "자유게시판",
    "social":  "활동(친목)",
    "project": "활동(프로젝트)",
}


class Competition(Base):
    __tablename__ = "competitions"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(300), nullable=False)
    organizer = Column(String(200), default="")
    tags = Column(String(500), default="[]")
    start_date = Column(Date, nullable=True)
    deadline = Column(Date, nullable=False)
    announcement_date = Column(Date, nullable=True)
    review_1_date = Column(Date, nullable=True)      # 1차 심사일 (legacy)
    review_2_date = Column(Date, nullable=True)      # 2차 심사일 (legacy)
    review_dates  = Column(Text, default="[]")       # JSON: [{"label":"1차 심사","date":"YYYY-MM-DD"}, ...]
    award_date    = Column(Date, nullable=True)      # 시상일
    prize = Column(String(500), default="")
    link = Column(String(1000), default="")
    description = Column(Text, default="")
    files = Column(Text, default="[]")
    view_count = Column(Integer, default=0)
    image = Column(String(500), nullable=True)
    max_members = Column(Integer, nullable=True)
    is_featured = Column(Boolean, default=False)
    is_active   = Column(Boolean, default=True)   # False = 비활성(숨김); 자동 추가 시 False로 시작
    submitted = Column(Boolean, default=False)
    submitted_at = Column(DateTime, nullable=True)
    submission_docs = Column(Text, default="[]")   # JSON: ["활동계획서","기획서",...] 제출 서류 목록
    stage_override  = Column(String(20), nullable=True)  # 수동 단계 지정: 접수중/심사중/발표준비중/마감
    created_at = Column(DateTime, default=_kst_now)


class Team(Base):
    __tablename__ = "teams"
    id             = Column(Integer, primary_key=True, index=True)
    competition_id = Column(Integer, nullable=False, index=True)
    name           = Column(String(100), nullable=False)
    description    = Column(String(300), default="")
    requirements   = Column(Text, default="")   # 팀 참여 요건 (연락처, 지원 조건 등)
    submitted      = Column(Boolean, default=False)
    submitted_at   = Column(DateTime, nullable=True)
    submitted_docs   = Column(Text, default="[]")   # JSON: 실제 제출한 서류 체크 목록
    submission_files = Column(Text, default="[]")   # JSON: 업로드된 제출 파일 목록
    # 팀 해체 투표
    dissolution_requested    = Column(Boolean, default=False)
    dissolution_requested_at = Column(DateTime, nullable=True)
    dissolution_votes        = Column(Text, default="[]")  # JSON: 동의한 member_id 목록
    created_at       = Column(DateTime, default=_kst_now)


class TeamMember(Base):
    __tablename__ = "team_members"

    id             = Column(Integer, primary_key=True, index=True)
    team_id        = Column(Integer, nullable=True, index=True)
    competition_id = Column(Integer, nullable=True, index=True)
    nickname       = Column(String(100), nullable=False)   # 표시명(닉네임)
    real_name      = Column(String(100), default="")       # 본명 (신청 시 필수)
    student_id     = Column(String(50),  default="")       # 학번 (신청 시 필수)
    password_hash  = Column(String(300), nullable=True)    # 팀장만 사용 (nullable)
    role           = Column(String(50),  default="기타")
    memo           = Column(String(500), default="")
    is_leader      = Column(Boolean, default=False)
    is_participant = Column(Boolean, default=False)
    # pending = 승인 대기 / approved = 승인 완료 / rejected = 거절
    status         = Column(String(20),  default="approved")
    member_id      = Column(Integer, nullable=True)
    # ── 수상 정보 ──────────────────────────────────────────────────
    award_rank  = Column(String(50),  nullable=True)   # 대상/최우수상/우수상/장려상/입선
    award_prize = Column(String(300), default="")      # 상금·부상 내용
    award_note  = Column(Text,        default="")      # 수상 관련 메모
    created_at  = Column(DateTime, default=_kst_now)


class Member(Base):
    __tablename__ = "members"

    id = Column(Integer, primary_key=True, index=True)
    activity_name = Column(String(100), unique=True, nullable=False, index=True)
    real_name = Column(String(100), nullable=False)
    student_id = Column(String(50), default="")
    phone = Column(String(50), default="")
    password_hash = Column(String(300), nullable=False)
    bio = Column(Text, default="")
    profile_image = Column(String(500), nullable=True)
    role = Column(String(20), default="member")   # member / sub_admin
    invite_code_used = Column(String(100), nullable=True)
    intro_text = Column(Text, default="")         # 긴 자기소개
    skills     = Column(Text, default="[]")       # JSON: [{"skill":"Python","category":"개발"}]
    links      = Column(Text, default="[]")       # JSON: [{"label":"GitHub","url":"https://..."}]
    comment_muted_until = Column(DateTime, nullable=True)
    generation   = Column(Integer, nullable=True)  # 기수 (1기, 2기, ...)
    permissions  = Column(Text, default="[]")      # JSON: 중간관리자 부여 권한 목록
    birthday     = Column(String(5),  nullable=True)   # "MM-DD" 형식 생일
    is_graduated = Column(Boolean, default=False)      # 졸업 여부 (True면 캘린더에서 흐리게)
    show_birthday= Column(Boolean, default=True)       # 생일 공개 여부 (멤버 페이지·프로필)
    show_participation_history = Column(Boolean, default=True)  # 공개 프로필에 참여 내역 표시 여부
    # ── 설정 ───────────────────────────────────────────────────────────────
    follow_auto_approve = Column(Boolean, default=True)        # 팔로우 자동 승인 여부
    dm_allowed_from     = Column(String(20), default="all")    # DM 수신: all / followers / none
    notif_settings      = Column(Text, default="{}")           # JSON: 알림 ON/OFF 설정
    created_at   = Column(DateTime, default=_kst_now)


class InviteCode(Base):
    __tablename__ = "invite_codes"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(100), unique=True, nullable=False, index=True)
    note = Column(String(200), default="")
    code_type = Column(String(20), default="personal")  # personal / group
    max_uses = Column(Integer, nullable=True)
    use_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_kst_now)
    expires_at = Column(DateTime, nullable=True)
    used_by_member_id = Column(Integer, nullable=True)
    generation = Column(Integer, nullable=True)  # 이 코드로 가입 시 자동 배정 기수


class InviteCodeUseLog(Base):
    __tablename__ = "invite_code_use_logs"

    id = Column(Integer, primary_key=True, index=True)
    invite_code_id = Column(Integer, nullable=False, index=True)
    member_id = Column(Integer, nullable=True, index=True)
    activity_name = Column(String(100), default="")
    real_name = Column(String(100), default="")
    used_at = Column(DateTime, default=_kst_now)
    revoked_at = Column(DateTime, nullable=True)
    revoked_by = Column(String(100), default="")


# ── 게시판 ────────────────────────────────────────────────────────────────────

class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    board = Column(String(20), nullable=False, index=True)   # free / social / project
    title = Column(String(300), nullable=False)
    content = Column(Text, default="")
    author_id = Column(Integer, nullable=False, index=True)
    images = Column(Text, default="[]")              # JSON list of filenames
    view_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=_kst_now)
    updated_at = Column(DateTime, default=_kst_now)


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True)
    post_id = Column(Integer, nullable=False, index=True)
    parent_id = Column(Integer, nullable=True)       # None = 최상위 댓글
    author_id = Column(Integer, nullable=False, index=True)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=_kst_now)


class PostLike(Base):
    __tablename__ = "post_likes"
    __table_args__ = (UniqueConstraint("post_id", "member_id", name="uq_post_like"),)

    id = Column(Integer, primary_key=True, index=True)
    post_id = Column(Integer, nullable=False, index=True)
    member_id = Column(Integer, nullable=False, index=True)


class CommentLike(Base):
    __tablename__ = "comment_likes"
    __table_args__ = (UniqueConstraint("comment_id", "member_id", name="uq_comment_like"),)

    id = Column(Integer, primary_key=True, index=True)
    comment_id = Column(Integer, nullable=False, index=True)
    member_id = Column(Integer, nullable=False, index=True)


# ── 채팅 ──────────────────────────────────────────────────────────────────────

class ChatRoom(Base):
    __tablename__ = "chat_rooms"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(String(300), default="")
    password_hash = Column(String(300), nullable=True)   # None = 공개방
    created_by_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=_kst_now)


class ChatRoomMember(Base):
    __tablename__ = "chat_room_members"
    __table_args__ = (UniqueConstraint("room_id", "member_id", name="uq_chat_room_member"),)

    id = Column(Integer, primary_key=True, index=True)
    room_id = Column(Integer, nullable=False, index=True)
    member_id = Column(Integer, nullable=False, index=True)
    role = Column(String(20), default="member")  # owner / co_owner / member
    muted_until = Column(DateTime, nullable=True)
    joined_at = Column(DateTime, default=_kst_now)


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, index=True)
    room_id = Column(Integer, nullable=False, index=True)
    author_id = Column(Integer, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=_kst_now)


class TeamResult(Base):
    """팀별 단계 결과 (1차심사/2차심사/발표/시상)"""
    __tablename__ = "team_results"
    __table_args__ = (UniqueConstraint("team_id", "stage", name="uq_team_stage"),)

    id             = Column(Integer, primary_key=True, index=True)
    team_id        = Column(Integer, nullable=False, index=True)
    competition_id = Column(Integer, nullable=False, index=True)
    stage          = Column(String(30), nullable=False)   # review_1/review_2/announcement/award
    passed         = Column(Boolean, nullable=True)       # True=통과, False=탈락, None=미정
    note           = Column(Text, default="")
    recorded_at    = Column(DateTime, default=_kst_now)
    recorded_by_id = Column(Integer, nullable=True)       # 기록한 팀장 member_id


class CompetitionScrap(Base):
    """회원별 공모전 스크랩"""
    __tablename__ = "competition_scraps"
    __table_args__ = (UniqueConstraint("competition_id", "member_id", name="uq_comp_scrap"),)

    id             = Column(Integer, primary_key=True, index=True)
    competition_id = Column(Integer, nullable=False, index=True)
    member_id      = Column(Integer, nullable=False, index=True)
    scrapped_at    = Column(DateTime, default=_kst_now)


class Follow(Base):
    """팔로우 관계 (승인 필요)"""
    __tablename__ = "follows"
    __table_args__ = (UniqueConstraint("follower_id", "following_id", name="uq_follow"),)

    id           = Column(Integer, primary_key=True, index=True)
    follower_id  = Column(Integer, nullable=False, index=True)   # 팔로우 신청자
    following_id = Column(Integer, nullable=False, index=True)   # 대상
    status       = Column(String(20), default="pending")         # pending / approved
    created_at   = Column(DateTime, default=_kst_now)
    approved_at  = Column(DateTime, nullable=True)


class DirectMessage(Base):
    """1:1 DM"""
    __tablename__ = "direct_messages"

    id          = Column(Integer, primary_key=True, index=True)
    thread_key  = Column(String(50), nullable=False, index=True)  # f"{min(a,b)}:{max(a,b)}"
    sender_id   = Column(Integer, nullable=False, index=True)
    receiver_id = Column(Integer, nullable=False, index=True)
    content     = Column(Text, nullable=False)
    is_read     = Column(Boolean, default=False)
    created_at  = Column(DateTime, default=_kst_now)


class Notification(Base):
    """알림"""
    __tablename__ = "notifications"

    id         = Column(Integer, primary_key=True, index=True)
    member_id  = Column(Integer, nullable=False, index=True)  # 수신자
    type       = Column(String(50))   # follow_request / follow_approved / team_recruit
    actor_id   = Column(Integer, nullable=True)
    ref_id     = Column(Integer, nullable=True)
    message    = Column(String(300))
    is_read    = Column(Boolean, default=False)
    created_at = Column(DateTime, default=_kst_now)


class AppSetting(Base):
    """앱 전역 설정 (key-value)"""
    __tablename__ = "app_settings"

    id         = Column(Integer, primary_key=True, index=True)
    key        = Column(String(100), unique=True, nullable=False, index=True)
    value      = Column(Text, default="")
    updated_at = Column(DateTime, default=_kst_now)


class TeamCompetitionEntry(Base):
    """팀 자기 기재 공모전 실적 (결과 + 증빙)"""
    __tablename__ = "team_competition_entries"
    __table_args__ = (UniqueConstraint("team_id", "competition_id", name="uq_team_comp_entry"),)

    id             = Column(Integer, primary_key=True, index=True)
    team_id        = Column(Integer, nullable=False, index=True)
    competition_id = Column(Integer, nullable=False, index=True)

    # 단계별 결과 JSON: [{"label":"1차 서류","passed":true,"note":""}, ...]
    stage_results  = Column(Text, default="[]")

    # 최종 수상
    is_awarded     = Column(Boolean, default=False)
    award_name     = Column(String(100), default="")    # 최우수상, 우수상 등
    prize_amount   = Column(String(100), default="")    # 선택 입력

    # 증빙 사진 (관리자 승인 후 공개)
    proof_image          = Column(String(500), nullable=True)
    proof_approved       = Column(Boolean, default=False)
    proof_approved_at    = Column(DateTime, nullable=True)
    proof_approved_by    = Column(Integer, nullable=True)
    proof_rejected_reason = Column(String(300), default="")

    # 공개 여부 (팀장이 토글)
    is_public      = Column(Boolean, default=False)

    # 메모
    note           = Column(Text, default="")

    recorded_by_id = Column(Integer, nullable=True)     # 기록한 팀장 member_id
    created_at     = Column(DateTime, default=_kst_now)
    updated_at     = Column(DateTime, default=_kst_now)


class GalleryPost(Base):
    """갤러리 — 행사 사진 기록"""
    __tablename__ = "gallery_posts"

    id          = Column(Integer, primary_key=True, index=True)
    title       = Column(String(200), nullable=False)
    description = Column(Text, default="")
    event_type  = Column(String(50), default="기타")   # MT / 개강총회 / 종강파티 / 수상 / 기타
    event_date  = Column(Date, nullable=True)
    images      = Column(Text, default="[]")            # JSON list of filenames
    created_by_id = Column(Integer, nullable=False)
    is_public   = Column(Boolean, default=True)
    is_easter          = Column(Boolean, default=False)   # 이스터에그 갤러리 여부
    sort_order         = Column(Integer, default=0, nullable=False, server_default="0")
    show_on_calendar   = Column(Boolean, default=True)    # 캘린더 날짜에 표시 여부
    created_at         = Column(DateTime, default=_kst_now)


class CalendarEvent(Base):
    """캘린더 행사 일정"""
    __tablename__ = "calendar_events"

    id          = Column(Integer, primary_key=True, index=True)
    title       = Column(String(200), nullable=False)
    description = Column(Text, default="")
    event_type  = Column(String(30), default="기타")  # 정기모임 / 공모전 / 행사 / MT / 기타
    start_date  = Column(Date, nullable=False)
    end_date    = Column(Date, nullable=True)          # None이면 하루짜리
    created_by_id = Column(Integer, nullable=True)
    created_at  = Column(DateTime, default=_kst_now)


class PushSubscription(Base):
    """웹 푸시 구독 정보 (PWA 알림용)"""
    __tablename__ = "push_subscriptions"

    id         = Column(Integer, primary_key=True, index=True)
    member_id  = Column(Integer, nullable=True)          # 로그인한 회원 ID (없으면 None)
    endpoint   = Column(Text, nullable=False, unique=True)
    p256dh     = Column(Text, nullable=False)
    auth       = Column(Text, nullable=False)
    created_at = Column(DateTime, default=_kst_now)


class TeamKickRequest(Base):
    """팀장이 접수 이후 팀원 강퇴 요청 — 관리자 승인 필요"""
    __tablename__ = "team_kick_requests"

    id              = Column(Integer, primary_key=True, index=True)
    team_id         = Column(Integer, nullable=False, index=True)
    competition_id  = Column(Integer, nullable=False, index=True)
    team_member_id  = Column(Integer, nullable=False)   # TeamMember.id
    requested_by_id = Column(Integer, nullable=False)   # Member.id (팀장)
    reason          = Column(Text, default="")
    created_at      = Column(DateTime, default=_kst_now)


class ExternalAchievement(Base):
    """자기 기재 외부 이력 (증빙 없음)"""
    __tablename__ = "external_achievements"

    id             = Column(Integer, primary_key=True, index=True)
    member_id      = Column(Integer, nullable=False, index=True)
    title          = Column(String(200), nullable=False)
    organizer      = Column(String(100), default="")
    result         = Column(String(100), default="")   # 수상/참가/장려상 등
    achieved_year  = Column(Integer, nullable=True)
    note           = Column(Text, default="")
    created_at     = Column(DateTime, default=_kst_now)


class JobPosting(Base):
    """취업/인턴/대외활동 공고"""
    __tablename__ = "job_postings"

    id           = Column(Integer, primary_key=True, index=True)
    title        = Column(String(500), default="")
    company      = Column(String(200), default="")
    job_type     = Column(String(50), default="")   # 인턴/채용/서포터즈/대외활동
    location     = Column(String(200), default="")
    deadline     = Column(Date, nullable=True)
    link         = Column(String(1000), default="")
    source       = Column(String(50), default="")
    source_label = Column(String(100), default="")
    view_count   = Column(Integer, default=0)
    created_at   = Column(DateTime, default=_kst_now)


class CrawlSession(Base):
    """크롤링 세션 이력 (날짜별 영속 보관)"""
    __tablename__ = "crawl_sessions"

    id           = Column(Integer, primary_key=True, index=True)
    sources      = Column(Text, default="[]")    # JSON: ["contestkorea", "wevity", ...]
    items        = Column(Text, default="[]")    # JSON: [{source, title, link, ...}, ...]
    errors       = Column(Text, default="[]")    # JSON: ["오류 메시지", ...]
    counts       = Column(Text, default="{}")    # JSON: {"사이트": n}
    item_count   = Column(Integer, default=0)
    skipped_count = Column(Integer, default=0)   # 분야 필터로 제외된 수
    crawled_at   = Column(DateTime, default=_kst_now)


class JobCrawlSession(Base):
    """취업 크롤링 세션 이력 (날짜별 영속 보관)"""
    __tablename__ = "job_crawl_sessions"

    id          = Column(Integer, primary_key=True, index=True)
    sources     = Column(Text, default="[]")   # JSON: ["linkareer", "saramin"]
    items       = Column(Text, default="[]")   # JSON: [{source, title, company, link, ...}]
    errors      = Column(Text, default="[]")   # JSON: ["오류 메시지"]
    counts      = Column(Text, default="{}")   # JSON: {"링커리어": n}
    item_count  = Column(Integer, default=0)
    crawled_at  = Column(DateTime, default=_kst_now)


class PersonalPost(Base):
    """팀원 개인 갤러리 게시물 (인스타그램 스타일)"""
    __tablename__ = "personal_posts"

    id         = Column(Integer, primary_key=True, index=True)
    member_id  = Column(Integer, nullable=False, index=True)
    caption    = Column(Text, default="")            # 게시물 설명
    images     = Column(Text, default="[]")          # JSON: ["파일명1.jpg", ...]
    is_public  = Column(Boolean, default=True)       # False = 본인만 볼 수 있음
    created_at = Column(DateTime, default=_kst_now)
