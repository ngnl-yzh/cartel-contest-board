import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./competitions.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def _add_col(conn, inspector, table: str, col: str, col_def: str):
    cols = [c["name"] for c in inspector.get_columns(table)]
    if col not in cols:
        from sqlalchemy import text
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_def}"))


def init_db():
    Base.metadata.create_all(bind=engine)

    from sqlalchemy import inspect, text as _t
    with engine.connect() as conn:
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        if "competitions" in tables:
            _add_col(conn, inspector, "competitions", "image",             "image VARCHAR(500)")
            _add_col(conn, inspector, "competitions", "max_members",       "max_members INTEGER")
            _add_col(conn, inspector, "competitions", "submitted",         "submitted BOOLEAN DEFAULT FALSE")
            _add_col(conn, inspector, "competitions", "submitted_at",      "submitted_at TIMESTAMP")
            _add_col(conn, inspector, "competitions", "start_date",        "start_date DATE")
            _add_col(conn, inspector, "competitions", "announcement_date", "announcement_date DATE")
            _add_col(conn, inspector, "competitions", "review_1_date",     "review_1_date DATE")
            _add_col(conn, inspector, "competitions", "review_2_date",     "review_2_date DATE")
            _add_col(conn, inspector, "competitions", "award_date",        "award_date DATE")
            _add_col(conn, inspector, "competitions", "review_dates",      "review_dates TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "competitions", "is_featured",       "is_featured BOOLEAN DEFAULT FALSE")
            _add_col(conn, inspector, "competitions", "is_active",         "is_active BOOLEAN DEFAULT 1")
            _add_col(conn, inspector, "competitions", "view_count",        "view_count INTEGER DEFAULT 0")
            _add_col(conn, inspector, "competitions", "submission_docs",   "submission_docs TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "competitions", "stage_override",    "stage_override VARCHAR(20)")

        if "members" in tables:
            _add_col(conn, inspector, "members", "comment_muted_until", "comment_muted_until TIMESTAMP")
            _add_col(conn, inspector, "members", "intro_text",   "intro_text TEXT DEFAULT ''")
            _add_col(conn, inspector, "members", "skills",       "skills TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "members", "links",        "links TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "members", "generation",   "generation INTEGER")
            _add_col(conn, inspector, "members", "permissions",  "permissions TEXT DEFAULT '[]'")

        if "invite_codes" in tables:
            _add_col(conn, inspector, "invite_codes", "code_type", "code_type VARCHAR(20) DEFAULT 'personal'")
            _add_col(conn, inspector, "invite_codes", "max_uses", "max_uses INTEGER")
            _add_col(conn, inspector, "invite_codes", "use_count", "use_count INTEGER DEFAULT 0")
            _add_col(conn, inspector, "invite_codes", "is_active", "is_active BOOLEAN DEFAULT TRUE")
            _add_col(conn, inspector, "invite_codes", "generation", "generation INTEGER")

        if "chat_room_members" in tables:
            _add_col(conn, inspector, "chat_room_members", "role", "role VARCHAR(20) DEFAULT 'member'")
            _add_col(conn, inspector, "chat_room_members", "muted_until", "muted_until TIMESTAMP")

        if "teams" in tables:
            _add_col(conn, inspector, "teams", "requirements",             "requirements TEXT DEFAULT ''")
            _add_col(conn, inspector, "teams", "submitted_docs",           "submitted_docs TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "teams", "submission_files",         "submission_files TEXT DEFAULT '[]'")
            _add_col(conn, inspector, "teams", "dissolution_requested",    "dissolution_requested BOOLEAN DEFAULT FALSE")
            _add_col(conn, inspector, "teams", "dissolution_requested_at", "dissolution_requested_at TIMESTAMP")
            _add_col(conn, inspector, "teams", "dissolution_votes",        "dissolution_votes TEXT DEFAULT '[]'")

        if "team_members" in tables:
            _add_col(conn, inspector, "team_members", "is_participant", "is_participant BOOLEAN DEFAULT FALSE")
            _add_col(conn, inspector, "team_members", "member_id",      "member_id INTEGER")
            _add_col(conn, inspector, "team_members", "team_id",        "team_id INTEGER")
            _add_col(conn, inspector, "team_members", "award_rank",     "award_rank VARCHAR(50)")
            _add_col(conn, inspector, "team_members", "award_prize",    "award_prize VARCHAR(300) DEFAULT ''")
            _add_col(conn, inspector, "team_members", "award_note",     "award_note TEXT DEFAULT ''")
            # 2025-05 — 팀원 신청 승인 시스템
            _add_col(conn, inspector, "team_members", "real_name",  "real_name VARCHAR(100) DEFAULT ''")
            _add_col(conn, inspector, "team_members", "student_id", "student_id VARCHAR(50) DEFAULT ''")
            _add_col(conn, inspector, "team_members", "status",     "status VARCHAR(20) DEFAULT 'approved'")

            # 2025-05 — password_hash를 nullable로 변경 (팀원은 비밀번호 없음)
            # PostgreSQL: ALTER COLUMN ... DROP NOT NULL
            # SQLite: NOT NULL 변경 불가이므로 스킵 (새 DB는 nullable로 생성됨)
            if not DATABASE_URL.startswith("sqlite"):
                try:
                    col_info = next(
                        (c for c in inspector.get_columns("team_members") if c["name"] == "password_hash"),
                        None,
                    )
                    if col_info and not col_info.get("nullable", True):
                        conn.execute(_t(
                            "ALTER TABLE team_members ALTER COLUMN password_hash DROP NOT NULL"
                        ))
                except Exception:
                    pass  # 이미 nullable이거나 지원하지 않는 DB

        # team_competition_entries — 신규 테이블이므로 create_all로 생성됨
        # job_postings — 신규 테이블이므로 create_all로 생성됨 (추가 컬럼 없음)

        if "gallery_posts" in tables:
            _add_col(conn, inspector, "gallery_posts", "is_easter",   "is_easter BOOLEAN DEFAULT FALSE")
            _add_col(conn, inspector, "gallery_posts", "sort_order",  "sort_order INTEGER DEFAULT 0")

        if "chat_rooms" in tables and "chat_room_members" in tables:
            rooms = conn.execute(_t(
                "SELECT id, created_by_id FROM chat_rooms WHERE created_by_id IS NOT NULL"
            )).fetchall()
            for room_id, owner_id in rooms:
                existing = conn.execute(_t(
                    "SELECT id FROM chat_room_members WHERE room_id = :rid AND member_id = :mid LIMIT 1"
                ), {"rid": room_id, "mid": owner_id}).fetchone()
                if not existing:
                    conn.execute(_t(
                        "INSERT INTO chat_room_members (room_id, member_id, role, joined_at) "
                        "VALUES (:rid, :mid, 'owner', CURRENT_TIMESTAMP)"
                    ), {"rid": room_id, "mid": owner_id})

        # 기존 TeamMember(team_id=NULL) 데이터를 위해 기본 팀 생성
        rows = conn.execute(_t(
            "SELECT DISTINCT competition_id FROM team_members WHERE team_id IS NULL AND competition_id IS NOT NULL"
        )).fetchall()
        for (cid,) in rows:
            existing = conn.execute(_t(
                "SELECT id FROM teams WHERE competition_id = :cid LIMIT 1"
            ), {"cid": cid}).fetchone()
            if existing:
                tid = existing[0]
            else:
                conn.execute(_t(
                    "INSERT INTO teams (competition_id, name, description, submitted, created_at) "
                    "VALUES (:cid, '기본 팀', '', FALSE, CURRENT_TIMESTAMP)"
                ), {"cid": cid})
                tid = conn.execute(_t(
                    "SELECT id FROM teams WHERE competition_id = :cid ORDER BY id DESC LIMIT 1"
                ), {"cid": cid}).fetchone()[0]
            conn.execute(_t(
                "UPDATE team_members SET team_id = :tid WHERE competition_id = :cid AND team_id IS NULL"
            ), {"tid": tid, "cid": cid})

        conn.commit()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
