"""报送后台本地 SQLite 数据层（多控制台共享版）。

设计要点：
- 两个控制台共享同一 SQLite（WAL）与同一上传目录；
- 建批是"草稿 → 发布"两阶段：批次先以 CREATING 入库（不可查询、不可报送），
  原件完整落盘、全部行一次事务入库、发布前校验通过后，才转为 STOPPED
  （可查询、可报送）。写入失败或进程退出只会留下 CREATING 残骸，永远不会
  变成"可报送的半批"；
- 控制台以心跳登记存活性；清理（启动时 + 后台循环）只回收"死控制台"的
  未发布草稿与其原件，绝不动仍在心跳的控制台正在上传的文件；
- 文件删除由调用方在不持任何数据库事务的空档完成；本模块所有写入都是
  短事务，已有批次在上传/清理期间照常可查询、可报送；
- worker_lock 记录所属控制台：只有锁属主死掉，锁才会被清理并恢复现场
  （SAVING→UNVERIFIED，RUNNING→STOPPED），不会误伤另一控制台的在跑任务。
"""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# 行状态
PENDING = "PENDING"                # 待报送
QUERY_FAILED = "QUERY_FAILED"      # 查询失败（绝不能当作"不存在"）
CONFLICT = "CONFLICT"              # 远端已存在异值记录，人工处理，批次停止
UNVERIFIED = "UNVERIFIED"          # 保存后回执丢失/进程退出，待核实
ALREADY_SENT = "ALREADY_SENT"      # 远端已存在同值记录，视为已送
SAVED = "SAVED"                    # 本次确认新增成功，拿到远端记录编号
SAVING = "SAVING"                  # 正在提交（仅瞬时存在，崩溃后被恢复为 UNVERIFIED）

ROW_STATUS_LABELS = {
    PENDING: "待报送",
    QUERY_FAILED: "查询失败",
    CONFLICT: "异值冲突·停批",
    UNVERIFIED: "待核实",
    ALREADY_SENT: "已送（远端同值）",
    SAVED: "已新增",
    SAVING: "提交中",
}

# 批次状态
BATCH_CREATING = "CREATING"   # 建批中（未发布）：不可查询、不可报送
BATCH_RUNNING = "RUNNING"
BATCH_STOPPED = "STOPPED"
BATCH_DONE = "DONE"

# 续跑时需要处理的行（终态 SAVED / ALREADY_SENT / CONFLICT 不再动）
ACTIONABLE = (PENDING, QUERY_FAILED, UNVERIFIED)

# 控制台心跳超过该秒数未更新 => 视为已退出（其未发布产物才可被回收）
CONSOLE_TTL_SECONDS = 15

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,                 -- 用户上传时的原始文件名
    stored_name TEXT NOT NULL DEFAULT '',   -- 去重后的原件文件名（上传目录内）
    sha256 TEXT NOT NULL DEFAULT '',        -- 原件摘要：批次行与原件对应的锚点
    row_count INTEGER NOT NULL DEFAULT 0,   -- 发布前校验用：应入库的行数
    owner_id TEXT NOT NULL DEFAULT '',      -- 建批控制台实例 id（发布后清空）
    created_by TEXT NOT NULL DEFAULT '',    -- 建批控制台名称（展示用，保留）
    status TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rows(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    seq INTEGER NOT NULL,
    sample_no TEXT NOT NULL,
    project TEXT NOT NULL,
    result TEXT NOT NULL,
    status TEXT NOT NULL,
    remote_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    screenshot TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(batch_id, seq)
);
CREATE TABLE IF NOT EXISTS row_events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id INTEGER NOT NULL REFERENCES rows(id),
    event TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS worker_lock(
    id INTEGER PRIMARY KEY CHECK (id = 1),
    worker_id TEXT NOT NULL,
    batch_id INTEGER NOT NULL,
    owner_id TEXT NOT NULL DEFAULT '',
    acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS consoles(
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);
"""

# 旧库就地升级（CREATE TABLE IF NOT EXISTS 不会给已有表补列）
_MIGRATIONS = (
    ("batches", "stored_name",
     "ALTER TABLE batches ADD COLUMN stored_name TEXT NOT NULL DEFAULT ''"),
    ("batches", "sha256",
     "ALTER TABLE batches ADD COLUMN sha256 TEXT NOT NULL DEFAULT ''"),
    ("batches", "row_count",
     "ALTER TABLE batches ADD COLUMN row_count INTEGER NOT NULL DEFAULT 0"),
    ("batches", "owner_id",
     "ALTER TABLE batches ADD COLUMN owner_id TEXT NOT NULL DEFAULT ''"),
    ("batches", "created_by",
     "ALTER TABLE batches ADD COLUMN created_by TEXT NOT NULL DEFAULT ''"),
    ("worker_lock", "owner_id",
     "ALTER TABLE worker_lock ADD COLUMN owner_id TEXT NOT NULL DEFAULT ''"),
)


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        for table, column, ddl in _MIGRATIONS:
            cols = {r["name"] for r in conn.execute(
                f"PRAGMA table_info({table})")}
            if column not in cols:
                conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ---------- 控制台存活登记 ----------

def register_console(conn: sqlite3.Connection, console_id: str,
                     name: str) -> None:
    ts = now()
    with conn:
        conn.execute(
            "INSERT INTO consoles(id, name, started_at, heartbeat_at) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name, "
            "heartbeat_at=excluded.heartbeat_at",
            (console_id, name, ts, ts),
        )


def heartbeat_console(conn: sqlite3.Connection, console_id: str) -> None:
    with conn:
        conn.execute(
            "UPDATE consoles SET heartbeat_at=? WHERE id=?",
            (now(), console_id),
        )


def retire_console(conn: sqlite3.Connection, console_id: str) -> None:
    """进程正常退出时注销自己（异常退出由心跳超时兜底）。"""
    with conn:
        conn.execute("DELETE FROM consoles WHERE id=?", (console_id,))


def _is_live(heartbeat_at: str, ttl_seconds: int) -> bool:
    try:
        ts = datetime.fromisoformat(heartbeat_at).timestamp()
    except ValueError:
        return False
    return ts >= datetime.now().timestamp() - ttl_seconds


def live_console_ids(conn: sqlite3.Connection,
                     ttl_seconds: int = CONSOLE_TTL_SECONDS) -> set[str]:
    return {
        r["id"]
        for r in conn.execute("SELECT id, heartbeat_at FROM consoles")
        if _is_live(r["heartbeat_at"], ttl_seconds)
    }


def reap_same_name_consoles(conn: sqlite3.Connection, name: str,
                            keep_id: str) -> list[str]:
    """启动时回收同名旧实例。

    控制台名称在部署中唯一（一个名称只跑一个容器/进程），所以新实例启动时，
    同名不同 id 的登记必然来自已经退出的旧实例，可立即回收，不必等心跳超时。
    """
    ids = [r["id"] for r in conn.execute(
        "SELECT id FROM consoles WHERE name=? AND id!=?", (name, keep_id))]
    if ids:
        marks = ",".join("?" * len(ids))
        with conn:
            conn.execute(f"DELETE FROM consoles WHERE id IN ({marks})", ids)
    return ids


# ---------- 建批：草稿 → 发布 ----------

def create_batch_draft(conn: sqlite3.Connection, filename: str,
                       stored_name: str, sha256: str, owner_id: str,
                       created_by: str, row_count: int) -> int:
    """登记建批草稿（CREATING）：不可查询、不可报送，但清理逻辑据此认识原件。"""
    ts = now()
    with conn:
        cur = conn.execute(
            "INSERT INTO batches(filename, stored_name, sha256, row_count, "
            "owner_id, created_by, status, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (filename, stored_name, sha256, row_count, owner_id, created_by,
             BATCH_CREATING, ts, ts),
        )
        return cur.lastrowid


def insert_batch_rows(conn: sqlite3.Connection, batch_id: int,
                      rows: list[tuple[int, str, str, str]]) -> None:
    """全部行一次短事务入库；rows 元素为 (seq, sample_no, project, result)。"""
    ts = now()
    with conn:
        conn.executemany(
            "INSERT INTO rows(batch_id, seq, sample_no, project, result, "
            "status, updated_at) VALUES(?,?,?,?,?,?,?)",
            [(batch_id, seq, sample_no, project, result, PENDING, ts)
             for seq, sample_no, project, result in rows],
        )


class PublishError(RuntimeError):
    pass


def publish_batch(conn: sqlite3.Connection, batch_id: int) -> None:
    """发布批次：校验全部行已完整入库后，才转为可查询/可报送的 STOPPED。"""
    with conn:
        batch = conn.execute(
            "SELECT id, row_count, status FROM batches WHERE id=?",
            (batch_id,),
        ).fetchone()
        if batch is None or batch["status"] != BATCH_CREATING:
            raise PublishError("批次草稿不存在或已被处理")
        n = conn.execute(
            "SELECT COUNT(*) c FROM rows WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]
        if n != batch["row_count"]:
            raise PublishError(
                f"批次行不完整：已入库 {n} 行，应为 {batch['row_count']} 行")
        conn.execute(
            "UPDATE batches SET status=?, owner_id='', updated_at=? "
            "WHERE id=?",
            (BATCH_STOPPED, now(), batch_id),
        )


def discard_batch_draft(conn: sqlite3.Connection, batch_id: int) -> None:
    """建批失败后清理草稿（只删 CREATING，绝不动已发布批次）。"""
    with conn:
        conn.execute(
            "DELETE FROM rows WHERE batch_id=? AND batch_id IN "
            "(SELECT id FROM batches WHERE id=? AND status=?)",
            (batch_id, batch_id, BATCH_CREATING),
        )
        conn.execute(
            "DELETE FROM batches WHERE id=? AND status=?",
            (batch_id, BATCH_CREATING),
        )


# ---------- 死控制台产物清理 ----------

@dataclass
class Garbage:
    """一次清理扫描的结果：文件由调用方在无数据库事务时删除，行随后短事务删除。"""
    dead_console_ids: list[str] = field(default_factory=list)
    draft_batch_ids: list[int] = field(default_factory=list)
    stored_names: list[str] = field(default_factory=list)  # 待删原件（含 .part）
    stale_lock: sqlite3.Row | None = None

    @property
    def has_rows(self) -> bool:
        return bool(self.dead_console_ids or self.draft_batch_ids
                    or self.stale_lock is not None)


def collect_garbage(conn: sqlite3.Connection,
                    ttl_seconds: int = CONSOLE_TTL_SECONDS) -> Garbage:
    """只读扫描：找出死控制台持有的未发布草稿、原件与残留锁（不持写锁）。"""
    live = live_console_ids(conn, ttl_seconds)
    g = Garbage()
    g.dead_console_ids = [
        r["id"] for r in conn.execute("SELECT id FROM consoles")
        if r["id"] not in live
    ]
    for b in conn.execute(
            "SELECT id, stored_name, owner_id FROM batches WHERE status=?",
            (BATCH_CREATING,)):
        if b["owner_id"] not in live:
            g.draft_batch_ids.append(b["id"])
            if b["stored_name"]:
                g.stored_names.append(b["stored_name"])
    lock = conn.execute("SELECT * FROM worker_lock WHERE id=1").fetchone()
    if lock and lock["owner_id"] not in live:
        g.stale_lock = lock
    return g


def remove_garbage_rows(conn: sqlite3.Connection, g: Garbage) -> None:
    """短事务删除残骸行；若因此不再有任何锁，说明没有任何 worker 在跑，
    残留的 SAVING/RUNNING 一并恢复（SAVING→UNVERIFIED，RUNNING→STOPPED）。"""
    with conn:
        if g.draft_batch_ids:
            marks = ",".join("?" * len(g.draft_batch_ids))
            conn.execute(
                f"DELETE FROM rows WHERE batch_id IN ({marks})",
                g.draft_batch_ids,
            )
            conn.execute(
                f"DELETE FROM batches WHERE id IN ({marks}) AND status=?",
                (*g.draft_batch_ids, BATCH_CREATING),
            )
        if g.stale_lock is not None:
            # 带属主条件：若锁已被其他清理者删除并换成新任务，绝不误删
            conn.execute(
                "DELETE FROM worker_lock WHERE id=1 AND owner_id=?",
                (g.stale_lock["owner_id"],),
            )
        if g.dead_console_ids:
            marks = ",".join("?" * len(g.dead_console_ids))
            conn.execute(
                f"DELETE FROM consoles WHERE id IN ({marks})",
                g.dead_console_ids,
            )
        locked = conn.execute(
            "SELECT COUNT(*) c FROM worker_lock").fetchone()["c"]
        if locked == 0:
            conn.execute(
                "UPDATE rows SET status=?, message='进程退出，保存结果待核实', "
                "updated_at=? WHERE status=?",
                (UNVERIFIED, now(), SAVING),
            )
            conn.execute(
                "UPDATE batches SET status=?, "
                "note=COALESCE(note,'') || ?, updated_at=? WHERE status=?",
                (BATCH_STOPPED,
                 f"[{now()}] 报送进程退出，批次转暂停待续跑；",
                 now(), BATCH_RUNNING),
            )


# ---------- 报送任务锁（全局单实例，带属主） ----------

class LockBusy(RuntimeError):
    pass


def acquire_lock(conn: sqlite3.Connection, batch_id: int,
                 owner_id: str = "") -> str:
    worker_id = uuid.uuid4().hex
    try:
        with conn:
            conn.execute(
                "INSERT INTO worker_lock(id, worker_id, batch_id, owner_id, "
                "acquired_at) VALUES(1, ?, ?, ?, ?)",
                (worker_id, batch_id, owner_id, now()),
            )
    except sqlite3.IntegrityError as exc:
        raise LockBusy(
            "已有报送任务在运行，每批同一时刻只允许一份；"
            "若属主控制台已退出，清理会在数秒内自动完成"
        ) from exc
    return worker_id


def release_lock(conn: sqlite3.Connection, worker_id: str) -> None:
    with conn:
        conn.execute("DELETE FROM worker_lock WHERE worker_id=?", (worker_id,))


def current_lock(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT wl.*, COALESCE((SELECT name FROM consoles "
        " WHERE consoles.id = wl.owner_id), '') AS owner_name "
        "FROM worker_lock wl WHERE id=1"
    ).fetchone()


def recover_batch_saving(conn: sqlite3.Connection, batch_id: int) -> int:
    """worker 线程异常（进程未退出）时，把该批残留的 SAVING 转待核实。"""
    with conn:
        cur = conn.execute(
            "UPDATE rows SET status=?, message='报送进程异常，保存结果待核实', "
            "updated_at=? WHERE batch_id=? AND status=?",
            (UNVERIFIED, now(), batch_id, SAVING),
        )
        return cur.rowcount


# ---------- 批次 / 行查询（CREATING 一律不可见） ----------

def set_batch_status(conn: sqlite3.Connection, batch_id: int, status: str,
                     note: str | None = None) -> None:
    with conn:
        if note is None:
            conn.execute(
                "UPDATE batches SET status=?, updated_at=? WHERE id=?",
                (status, now(), batch_id),
            )
        else:
            conn.execute(
                "UPDATE batches SET status=?, note=?, updated_at=? WHERE id=?",
                (status, note, now(), batch_id),
            )


def mark_batch_running(conn: sqlite3.Connection, batch_id: int,
                       resume: bool) -> None:
    action = "续跑" if resume else "开始报送"
    with conn:
        conn.execute(
            "UPDATE batches SET status=?, note=COALESCE(note,'') || ?, "
            "updated_at=? WHERE id=?",
            (BATCH_RUNNING, f"[{now()}] {action}；", now(), batch_id),
        )


def get_batch(conn: sqlite3.Connection, batch_id: int) -> sqlite3.Row | None:
    """只返回已发布批次；未发布草稿对外等同于不存在。"""
    return conn.execute(
        "SELECT * FROM batches WHERE id=? AND status!=?",
        (batch_id, BATCH_CREATING),
    ).fetchone()


def list_batches(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM batches WHERE status!=? ORDER BY id DESC",
        (BATCH_CREATING,),
    ).fetchall())


def get_rows(conn: sqlite3.Connection, batch_id: int) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM rows WHERE batch_id=? ORDER BY seq", (batch_id,)
    ).fetchall())


def get_row(conn: sqlite3.Connection, row_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM rows WHERE id=?", (row_id,)).fetchone()


def update_row(conn: sqlite3.Connection, row_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols = ", ".join(f"{k}=?" for k in fields)
    with conn:
        conn.execute(
            f"UPDATE rows SET {cols} WHERE id=?",
            (*fields.values(), row_id),
        )


def add_event(conn: sqlite3.Connection, row_id: int, event: str,
              detail: str = "") -> None:
    with conn:
        conn.execute(
            "INSERT INTO row_events(row_id, event, detail, created_at) "
            "VALUES(?,?,?,?)",
            (row_id, event, detail, now()),
        )


def get_events(conn: sqlite3.Connection, row_id: int) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM row_events WHERE row_id=? ORDER BY id", (row_id,)
    ).fetchall())


def row_counts(conn: sqlite3.Connection, batch_id: int) -> dict[str, int]:
    counts = {s: 0 for s in ROW_STATUS_LABELS}
    for r in conn.execute(
        "SELECT status, COUNT(*) c FROM rows WHERE batch_id=? GROUP BY status",
        (batch_id,),
    ):
        counts[r["status"]] = r["c"]
    return counts
