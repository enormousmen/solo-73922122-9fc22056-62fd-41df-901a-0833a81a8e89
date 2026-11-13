"""报送后台 Web 应用（FastAPI，多控制台共享版）。

两个控制台进程共享同一 SQLite 与上传目录。建批流程：

1. 校验 CSV（三列、非负十进制、业务键唯一）——不通过则整批不建；
2. 登记 CREATING 草稿批（短事务）：批次此刻起不可查询、不可报送，
   但清理逻辑据此知道这份原件归谁；
3. 原件落盘（incoming/*.part → fsync → 原子硬链接就位），全程不持数据库事务；
4. 全部行一次短事务入库；
5. 发布前校验（原件摘要 + 行数），通过才把批次转为可查询/可报送。

任何一步失败或进程中途退出，都只会留下 CREATING 残骸：永远不可报送，
并会被本控制台重启时或另一控制台的后台清理回收（只回收死控制台的）。
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db, uploads
from .browser_worker import ReportingWorker, WorkerManager
from .csvkit import CsvError, parse_csv

DB_PATH = os.environ.get("REPORTER_DB", "/data/reporter.db")
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "/data/uploads"))
SHOT_DIR = Path(os.environ.get("SCREENSHOT_DIR", "/data/screenshots"))
LEGACY_URL = os.environ.get("LEGACY_URL", "http://legacy:8000")
HEADLESS = os.environ.get("HEADLESS", "1") != "0"
SLOW_MO = int(os.environ.get("SLOW_MO_MS", "0"))
BROWSER_CHANNEL = os.environ.get("BROWSER_CHANNEL") or None

# 控制台身份：名称在部署中唯一（一个名称只跑一个进程），实例 id 每次启动随机
CONSOLE_NAME = os.environ.get("CONSOLE_NAME", "A")
CONSOLE_ID = os.environ.get("CONSOLE_ID") or uuid.uuid4().hex
CONSOLE_TTL = int(os.environ.get("CONSOLE_TTL_SECONDS",
                                 str(db.CONSOLE_TTL_SECONDS)))
JANITOR_INTERVAL = float(os.environ.get("JANITOR_INTERVAL_SECONDS", "2"))
MAX_DEMO_DELAY = 120   # 演示用建批延时上限（秒）

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["console_name"] = CONSOLE_NAME
templates.env.globals["peer_url"] = os.environ.get("PEER_CONSOLE_URL", "")

# 必须在挂载 StaticFiles 前建好目录，否则全新数据卷首启会抛异常
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
(UPLOAD_DIR / uploads.INCOMING).mkdir(parents=True, exist_ok=True)
SHOT_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="water-reporting-console")
app.mount("/screenshots", StaticFiles(directory=str(SHOT_DIR)),
          name="screenshots")

manager = WorkerManager()
_janitor_stop = threading.Event()
_janitor_thread: threading.Thread | None = None


def make_worker() -> ReportingWorker:
    return ReportingWorker(
        base_url=LEGACY_URL,
        db_path=DB_PATH,
        screenshot_dir=str(SHOT_DIR),
        headless=HEADLESS,
        slow_mo_ms=SLOW_MO,
        channel=BROWSER_CHANNEL,
        owner_id=CONSOLE_ID,
    )


def _cleanup_once() -> None:
    """心跳 + 回收死控制台产物。文件删除发生在没有任何数据库事务的空档。"""
    conn = db.connect(DB_PATH)
    try:
        db.heartbeat_console(conn, CONSOLE_ID)
        garbage = db.collect_garbage(conn, CONSOLE_TTL)
    finally:
        conn.close()
    if not (garbage.stored_names or garbage.has_rows):
        return
    for stored in garbage.stored_names:      # 纯文件操作，不持数据库锁
        uploads.delete_originals(UPLOAD_DIR, stored)
    if garbage.has_rows:
        conn = db.connect(DB_PATH)
        try:
            db.remove_garbage_rows(conn, garbage)
        finally:
            conn.close()
    print(f"[janitor] 回收死控制台产物：草稿批 {garbage.draft_batch_ids}，"
          f"原件 {garbage.stored_names}，"
          f"残留锁 {'有' if garbage.stale_lock is not None else '无'}")


def _janitor_loop() -> None:
    while not _janitor_stop.is_set():
        try:
            _cleanup_once()
        except Exception as exc:  # 清理失败下一轮再试，绝不影响主服务
            print(f"[janitor] 清理异常（下轮重试）：{exc}")
        _janitor_stop.wait(JANITOR_INTERVAL)


@app.on_event("startup")
def _startup() -> None:
    global _janitor_thread
    db.init_db(DB_PATH)
    conn = db.connect(DB_PATH)
    try:
        db.register_console(conn, CONSOLE_ID, CONSOLE_NAME)
        reaped = db.reap_same_name_consoles(conn, CONSOLE_NAME, CONSOLE_ID)
        if reaped:
            print(f"[startup] 回收同名旧控制台实例：{reaped}")
    finally:
        conn.close()
    _cleanup_once()  # 启动即清理：死控制台的未发布原件、残留锁一并回收
    _janitor_thread = threading.Thread(
        target=_janitor_loop, name="console-janitor", daemon=True)
    _janitor_thread.start()
    print(f"[startup] 控制台 {CONSOLE_NAME}（{CONSOLE_ID[:8]}）就绪，"
          f"心跳 {JANITOR_INTERVAL}s / 死亡判定 {CONSOLE_TTL}s")


@app.on_event("shutdown")
def _shutdown() -> None:
    _janitor_stop.set()
    try:
        conn = db.connect(DB_PATH)
        try:
            db.retire_console(conn, CONSOLE_ID)
        finally:
            conn.close()
    except Exception:
        pass


def _row_view(conn, row) -> dict:
    return {
        **dict(row),
        "status_label": db.ROW_STATUS_LABELS.get(row["status"], row["status"]),
    }


def _lock_view(lock) -> dict | None:
    if lock is None:
        return None
    return {
        "batch_id": lock["batch_id"],
        "owner_name": lock["owner_name"] or "另一控制台",
        "acquired_at": lock["acquired_at"],
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = db.connect(DB_PATH)
    try:
        batches = []
        for b in db.list_batches(conn):
            counts = db.row_counts(conn, b["id"])
            batches.append({
                **dict(b),
                "counts": counts,
                "total": sum(counts.values()),
            })
        lock = db.current_lock(conn)
        return templates.TemplateResponse(
            request, "index.html",
            {"batches": batches, "busy": manager.busy,
             "lock": _lock_view(lock),
             "legacy_url": LEGACY_URL,
             "max_demo_delay": MAX_DEMO_DELAY,
             "status_labels": db.ROW_STATUS_LABELS},
        )
    finally:
        conn.close()


@app.post("/batches")
async def create_batch(request: Request, file: UploadFile = File(...),
                       demo_delay: str = Form("0")):
    content = await file.read()
    if not content:
        return templates.TemplateResponse(
            request, "error.html", {"message": "上传文件为空"}, status_code=400)
    try:
        csv_rows = parse_csv(content)
    except CsvError as exc:
        return templates.TemplateResponse(
            request, "error.html",
            {"message": f"CSV 校验失败：{exc}"}, status_code=400)

    original_name = Path(file.filename or "upload.csv").name
    digest = hashlib.sha256(content).hexdigest()
    batch_id: int | None = None
    stored_name: str | None = None
    try:
        # 1) 登记草稿批 + 2) 原件落盘；文件名冲突（几乎不可能）时换名重试
        for _ in range(3):
            stored_name = uploads.unique_stored_name(original_name)
            conn = db.connect(DB_PATH)
            try:
                batch_id = db.create_batch_draft(
                    conn, original_name, stored_name, digest,
                    CONSOLE_ID, CONSOLE_NAME, len(csv_rows))
            finally:
                conn.close()
            try:
                uploads.write_original(UPLOAD_DIR, stored_name, content)
                break
            except FileExistsError:
                conn = db.connect(DB_PATH)
                try:
                    db.discard_batch_draft(conn, batch_id)
                finally:
                    conn.close()
                batch_id = None
        else:
            raise RuntimeError("无法分配唯一原件文件名")

        # 3) 全部行一次短事务入库（文件已就位，行与原件始终对应）
        conn = db.connect(DB_PATH)
        try:
            db.insert_batch_rows(
                conn, batch_id,
                [(r.seq, r.sample_no, r.project, r.result) for r in csv_rows])
        finally:
            conn.close()

        # 4) 演示钩子：拉长"草稿已建、尚未发布"的窗口，便于演练建批中途退出；
        #    期间本控制台心跳由后台线程维持，另一控制台不会误清这份原件
        try:
            delay = max(0, min(int(demo_delay or "0"), MAX_DEMO_DELAY))
        except ValueError:
            delay = 0
        if delay:
            await asyncio.sleep(delay)

        # 5) 发布前校验：原件与全部行都完整保存，批次才可查询和报送
        final = uploads.final_path(UPLOAD_DIR, stored_name)
        if not final.exists() or uploads.sha256_file(final) != digest:
            raise RuntimeError("原件校验失败：文件缺失或内容摘要不符")
        conn = db.connect(DB_PATH)
        try:
            db.publish_batch(conn, batch_id)
        finally:
            conn.close()
    except Exception as exc:
        # 任何一步失败：草稿行与原件一起清理，绝不留下"可报送的半批"
        if batch_id is not None:
            try:
                conn = db.connect(DB_PATH)
                try:
                    db.discard_batch_draft(conn, batch_id)
                finally:
                    conn.close()
            except Exception:
                pass
        if stored_name:
            uploads.delete_originals(UPLOAD_DIR, stored_name)
        return templates.TemplateResponse(
            request, "error.html",
            {"message": f"建批失败，半成品已清理：{exc}"}, status_code=500)
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


def _get_batch_or_none(conn, batch_id: int):
    batch = db.get_batch(conn, batch_id)
    if batch is None:
        return None, None
    rows = [_row_view(conn, r) for r in db.get_rows(conn, batch_id)]
    return batch, rows


@app.get("/batches/{batch_id}", response_class=HTMLResponse)
def batch_detail(request: Request, batch_id: int):
    conn = db.connect(DB_PATH)
    try:
        batch, rows = _get_batch_or_none(conn, batch_id)
        if batch is None:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"批次 {batch_id} 不存在或尚未发布"},
                status_code=404)
        lock = db.current_lock(conn)
        return templates.TemplateResponse(
            request, "batch.html",
            {
                "b": batch, "rows": rows,
                "counts": db.row_counts(conn, batch_id),
                "status_labels": db.ROW_STATUS_LABELS,
                "busy": manager.busy,
                "lock": _lock_view(lock),
                "actionable": db.ACTIONABLE,
            },
        )
    finally:
        conn.close()


@app.get("/batches/{batch_id}/original")
def download_original(batch_id: int):
    """下载批次对应的原件（仅已发布批次；文件名即用户上传时的名字）。"""
    conn = db.connect(DB_PATH)
    try:
        batch = db.get_batch(conn, batch_id)
        if batch is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        stored = batch["stored_name"]
        path = uploads.final_path(UPLOAD_DIR, stored) if stored else None
        if not path or not path.exists():
            return JSONResponse({"error": "原件文件缺失"}, status_code=404)
        return FileResponse(path, filename=batch["filename"],
                            media_type="text/csv")
    finally:
        conn.close()


@app.post("/batches/{batch_id}/start")
def start_batch(request: Request, batch_id: int, resume: str = Form("0")):
    do_resume = resume == "1"
    conn = db.connect(DB_PATH)
    try:
        batch = db.get_batch(conn, batch_id)
        if batch is None:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"批次 {batch_id} 不存在或尚未发布，不能报送"},
                status_code=404)
        lock = db.current_lock(conn)
        if manager.busy or lock:
            who = _lock_view(lock)["owner_name"] if lock else "本控制台"
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"已有报送任务在运行（{who}），每批同一时刻只允许"
                            "一份；请停止当前任务或稍后再试。"},
                status_code=409)
        if batch["status"] == db.BATCH_DONE and not do_resume:
            return RedirectResponse(url=f"/batches/{batch_id}",
                                    status_code=303)
        if do_resume:
            pending_rows = [r for r in db.get_rows(conn, batch_id)
                            if r["status"] in db.ACTIONABLE]
            if not pending_rows:
                return RedirectResponse(url=f"/batches/{batch_id}",
                                        status_code=303)
    finally:
        conn.close()

    try:
        manager.start(make_worker(), batch_id, do_resume)
    except RuntimeError as exc:
        return templates.TemplateResponse(
            request, "error.html", {"message": str(exc)}, status_code=409)
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.post("/batches/{batch_id}/stop")
def stop_batch(request: Request, batch_id: int):
    if not manager.busy:
        conn = db.connect(DB_PATH)
        try:
            lock = db.current_lock(conn)
        finally:
            conn.close()
        if lock:
            who = _lock_view(lock)["owner_name"]
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"报送任务正运行在控制台 {who}，"
                            "请到该控制台页面停止。"},
                status_code=409)
    manager.stop_current()
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.post("/batches/{batch_id}/rows/{row_id}/recheck")
def recheck_row(request: Request, batch_id: int, row_id: int):
    """人工在旧站处理完冲突后，把行重置为待核实，续跑时仍会先查询再决定。"""
    conn = db.connect(DB_PATH)
    try:
        row = db.get_row(conn, row_id)
        if row is None or row["batch_id"] != batch_id:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "行记录不存在"}, status_code=404)
        if manager.busy or db.current_lock(conn):
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "报送任务运行中，请先停止再重置行状态。"},
                status_code=409)
        if row["status"] != db.CONFLICT:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"仅冲突行可重置，当前状态 {row['status']}"},
                status_code=400)
        db.update_row(conn, row_id, status=db.UNVERIFIED, remote_id=None,
                      screenshot=None,
                      message="人工处理后重置为待核实，续跑将先查询核实")
        db.add_event(conn, row_id, "RESET_FOR_RECHECK",
                     "冲突行经人工处理，重置为待核实")
        db.set_batch_status(
            conn, batch_id, db.BATCH_STOPPED,
            note=f"[{db.now()}] 行 {row['seq']} 重置待核实，可续跑；")
    finally:
        conn.close()
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.get("/api/batches/{batch_id}")
def api_batch(batch_id: int):
    conn = db.connect(DB_PATH)
    try:
        batch, rows = _get_batch_or_none(conn, batch_id)
        if batch is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        snap = manager.worker.snapshot() if manager.worker else {}
        return JSONResponse({
            "batch": dict(batch),
            "busy": manager.busy,
            "lock": _lock_view(db.current_lock(conn)),
            "console": CONSOLE_NAME,
            "current_seq": snap.get("row_seq") if manager.busy else None,
            "counts": db.row_counts(conn, batch_id),
            "rows": rows,
        })
    finally:
        conn.close()


@app.get("/batches/{batch_id}/rows/{row_id}/events",
         response_class=HTMLResponse)
def row_events(request: Request, batch_id: int, row_id: int):
    conn = db.connect(DB_PATH)
    try:
        row = db.get_row(conn, row_id)
        if row is None or row["batch_id"] != batch_id:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "行记录不存在"}, status_code=404)
        events = db.get_events(conn, row_id)
        return templates.TemplateResponse(
            request, "events.html",
            {"b": db.get_batch(conn, batch_id), "row": row,
             "events": events,
             "status_label": db.ROW_STATUS_LABELS.get(row["status"],
                                                     row["status"])},
        )
    finally:
        conn.close()
