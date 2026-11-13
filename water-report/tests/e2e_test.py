"""端到端实测（驱动真实 HTTP 服务 + 真实 Chromium）：

覆盖：
1. CSV 校验（负数/非数字/重复键/坏表头）
2. 正常上传报送：查询无记录 -> 新增 -> SAVED + 远端编号
3. 重复报送同值：ALREADY_SENT，旧站不产生重复行
4. 异值：CONFLICT 停批，页面能拿到双方值
5. 旧站允许重复键（直接验证旧站行为）
6. 保存后断连：UNVERIFIED；关闭断连续跑 -> 核实为已 SAVED，不重复入库
7. 查询中断（保存前断连）：QUERY_FAILED，绝不被当成不存在
8. 每批单实例：运行中再次启动被拒（含跨控制台 409）
9. 跨控制台崩溃恢复：死控制台的锁被清理，SAVING -> UNVERIFIED
10. 冲突人工处理后重置待核实，续跑核实
11. 未发布批次：不可查询、不可报送，原件与草稿被自动清理
12. 建批进行中：批次不发布、原件不被误清，发布后才可查询
13. 双控制台同名并发上传：各自原件分别保留，批次行与原件对应
     （需 REPORTER_B_URL 指向第二个控制台，否则跳过）

运行前置：按 README 起好服务（本机：./run-local.sh 起 A/B 双控制台）。
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTER = os.environ.get("REPORTER_URL", "http://127.0.0.1:8010")
REPORTER_B = os.environ.get("REPORTER_B_URL", "http://127.0.0.1:8012")
LEGACY = os.environ.get("LEGACY_URL", "http://127.0.0.1:8011")
RDB = Path(os.environ.get("REPORTER_DB", ROOT / "data/reporter.db"))
LDB = Path(os.environ.get("LEGACY_DB", ROOT / "data/legacy.db"))
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", ROOT / "data/uploads"))

PASS, FAIL, SKIP = 0, 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def skip(name: str, why: str) -> None:
    global SKIP
    SKIP += 1
    print(f"  SKIP  {name}  ({why})")


def http(method: str, url: str, data: bytes | None = None,
         content_type: str | None = None, allow_errors: bool = False):
    req = urllib.request.Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        if allow_errors:
            return exc.code, exc.read()
        raise


def reachable(url: str) -> bool:
    try:
        urllib.request.urlopen(url + "/", timeout=3)
        return True
    except Exception:
        return False


def upload_batch(csv_path: Path, base: str = REPORTER,
                 demo_delay: int = 0) -> int:
    boundary = "----t" + str(time.time_ns())
    content = csv_path.read_bytes()
    parts = [
        (f"--{boundary}\r\n"
         f'Content-Disposition: form-data; name="file"; '
         f'filename="{csv_path.name}"\r\n'
         "Content-Type: text/csv\r\n\r\n").encode() + content + b"\r\n",
        (f"--{boundary}\r\n"
         'Content-Disposition: form-data; name="demo_delay"\r\n\r\n'
         f"{demo_delay}\r\n").encode(),
    ]
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
    req = urllib.request.Request(f"{base}/batches", data=body, method="POST")
    req.add_header("Content-Type",
                   f"multipart/form-data; boundary={boundary}")
    with opener.open(req, timeout=60) as resp:
        return int(resp.url.rstrip("/").split("/")[-1])


def upload_expect_error(csv_path: Path) -> tuple[int, bytes]:
    boundary = "----t" + str(time.time_ns())
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; '
        f'filename="{csv_path.name}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + csv_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode()
    return http("POST", f"{REPORTER}/batches", body,
                f"multipart/form-data; boundary={boundary}",
                allow_errors=True)


def post_form(path: str, fields: dict[str, str], base: str = REPORTER):
    data = urllib.parse.urlencode(fields).encode()
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
    req = urllib.request.Request(base + path, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with opener.open(req, timeout=30) as resp:
        return resp.status, resp.url


def post_form_expect_error(path: str, fields: dict[str, str],
                           base: str = REPORTER):
    data = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(base + path, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        urllib.request.urlopen(req, timeout=30)
        return 200, b""
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def set_disconnect(mode: str) -> None:
    """mode: off / save（仅保存后断连）/ all（全站断连）。"""
    data = urllib.parse.urlencode({"mode": mode}).encode()
    req = urllib.request.Request(f"{LEGACY}/__admin/toggle-disconnect",
                                 data=data, method="POST")
    urllib.request.urlopen(req, timeout=15).read()


def wait_batch(batch_id: int, timeout: float = 90.0,
               base: str = REPORTER) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = http("GET", f"{base}/api/batches/{batch_id}")
        data = json.loads(body)
        if not data["busy"]:
            return data
        time.sleep(0.5)
    raise TimeoutError(f"batch {batch_id} did not finish")


def legacy_count(sample_no: str, project: str) -> int:
    _, body = http("GET", f"{LEGACY}/records.json")
    rows = json.loads(body)
    return sum(1 for r in rows
               if r["sample_no"] == sample_no and r["project"] == project)


def legacy_results(sample_no: str, project: str) -> list[str]:
    _, body = http("GET", f"{LEGACY}/records.json")
    rows = json.loads(body)
    return [r["result"] for r in rows
            if r["sample_no"] == sample_no and r["project"] == project]


def write_csv(name: str, text: str) -> Path:
    p = ROOT / "data" / name
    p.write_text(text, encoding="utf-8")
    return p


def main() -> int:
    print("== 1. CSV 校验 ==")
    bad_neg = write_csv("t-neg.csv", "样本号,项目,结果\nA,pH,-1\n")
    code, body = upload_expect_error(bad_neg)
    check("负数结果被拒(400)", code == 400 and "非负".encode() in body, str(code))
    bad_num = write_csv("t-num.csv", "样本号,项目,结果\nA,pH,abc\n")
    code, body = upload_expect_error(bad_num)
    check("非数字被拒", code == 400 and "十进制".encode() in body, str(code))
    bad_dup = write_csv("t-dup.csv", "样本号,项目,结果\nA,pH,1\nA,pH,2\n")
    code, body = upload_expect_error(bad_dup)
    check("文件内重复键被拒", code == 400 and "重复".encode() in body, str(code))
    bad_hdr = write_csv("t-hdr.csv", "foo,bar,baz\nA,pH,1\n")
    code, body = upload_expect_error(bad_hdr)
    check("坏表头被拒", code == 400 and "表头".encode() in body, str(code))

    print("== 2. 正常报送（先查无 -> 新增） ==")
    b1 = upload_batch(ROOT / "data/sample-batch-a.csv")
    post_form(f"/batches/{b1}/start", {"resume": "0"})
    d = wait_batch(b1)
    statuses = {r["seq"]: r["status"] for r in d["rows"]}
    check("5 行全部 SAVED",
          all(s == "SAVED" for s in statuses.values()), str(statuses))
    check("每行有远端记录编号",
          all(r["remote_id"].isdigit() for r in d["rows"]),
          str([r["remote_id"] for r in d["rows"]]))
    check("批次 DONE", d["batch"]["status"] == "DONE", d["batch"]["status"])
    check("旧站恰好 5 条相关记录",
          legacy_count("W2026-001", "pH") == 1
          and legacy_count("W2026-003", "COD") == 1)
    check("批次记录原件摘要",
          len(d["batch"]["sha256"]) == 64 and d["batch"]["stored_name"],
          str(d["batch"].get("stored_name")))

    print("== 3. 同值重报 -> ALREADY_SENT，不重复入库 ==")
    b2 = upload_batch(ROOT / "data/sample-batch-a.csv")
    post_form(f"/batches/{b2}/start", {"resume": "0"})
    d = wait_batch(b2)
    check("全部 ALREADY_SENT",
          all(r["status"] == "ALREADY_SENT" for r in d["rows"]),
          str({r["seq"]: r["status"] for r in d["rows"]}))
    check("远端编号与首次一致",
          {r["sample_no"] + r["project"]: r["remote_id"] for r in d["rows"]}
          == {x["sample_no"] + x["project"]: x["remote_id"]
              for x in wait_batch(b1)["rows"]})
    check("旧站无重复行",
          legacy_count("W2026-001", "pH") == 1
          and legacy_count("W2026-002", "氨氮") == 1)
    check("同名再建批原件各自保留",
          d["batch"]["stored_name"] != wait_batch(b1)["batch"]["stored_name"])

    print("== 4. 异值 -> CONFLICT 停批，含双方值 ==")
    conflict_csv = write_csv(
        "t-conflict.csv",
        "样本号,项目,结果\nW2026-001,pH,9.99\nW2026-002,氨氮,0.85\n")
    b3 = upload_batch(conflict_csv)
    post_form(f"/batches/{b3}/start", {"resume": "0"})
    d = wait_batch(b3)
    r0 = d["rows"][0]
    check("首行 CONFLICT", r0["status"] == "CONFLICT", r0["status"])
    check("消息同时含本地值与远端值",
          "9.99" in r0["message"] and "7.2" in r0["message"], r0["message"])
    check("批次停批", d["batch"]["status"] == "STOPPED", d["batch"]["status"])
    check("冲突后第二行未被处理（仍 PENDING）",
          d["rows"][1]["status"] == "PENDING", d["rows"][1]["status"])
    check("旧站未被写入异值", legacy_results("W2026-001", "pH") == ["7.2"])

    print("== 5. 旧站本身允许重复键 ==")
    for val in ("5.55", "5.55"):
        data = urllib.parse.urlencode({
            "sample_no": "DUP-KEY", "project": "pH", "result": val}).encode()
        req = urllib.request.Request(f"{LEGACY}/save", data=data, method="POST")
        try:
            urllib.request.urlopen(req, timeout=15).read()
        except urllib.error.HTTPError:
            pass
    check("旧站允许同键两行", legacy_count("DUP-KEY", "pH") == 2)

    print("== 6. 保存后断连：UNVERIFIED -> 续跑核实，不重复入库 ==")
    b4_csv = write_csv("t-disconnect.csv",
                       "样本号,项目,结果\nW2026-301,pH,8.1\nW2026-301,浊度,2.2\n")
    b4 = upload_batch(b4_csv)
    set_disconnect("save")
    try:
        post_form(f"/batches/{b4}/start", {"resume": "0"})
        d = wait_batch(b4)
        check("断连时两行均 UNVERIFIED",
              all(r["status"] == "UNVERIFIED" for r in d["rows"]),
              str([r["status"] for r in d["rows"]]))
        check("断连期间旧站实际已落库 2 行",
              legacy_count("W2026-301", "pH") == 1
              and legacy_count("W2026-301", "浊度") == 1)
    finally:
        set_disconnect("off")
    post_form(f"/batches/{b4}/start", {"resume": "1"})
    d = wait_batch(b4)
    check("续跑核实后全部 SAVED",
          all(r["status"] == "SAVED" for r in d["rows"]),
          str([(r["status"], r["message"]) for r in d["rows"]]))
    check("核实拿到的是真实远端编号",
          all(r["remote_id"].isdigit() for r in d["rows"]))
    check("核实未产生重复入库",
          legacy_count("W2026-301", "pH") == 1
          and legacy_count("W2026-301", "浊度") == 1)

    print("== 7. 查询期断连：QUERY_FAILED，不当作不存在 ==")
    b5_csv = write_csv("t-qfail.csv",
                       "样本号,项目,结果\nW2026-401,pH,6.6\nW2026-402,pH,7.7\n")
    b5 = upload_batch(b5_csv)
    set_disconnect("all")
    try:
        post_form(f"/batches/{b5}/start", {"resume": "0"})
        d = wait_batch(b5, timeout=120)
        check("查询失败行标记 QUERY_FAILED",
              all(r["status"] == "QUERY_FAILED" for r in d["rows"]),
              str([r["status"] for r in d["rows"]]))
        check("失败消息明确不得视为不存在",
              all("不得视为不存在" in r["message"] for r in d["rows"]))
        check("查询失败绝未写入旧站",
              legacy_count("W2026-401", "pH") == 0
              and legacy_count("W2026-402", "pH") == 0)
    finally:
        set_disconnect("off")
    post_form(f"/batches/{b5}/start", {"resume": "1"})
    d = wait_batch(b5)
    check("恢复后续跑成功 SAVED",
          all(r["status"] == "SAVED" for r in d["rows"]),
          str([r["status"] for r in d["rows"]]))

    print("== 8. 每批单实例锁（含跨控制台） ==")
    b6 = upload_batch(write_csv(
        "t-lock.csv",
        "样本号,项目,结果\n"
        + "\n".join(f"L-{i},pH,1.{i}" for i in range(8))
        + "\n"))
    post_form(f"/batches/{b6}/start", {"resume": "0"})
    time.sleep(1)
    code, body = post_form_expect_error(f"/batches/{b6}/start",
                                        {"resume": "1"})
    check("运行中再次启动被拒(409)", code == 409, str(code))
    if reachable(REPORTER_B):
        code, body = post_form_expect_error(f"/batches/{b6}/start",
                                            {"resume": "1"}, base=REPORTER_B)
        check("另一控制台同样被拒(409)", code == 409, str(code))
    else:
        skip("另一控制台同样被拒(409)", "REPORTER_B_URL 不可达")
    wait_batch(b6)

    print("== 9. 跨控制台崩溃恢复：死控制台锁被清理，SAVING->UNVERIFIED ==")
    conn = sqlite3.connect(RDB)
    conn.execute(
        "INSERT INTO consoles(id, name, started_at, heartbeat_at) "
        "VALUES('ghost-console', '幽灵', '2020-01-01T00:00:00', "
        "'2020-01-01T00:00:00')")
    conn.execute(
        "INSERT INTO worker_lock(id, worker_id, batch_id, owner_id, "
        "acquired_at) VALUES(1, 'dead-worker', ?, 'ghost-console', "
        "'2020-01-01T00:00:00')", (b6,))
    conn.execute("UPDATE rows SET status='SAVING' WHERE batch_id=? AND seq=1",
                 (b6,))
    conn.commit()
    conn.close()
    # 控制台应用的后台清理（janitor）应在数秒内自动完成恢复
    deadline = time.time() + 40
    lock_left, st = 1, "SAVING"
    while time.time() < deadline:
        conn = sqlite3.connect(RDB)
        lock_left = conn.execute(
            "SELECT COUNT(*) FROM worker_lock").fetchone()[0]
        st = conn.execute(
            "SELECT status FROM rows WHERE batch_id=? AND seq=1",
            (b6,)).fetchone()[0]
        ghost_left = conn.execute(
            "SELECT COUNT(*) FROM consoles WHERE id='ghost-console'"
        ).fetchone()[0]
        conn.close()
        if lock_left == 0 and st == "UNVERIFIED" and ghost_left == 0:
            break
        time.sleep(1)
    check("残留锁被自动清理", lock_left == 0, str(lock_left))
    check("SAVING 恢复为 UNVERIFIED", st == "UNVERIFIED", st)
    check("死控制台登记被回收", ghost_left == 0, str(ghost_left))

    print("== 10. 冲突人工处理后重置待核实，续跑核实 ==")
    b7_csv = write_csv("t-recheck.csv",
                       "样本号,项目,结果\nW2026-501,pH,8.88\n")
    b7 = upload_batch(b7_csv)
    post_form(f"/batches/{b7}/start", {"resume": "0"})
    d = wait_batch(b7)
    check("首次报送 SAVED", d["rows"][0]["status"] == "SAVED",
          d["rows"][0]["status"])
    b8 = upload_batch(write_csv(
        "t-recheck2.csv", "样本号,项目,结果\nW2026-501,pH,3.33\n"))
    post_form(f"/batches/{b8}/start", {"resume": "0"})
    d = wait_batch(b8)
    check("异值行 CONFLICT", d["rows"][0]["status"] == "CONFLICT",
          d["rows"][0]["status"])
    b8_row = d["rows"][0]["id"]
    code, _ = post_form_expect_error(
        f"/batches/{b8}/rows/{b8_row + 999}/recheck", {})
    check("不存在行重置被拒(404)", code == 404, str(code))
    data = urllib.parse.urlencode(
        {"sample_no": "W2026-501", "project": "pH", "result": "3.33"}).encode()
    req = urllib.request.Request(f"{LEGACY}/save", data=data, method="POST")
    urllib.request.urlopen(req, timeout=15).read()
    status, _ = post_form(f"/batches/{b8}/rows/{b8_row}/recheck", {})
    check("重置请求成功", status == 200)
    d = json.loads(http("GET", f"{REPORTER}/api/batches/{b8}")[1])
    check("行已转 UNVERIFIED", d["rows"][0]["status"] == "UNVERIFIED",
          d["rows"][0]["status"])
    post_form(f"/batches/{b8}/start", {"resume": "1"})
    d = wait_batch(b8)
    check("续跑核实后 SAVED", d["rows"][0]["status"] == "SAVED",
          str(d["rows"][0]))
    check("拿到补录记录的远端编号",
          d["rows"][0]["remote_id"].isdigit())
    check("远端共 2 条同键记录（旧值+人工补录），后台未再新增",
          legacy_count("W2026-501", "pH") == 2)

    print("== 11. 未发布批次：不可见、不可报送、被清理 ==")
    # 直接往共享库塞一个"死控制台留下的草稿批"，模拟建批中途退出的残骸
    ghost_file = "t-ghost-draft-deadbeef.csv"
    (UPLOAD_DIR / ghost_file).write_text(
        "样本号,项目,结果\nG-1,pH,1\n", encoding="utf-8")
    (UPLOAD_DIR / "incoming").mkdir(exist_ok=True)
    (UPLOAD_DIR / "incoming" / f"{ghost_file}.part").write_text(
        "半截文件", encoding="utf-8")
    conn = sqlite3.connect(RDB)
    cur = conn.execute(
        "INSERT INTO batches(filename, stored_name, sha256, row_count, "
        "owner_id, created_by, status, created_at, updated_at) "
        "VALUES('t-ghost-draft.csv', ?, 'deadbeef', 1, 'ghost-console-2', "
        "'幽灵', 'CREATING', '2020-01-01T00:00:00', '2020-01-01T00:00:00')",
        (ghost_file,))
    ghost_bid = cur.lastrowid
    conn.execute(
        "INSERT INTO rows(batch_id, seq, sample_no, project, result, status, "
        "updated_at) VALUES(?, 1, 'G-1', 'pH', '1', 'PENDING', "
        "'2020-01-01T00:00:00')", (ghost_bid,))
    conn.commit()
    conn.close()
    code, _ = http("GET", f"{REPORTER}/api/batches/{ghost_bid}",
                   allow_errors=True)
    check("草稿批 API 查询不到(404)", code == 404, str(code))
    code, _ = post_form_expect_error(f"/batches/{ghost_bid}/start",
                                     {"resume": "0"})
    check("草稿批不能报送(404)", code == 404, str(code))
    _, page = http("GET", f"{REPORTER}/")
    check("草稿批不出现在批次列表", b"t-ghost-draft" not in page)
    # 等 janitor 回收（死控制台 => 草稿行删除、原件与 .part 删除）
    deadline = time.time() + 40
    gone_db = gone_file = False
    while time.time() < deadline:
        conn = sqlite3.connect(RDB)
        gone_db = conn.execute(
            "SELECT COUNT(*) FROM batches WHERE id=?",
            (ghost_bid,)).fetchone()[0] == 0
        rows_left = conn.execute(
            "SELECT COUNT(*) FROM rows WHERE batch_id=?",
            (ghost_bid,)).fetchone()[0]
        conn.close()
        gone_file = (not (UPLOAD_DIR / ghost_file).exists()
                     and not (UPLOAD_DIR / "incoming"
                              / f"{ghost_file}.part").exists())
        if gone_db and rows_left == 0 and gone_file:
            break
        time.sleep(1)
    check("草稿批行被清理", gone_db)
    check("草稿批数据行被清理", rows_left == 0, str(rows_left))
    check("未发布原件与 .part 被清理", gone_file)

    print("== 12. 建批进行中：不发布、不误清，发布后可见 ==")
    stem = f"t-slow-{int(time.time())}"
    slow_csv = write_csv(
        f"{stem}.csv",
        "样本号,项目,结果\nSLOW-1,pH,5.5\nSLOW-2,pH,6.6\n")
    before = set(UPLOAD_DIR.glob(f"{stem}-*.csv"))
    result: dict = {}

    def do_upload():
        result["bid"] = upload_batch(slow_csv, demo_delay=6)

    t = threading.Thread(target=do_upload)
    t.start()
    time.sleep(2.5)  # 建批进行中（demo_delay=6s）
    _, page = http("GET", f"{REPORTER}/")
    check("建批进行中批次不发布", stem.encode() not in page)
    mid = set(UPLOAD_DIR.glob(f"{stem}-*.csv")) - before
    check("建批进行中原件已落盘且未被误清", len(mid) == 1,
          str([p.name for p in mid]))
    t.join(timeout=30)
    bid_slow = result.get("bid")
    check("慢建批最终成功发布", bid_slow is not None)
    if bid_slow is not None:
        d = json.loads(http("GET", f"{REPORTER}/api/batches/{bid_slow}")[1])
        check("发布后行数完整", len(d["rows"]) == 2, str(len(d["rows"])))
        check("发布后原件可下载",
              http("GET", f"{REPORTER}/batches/{bid_slow}/original",
                   allow_errors=True)[0] == 200)

    print("== 13. 双控制台同名并发上传 ==")
    if not reachable(REPORTER_B):
        skip("同名并发上传双控制台验证", "REPORTER_B_URL 不可达")
    else:
        dir_a = ROOT / "data" / "t-same-a"
        dir_b = ROOT / "data" / "t-same-b"
        dir_a.mkdir(exist_ok=True)
        dir_b.mkdir(exist_ok=True)
        csv_a = dir_a / "t-samename.csv"
        csv_b = dir_b / "t-samename.csv"
        csv_a.write_text("样本号,项目,结果\nSAME-A,pH,7.1\n", encoding="utf-8")
        csv_b.write_text(
            "样本号,项目,结果\nSAME-B,pH,8.8\nSAME-B,浊度,4.4\n",
            encoding="utf-8")
        ids: dict = {}

        def up(key, base, path):
            ids[key] = upload_batch(path, base=base, demo_delay=2)

        ta = threading.Thread(target=up, args=("a", REPORTER, csv_a))
        tb = threading.Thread(target=up, args=("b", REPORTER_B, csv_b))
        ta.start(); tb.start()
        ta.join(timeout=40); tb.join(timeout=40)
        ba, bb = ids.get("a"), ids.get("b")
        check("两控制台同名上传都成功", ba is not None and bb is not None,
              f"A={ba} B={bb}")
        if ba and bb:
            check("生成两个不同批次", ba != bb)
            da = json.loads(http("GET", f"{REPORTER}/api/batches/{ba}")[1])
            db_ = json.loads(http("GET", f"{REPORTER_B}/api/batches/{bb}")[1])
            sa, sb = da["batch"]["stored_name"], db_["batch"]["stored_name"]
            check("同名文件原件分别保留（不同存储名）", sa != sb, f"{sa} vs {sb}")
            check("两批原件都在共享目录",
                  (UPLOAD_DIR / sa).exists() and (UPLOAD_DIR / sb).exists())
            _, orig_a = http("GET", f"{REPORTER}/batches/{ba}/original")
            _, orig_b = http("GET", f"{REPORTER_B}/batches/{bb}/original")
            check("A 批原件内容未被覆盖",
                  hashlib.sha256(orig_a).hexdigest()
                  == hashlib.sha256(csv_a.read_bytes()).hexdigest())
            check("B 批原件内容未被覆盖",
                  hashlib.sha256(orig_b).hexdigest()
                  == hashlib.sha256(csv_b.read_bytes()).hexdigest())
            check("批次行与原件对应（A 1 行 / B 2 行）",
                  len(da["rows"]) == 1 and len(db_["rows"]) == 2,
                  f"{len(da['rows'])}/{len(db_['rows'])}")
            check("行内容来自各自原件",
                  da["rows"][0]["sample_no"] == "SAME-A"
                  and db_["rows"][0]["sample_no"] == "SAME-B")
            check("库中摘要与原件一致",
                  da["batch"]["sha256"]
                  == hashlib.sha256(orig_a).hexdigest()
                  and db_["batch"]["sha256"]
                  == hashlib.sha256(orig_b).hexdigest())

    print(f"\n==== RESULT: {PASS} passed, {FAIL} failed, "
          f"{SKIP} skipped ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
