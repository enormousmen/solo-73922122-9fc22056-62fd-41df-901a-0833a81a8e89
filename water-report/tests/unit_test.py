"""不依赖浏览器/服务的单元测试：

- CSV 三列校验（保留原有规则）
- 建批两阶段：草稿不可见不可报送，发布后才可见
- 死控制台产物清理：只回收死控制台，绝不动活控制台的半成品
- 报送锁带属主：活锁不误清，死锁回收并恢复现场
"""
import sqlite3

import pytest

from reporter import db
from reporter.csvkit import CsvError, parse_csv


def csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class TestCsv:
    def test_basic_parse(self):
        rows = parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,7.2\nS2,COD,24\n"))
        assert len(rows) == 2
        assert rows[0].sample_no == "S1"
        assert rows[0].result == "7.2"
        assert rows[1].seq == 2

    def test_english_headers_accepted(self):
        rows = parse_csv(csv_bytes("sample_no,project,result\nS1,pH,0\n"))
        assert rows[0].result == "0"

    def test_negative_rejected(self):
        with pytest.raises(CsvError, match="非负"):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,-0.1\n"))

    def test_non_decimal_rejected(self):
        with pytest.raises(CsvError, match="十进制"):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,abc\n"))

    def test_infinity_rejected(self):
        with pytest.raises(CsvError):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,Infinity\n"))

    def test_duplicate_key_rejected(self):
        with pytest.raises(CsvError, match="重复"):
            parse_csv(csv_bytes(
                "样本号,项目,结果\nS1,pH,1\nS1,pH,2\n"))

    def test_missing_header_rejected(self):
        with pytest.raises(CsvError, match="表头"):
            parse_csv(csv_bytes("a,b,c\nS1,pH,1\n"))

    def test_blank_lines_skipped(self):
        rows = parse_csv(csv_bytes(
            "样本号,项目,结果\nS1,pH,1\n\n  \nS2,pH,2\n"))
        assert len(rows) == 2

    def test_gb18030_encoding(self):
        text = "样本号,项目,结果\nS1,pH,1\n".encode("gb18030")
        rows = parse_csv(text)
        assert rows[0].sample_no == "S1"


@pytest.fixture()
def conn(tmp_path):
    path = str(tmp_path / "test.db")
    db.init_db(path)
    c = db.connect(path)
    yield c
    c.close()


def make_draft(conn, owner="console-a", name="A", stored="f-abc.csv",
               rows=((1, "S1", "pH", "7.2"),)):
    bid = db.create_batch_draft(conn, "f.csv", stored, "sha-x", owner, name,
                                len(rows))
    db.insert_batch_rows(conn, bid, list(rows))
    return bid


def publish(conn, bid):
    db.publish_batch(conn, bid)


class TestBatchCreation:
    def test_draft_invisible_until_published(self, conn):
        bid = make_draft(conn)
        assert db.get_batch(conn, bid) is None       # 草稿不可查询
        assert db.list_batches(conn) == []           # 列表不出现
        publish(conn, bid)
        assert db.get_batch(conn, bid)["status"] == db.BATCH_STOPPED
        assert len(db.list_batches(conn)) == 1

    def test_publish_requires_all_rows(self, conn):
        bid = db.create_batch_draft(conn, "f.csv", "f-1.csv", "sha", "o", "A", 2)
        db.insert_batch_rows(conn, bid, [(1, "S1", "pH", "7.2")])  # 少一行
        with pytest.raises(db.PublishError, match="不完整"):
            db.publish_batch(conn, bid)
        assert db.get_batch(conn, bid) is None       # 仍未发布

    def test_publish_twice_rejected(self, conn):
        bid = make_draft(conn)
        publish(conn, bid)
        with pytest.raises(db.PublishError):
            db.publish_batch(conn, bid)

    def test_discard_only_touches_drafts(self, conn):
        bid = make_draft(conn)
        db.discard_batch_draft(conn, bid)
        assert conn.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM rows").fetchone()["c"] == 0
        # 已发布批次不受 discard 影响
        bid2 = make_draft(conn)
        publish(conn, bid2)
        db.discard_batch_draft(conn, bid2)
        assert db.get_batch(conn, bid2) is not None

    def test_owner_cleared_after_publish(self, conn):
        bid = make_draft(conn)
        publish(conn, bid)
        assert db.get_batch(conn, bid)["owner_id"] == ""


class TestGarbageCollection:
    def _register(self, conn, cid, name, heartbeat=None):
        db.register_console(conn, cid, name)
        if heartbeat is not None:
            conn.execute("UPDATE consoles SET heartbeat_at=? WHERE id=?",
                         (heartbeat, cid))
            conn.commit()

    def test_dead_console_draft_collected_live_kept(self, conn):
        self._register(conn, "live-a", "A")                      # 心跳新鲜
        self._register(conn, "dead-b", "B", "2020-01-01T00:00:00")
        live_draft = make_draft(conn, owner="live-a", stored="live-1.csv")
        dead_draft = make_draft(conn, owner="dead-b", stored="dead-1.csv")

        g = db.collect_garbage(conn, ttl_seconds=15)
        assert dead_draft in g.draft_batch_ids
        assert live_draft not in g.draft_batch_ids     # 活控制台的半成品不动
        assert "dead-1.csv" in g.stored_names
        assert "live-1.csv" not in g.stored_names
        assert "dead-b" in g.dead_console_ids
        assert "live-a" not in g.dead_console_ids

        db.remove_garbage_rows(conn, g)
        assert conn.execute("SELECT COUNT(*) c FROM batches WHERE id=?",
                            (dead_draft,)).fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM rows WHERE batch_id=?",
                            (dead_draft,)).fetchone()["c"] == 0
        # 活控制台的草稿行完好
        assert conn.execute("SELECT COUNT(*) c FROM rows WHERE batch_id=?",
                            (live_draft,)).fetchone()["c"] == 1

    def test_stale_lock_recovered_live_lock_untouched(self, conn):
        self._register(conn, "live-a", "A")
        bid = make_draft(conn, owner="live-a")
        publish(conn, bid)
        wid = db.acquire_lock(conn, bid, owner_id="live-a")
        g = db.collect_garbage(conn, ttl_seconds=15)
        assert g.stale_lock is None                    # 活控制台的锁不清
        db.remove_garbage_rows(conn, g)
        assert db.current_lock(conn)["worker_id"] == wid

        # 属主心跳停摆 -> 锁被回收，SAVING/RUNNING 现场恢复
        conn.execute("UPDATE consoles SET heartbeat_at='2020-01-01T00:00:00' "
                     "WHERE id='live-a'")
        conn.commit()
        row = db.get_rows(conn, bid)[0]
        db.update_row(conn, row["id"], status=db.SAVING)
        db.mark_batch_running(conn, bid, resume=False)
        g = db.collect_garbage(conn, ttl_seconds=15)
        assert g.stale_lock is not None
        db.remove_garbage_rows(conn, g)
        assert db.current_lock(conn) is None
        assert db.get_row(conn, row["id"])["status"] == db.UNVERIFIED
        assert db.get_batch(conn, bid)["status"] == db.BATCH_STOPPED

    def test_unknown_owner_is_dead(self, conn):
        # 属主从未登记过（进程痕迹全无）=> 视为死控制台
        bid = make_draft(conn, owner="ghost", stored="ghost.csv")
        g = db.collect_garbage(conn, ttl_seconds=15)
        assert bid in g.draft_batch_ids
        assert "ghost.csv" in g.stored_names

    def test_reap_same_name_consoles(self, conn):
        self._register(conn, "old-instance", "A", "2020-01-01T00:00:00")
        self._register(conn, "other", "B")
        reaped = db.reap_same_name_consoles(conn, "A", keep_id="new-instance")
        assert reaped == ["old-instance"]
        left = {r["id"] for r in conn.execute("SELECT id FROM consoles")}
        assert left == {"other"}


class TestLock:
    def _published_batch(self, conn):
        bid = make_draft(conn)
        publish(conn, bid)
        return bid

    def test_lock_is_single_slot(self, conn):
        bid = self._published_batch(conn)
        wid = db.acquire_lock(conn, bid, owner_id="a")
        assert wid
        with pytest.raises(db.LockBusy):
            db.acquire_lock(conn, bid, owner_id="b")
        db.release_lock(conn, wid)
        assert db.acquire_lock(conn, bid, owner_id="b")

    def test_current_lock_shows_owner_name(self, conn):
        db.register_console(conn, "a", "控制台A")
        bid = self._published_batch(conn)
        db.acquire_lock(conn, bid, owner_id="a")
        lock = db.current_lock(conn)
        assert lock["owner_name"] == "控制台A"

    def test_recover_batch_saving(self, conn):
        bid = self._published_batch(conn)
        row = db.get_rows(conn, bid)[0]
        db.update_row(conn, row["id"], status=db.SAVING)
        assert db.recover_batch_saving(conn, bid) == 1
        assert db.get_row(conn, row["id"])["status"] == db.UNVERIFIED


class TestStateMachine:
    def test_terminal_states_not_actionable(self):
        for st in (db.SAVED, db.ALREADY_SENT, db.CONFLICT):
            assert st not in db.ACTIONABLE
        for st in (db.PENDING, db.QUERY_FAILED, db.UNVERIFIED):
            assert st in db.ACTIONABLE

    def test_events_append(self, conn):
        bid = make_draft(conn)
        publish(conn, bid)
        rid = db.get_rows(conn, bid)[0]["id"]
        db.add_event(conn, rid, "SAVING", "detail-x")
        db.add_event(conn, rid, "SAVED", "remote_id=9")
        events = db.get_events(conn, rid)
        assert [e["event"] for e in events] == ["SAVING", "SAVED"]
        assert events[1]["detail"] == "remote_id=9"

    def test_row_counts(self, conn):
        bid = db.create_batch_draft(conn, "f.csv", "f-1.csv", "sha", "o", "A", 2)
        db.insert_batch_rows(conn, bid,
                             [(1, "S1", "pH", "1"), (2, "S2", "pH", "2")])
        publish(conn, bid)
        db.update_row(conn, db.get_rows(conn, bid)[0]["id"],
                      status=db.SAVED, remote_id="10")
        counts = db.row_counts(conn, bid)
        assert counts[db.SAVED] == 1
        assert counts[db.PENDING] == 1
