#!/usr/bin/env bash
# 可控演示：建批中途进程退出
#
# 剧本：
#   1) 控制台 B 先建一个正常批次（验证"成功批次重启保留"）
#   2) 控制台 A 以上传延时 60s 慢速建批，建批进行中确认两控制台都看不到该批
#   3) docker compose kill 掉 A（模拟进程中断）——半批仍不可见、不可报送
#   4) 重启 A —— 启动清理回收未发布原件与草稿，已发布批次完好保留
#
# 前置：docker compose up -d 已启动；依赖 curl、docker compose、python3（容器内）。
set -euo pipefail
cd "$(dirname "$0")/.."

A="${CONSOLE_A_URL:-http://localhost:8000}"
B="${CONSOLE_B_URL:-http://localhost:8002}"
NAME="demo-mid-exit-$$.csv"
WORKDIR_DEMO="$(mktemp -d)"
CSV="$WORKDIR_DEMO/$NAME"
printf '样本号,项目,结果\nMID-1,pH,7.7\nMID-2,pH,6.6\n' > "$CSV"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { echo "FAIL: $*"; exit 1; }

step "0. 前置检查：两个控制台都在线"
curl -fsS "$A/" >/dev/null || fail "控制台 A 不可达：$A"
curl -fsS "$B/" >/dev/null || fail "控制台 B 不可达：$B"

step "1. 控制台 B 建一个正常批次（稍后验证重启保留）"
KEEP_BID=$(curl -fsS -F "file=@$CSV;filename=keep-$NAME" -F "demo_delay=0" \
  -o /dev/null -w '%{redirect_url}' "$B/batches" | grep -o '[0-9]*$')
[ -n "$KEEP_BID" ] || fail "正常建批失败"
echo "已发布批次 #$KEEP_BID（控制台 B）"

step "2. 控制台 A 慢速建批（demo_delay=60s），进行中两控制台均不可见"
curl -fsS -F "file=@$CSV;filename=$NAME" -F "demo_delay=60" \
  -o /dev/null "$A/batches" &
UP_PID=$!
sleep 5
curl -fsS "$A/" | grep -q "$NAME" && fail "建批进行中批次已出现在 A 列表"
curl -fsS "$B/" | grep -q "$NAME" && fail "建批进行中批次已出现在 B 列表"
echo "OK：建批进行中，A、B 批次列表均无此批"
if docker compose exec -T reporter-a sh -c \
    "ls /data/uploads/ | grep -q '${NAME%.csv}'"; then
  echo "OK：原件已落入共享上传目录（批次仍未发布）"
else
  fail "建批进行中原件未落盘"
fi

step "3. 模拟进程中断：docker compose kill reporter-a"
docker compose kill reporter-a
wait $UP_PID 2>/dev/null || true
sleep 2
curl -fsS "$B/" | grep -q "$NAME" && fail "进程退出后半批在 B 变为可见"
echo "OK：进程退出后，半批在另一控制台仍不可见、不可报送"

step "4. 重启控制台 A"
docker compose up -d reporter-a >/dev/null
for _ in $(seq 1 40); do
  curl -fsS "$A/" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$A/" >/dev/null || fail "控制台 A 重启后不可达"

step "5. 验证：半批被清理，成功批次保留"
sleep 5   # 启动清理已在重启时执行，留一点余量
curl -fsS "$A/" | grep -q "$NAME" && fail "重启后半批出现在 A 列表"
curl -fsS "$B/" | grep -q "$NAME" && fail "重启后半批出现在 B 列表"
echo "OK：半批在两控制台均不可见"
if docker compose exec -T reporter-a sh -c \
    "ls /data/uploads/ /data/uploads/incoming/ 2>/dev/null | grep '${NAME%.csv}'"; then
  fail "未发布原件未被清理"
fi
echo "OK：未发布原件（含 .part 残片）已清理"
docker compose exec -T reporter-a python -c "
import sqlite3
conn = sqlite3.connect('/data/reporter.db')
n = conn.execute(\"SELECT COUNT(*) FROM batches WHERE status='CREATING'\").fetchone()[0]
assert n == 0, f'仍有 {n} 个草稿批残留'
print('OK：共享库中无草稿批残留')
"
curl -fsS "$A/api/batches/$KEEP_BID" | grep -q '"id"' \
  || fail "重启前已发布的批次 #$KEEP_BID 丢失"
echo "OK：重启前已发布的批次 #$KEEP_BID 完好保留"

step "PASS：建批中途退出 —— 半批不可见不可报送，重启清理未发布原件，成功批次保留"
