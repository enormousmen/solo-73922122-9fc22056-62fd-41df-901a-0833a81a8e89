#!/usr/bin/env bash
# 可控演示：两个控制台同时上传同名文件
#
# 剧本：
#   1) 准备两份同名（same-name.csv）但内容不同的 CSV
#   2) 同一时刻分别上传到控制台 A 与控制台 B（共享同一 SQLite 与上传目录）
#   3) 验证：生成两个批次；两份原件各自保留、互不覆盖；
#      批次行与各自原件一一对应（摘要一致）；两批都能正常报送
#
# 前置：docker compose up -d 已启动；依赖 curl、python3。
set -euo pipefail
cd "$(dirname "$0")/.."

A="${CONSOLE_A_URL:-http://localhost:8000}"
B="${CONSOLE_B_URL:-http://localhost:8002}"
L="${LEGACY_URL:-http://localhost:8001}"
NAME="same-name.csv"
WORK="$(mktemp -d)"
mkdir -p "$WORK/a" "$WORK/b"
printf '样本号,项目,结果\nSAME-A,pH,7.1\n' > "$WORK/a/$NAME"
printf '样本号,项目,结果\nSAME-B,pH,8.8\nSAME-B,浊度,4.4\n' > "$WORK/b/$NAME"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { echo "FAIL: $*"; exit 1; }

step "0. 前置检查"
curl -fsS "$A/" >/dev/null || fail "控制台 A 不可达：$A"
curl -fsS "$B/" >/dev/null || fail "控制台 B 不可达：$B"

step "1. 两控制台同时上传同名文件（demo_delay=3s 放大并发窗口）"
curl -fsS -F "file=@$WORK/a/$NAME;filename=$NAME" -F "demo_delay=3" \
  -o /dev/null -w '%{redirect_url}' "$A/batches" > "$WORK/ra.txt" &
PA=$!
curl -fsS -F "file=@$WORK/b/$NAME;filename=$NAME" -F "demo_delay=3" \
  -o /dev/null -w '%{redirect_url}' "$B/batches" > "$WORK/rb.txt" &
PB=$!
wait $PA $PB
BA=$(grep -o '[0-9]*$' "$WORK/ra.txt")
BB=$(grep -o '[0-9]*$' "$WORK/rb.txt")
[ -n "$BA" ] && [ -n "$BB" ] || fail "上传未建批：A='$BA' B='$BB'"
[ "$BA" != "$BB" ] || fail "两控制台返回了同一批次号 $BA"
echo "控制台 A -> 批次 #$BA；控制台 B -> 批次 #$BB"

step "2. 验证两份原件分别保留、互不覆盖"
python3 - "$A" "$B" "$BA" "$BB" "$WORK/a/$NAME" "$WORK/b/$NAME" <<'PY'
import hashlib, json, sys, urllib.request

A, B, ba, bb, fa, fb = sys.argv[1:7]

def api(base, bid):
    with urllib.request.urlopen(f"{base}/api/batches/{bid}", timeout=15) as r:
        return json.load(r)

def download(base, bid):
    with urllib.request.urlopen(f"{base}/batches/{bid}/original",
                                timeout=15) as r:
        return r.read()

da, db_ = api(A, ba), api(B, bb)
sa, sb = da["batch"]["stored_name"], db_["batch"]["stored_name"]
assert sa != sb, f"原件存储名相同：{sa}"
print(f"OK：原件分别保留：{sa} / {sb}")

oa, ob = download(A, ba), download(B, bb)
ha, hb = hashlib.sha256(oa).hexdigest(), hashlib.sha256(ob).hexdigest()
assert oa != ob, "两份原件内容相同（被覆盖）"
assert ha == hashlib.sha256(open(fa, "rb").read()).hexdigest(), "A 原件被覆盖"
assert hb == hashlib.sha256(open(fb, "rb").read()).hexdigest(), "B 原件被覆盖"
print("OK：两份原件内容与各自上传一致（互不覆盖）")

assert da["batch"]["sha256"] == ha and db_["batch"]["sha256"] == hb
print("OK：库中摘要与原件一致（批次行与原件对应）")
assert len(da["rows"]) == 1 and da["rows"][0]["sample_no"] == "SAME-A"
assert len(db_["rows"]) == 2 and db_["rows"][0]["sample_no"] == "SAME-B"
print("OK：批次行来自各自原件（A 1 行 / B 2 行）")
PY

step "3. 两个批次都能正常报送（共享库全局单任务，先后执行）"
post_start() { curl -fsS -o /dev/null -X POST "$1/batches/$2/start" -d resume=0; }
wait_done() {
  for _ in $(seq 1 90); do
    st=$(python3 -c "
import json,sys,urllib.request
with urllib.request.urlopen('$1/api/batches/$2', timeout=15) as r:
    d = json.load(r)
print('busy' if d['busy'] else d['batch']['status'])")
    [ "$st" = "DONE" ] && return 0
    sleep 2
  done
  return 1
}
post_start "$A" "$BA"
wait_done "$A" "$BA" || fail "批次 #$BA 报送未完成"
echo "批次 #$BA 报送 DONE"
post_start "$B" "$BB"
wait_done "$B" "$BB" || fail "批次 #$BB 报送未完成"
echo "批次 #$BB 报送 DONE"
python3 - "$L" <<'PY'
import json, sys, urllib.request
with urllib.request.urlopen(sys.argv[1] + "/records.json", timeout=15) as r:
    rows = json.load(r)
keys = {(r["sample_no"], r["project"]) for r in rows}
assert ("SAME-A", "pH") in keys and ("SAME-B", "pH") in keys \
    and ("SAME-B", "浊度") in keys, f"旧站缺少记录：{keys}"
print("OK：旧站已收到两批共 3 条记录")
PY

step "PASS：同名并发上传 —— 各自原件分别保留，批次行与原件始终对应"
