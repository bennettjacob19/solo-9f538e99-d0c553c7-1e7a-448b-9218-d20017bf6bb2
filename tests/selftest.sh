#!/usr/bin/env bash
# 端到端自测：在本机用便携 PostgreSQL + uvicorn 跑全链路验证。
# 用法：bash tests/selftest.sh
# 不依赖 Docker；Docker Compose 场景的启动方式见 README。
set -u
cd "$(dirname "$0")/.."
REPO=$PWD

# ------------------------------------------------------------- 环境定位
PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-/tmp/pgdeb/client/usr/lib/postgresql/15/bin}
export PATH="$PGCLIENT:$PGBIN:$PATH"
PGDATA=${PGDATA:-/tmp/selftest-pgdata}
PGPORT=55433
PGSOCK=/tmp/selftest-pgsock
SQLDIR=/tmp/selftest-sqlite
APPPORT=8011
RECPORT=8012
RECDB=/tmp/selftest-receiver
BASE="http://127.0.0.1:$APPPORT"
REC="http://127.0.0.1:$RECPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api()  { curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
wait_phase(){ # wait_phase <expected> <timeout_s>
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
start_app(){ # start_app <sqlite_dir> [init_sleep_ms]
  local dir=$1 sleep_ms=${2:-0}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$dir"
  PG_DSN="$DSN" SQLITE_PATH="$dir/history.db" CAPTURE_INIT_SLEEP_MS="$sleep_ms" \
  NOTIFIER_URL="$REC/notify" NOTIFIER_BACKOFF_S=1 NOTIFIER_POLL_S=0.5 \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
start_receiver(){ # 固定示例接收端（独立进程，自带 SQLite 持久化）
  stop_receiver
  rm -rf "$RECDB"; mkdir -p "$RECDB"
  RECEIVER_HOST=127.0.0.1 RECEIVER_PORT="$RECPORT" RECEIVER_DB="$RECDB/receiver.db" \
    setsid nohup python3 -m app.receiver.server > /tmp/selftest-receiver.log 2>&1 < /dev/null &
  for _ in $(seq 1 30); do
    curl -s -m 1 "$REC/healthz" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "receiver failed to start:"; cat /tmp/selftest-receiver.log; return 1
}
stop_receiver(){
  local pid
  pid=$(ps -eo pid,args | awk '/app.receiver.server/ && !/awk/ {print $1}')
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    ps -eo pid,args | grep -q '[a]pp.receiver.server' || return 0
    sleep 0.5
  done
}
# 等待某 seq 通知进入指定状态
wait_notif_status(){ # wait_notif_status <seq> <status> <timeout_s>
  local want_seq=$1 want_status=$2 deadline=$((SECONDS+$3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local st
    st=$(api /api/notifications | jq -r ".notifications[]|select(.seq==$want_seq)|.status" | head -1)
    [ "$st" = "$want_status" ] && return 0
    sleep 0.5
  done
  return 1
}
_wait_app_ready(){ # _wait_app_ready <expected_pid>
  local expected=$1
  for _ in $(seq 1 40); do
    if ! kill -0 "$expected" 2>/dev/null; then
      echo "new app exited early:"; cat /tmp/selftest-app.log; return 1
    fi
    if curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1; then
      # 确认响应者确实是新进程而不是占用端口的旧实例
      [ "$(app_pid)" = "$expected" ] && return 0
    fi
    sleep 0.5
  done
  echo "app failed to become ready:"; cat /tmp/selftest-app.log; return 1
}
stop_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    app_pid | grep -q . || return 0
    sleep 0.5
  done
}
force_kill_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
  sleep 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}'; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/selftest-pg.log "$@"; }

cleanup(){
  stop_app || true
  stop_receiver || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备源库
echo "=== 0. 准备全新源 PostgreSQL（wal_level=logical）==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc 'select count(*) from artifacts')" \
      "3" "源表初始化 3 行示例数据"
start_receiver && ok "固定示例接收端已启动 :$RECPORT" || bad "接收端启动失败"

# ------------------------------------------------------------- 1. 初始化未完成不开放历史（503）
echo "=== 1. 初始化窗口历史接口 503 ==="
start_app "$SQLDIR" 8000
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/points")
check "$code" "503" "INITIALIZING 期间 /api/points 返回 503"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=0/1")
check "$code" "503" "INITIALIZING 期间 /api/directory 返回 503"
check "$(api /api/progress | jq -r .phase)" "INITIALIZING" "进度接口暴露 INITIALIZING"
wait_phase STREAMING 15 && ok "初始化完成进入 STREAMING" || bad "初始化超时"
base_lsn=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
[ -n "$base_lsn" ] && ok "基线提交点存在: $base_lsn" || bad "缺少基线提交点"
check "$(api "/api/directory?point=$base_lsn" | jq '.artifacts|length')" "3" "基线目录 3 行"

# ------------------------------------------------------------- 2. 基线前开始、基线后提交的事务不漏
echo "=== 2. 跨基线事务（初始化窗口开启，基线后提交）==="
stop_app; sleep 1
rm -rf "$SQLDIR"
# 同步等待旧槽完全删除，避免新实例误走 resume 分支
for _ in $(seq 1 20); do
  psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots" 2>/dev/null || true
  n=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select count(*) from pg_replication_slots")
  [ "$n" = "0" ] && break
  sleep 0.5
done
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from pg_replication_slots")" "0" "旧复制槽已彻底删除，确保走全新基线"
# 清理历史失败运行可能残留的测试行（应用已停，不会再写入），
# 把源目录恢复到与 init/01_schema.sql 一致的 3 行初始状态
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "delete from artifacts where artifact_no not in (1001,1002,1003)" >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "insert into artifacts values
   (1001,'billing-gateway','1.4.2','a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'),
   (1002,'risk-engine','2.7.0','0f1e2d3c4b5a69788796a5b4c3d2e1f009182736455463728190aabbccddeeff'),
   (1003,'notify-hub','0.9.3','ffeeddccbbaa9988776655443322110099887766554433221100998877665544')
   on conflict do nothing" >/dev/null
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "select count(*) from artifacts")" \
      "3" "源目录已重置为 3 行初始状态"
# 用应用内置测试钩子确定性构造"基线前开始、基线后提交"的事务：
# 应用在建槽/基线前打开源事务写入 2001 并持有，基线落盘后才提交。
start_app_hook(){
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" \
    TEST_INFLIGHT_ARTIFACT_NO=2001 TEST_INFLIGHT_SLEEP_S=3 \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
start_app_hook || bad "带钩子的应用启动失败"
wait_phase STREAMING 20 || bad "钩子场景初始化超时"
grep -q "TEST hook: inflight transaction opened" /tmp/selftest-app.log \
  && ok "测试钩子事务已在基线前打开" \
  || bad "测试钩子未生效: $(grep -c 'TEST hook' /tmp/selftest-app.log)"
# 基线快照里不允许出现 2001（事务尚未提交）
new_base=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "0" "跨基线事务未混入基线快照（不重）"
# 等钩子事务在基线后提交并被复制流捕获
for _ in $(seq 1 30); do
  n=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
  [ "$n" = "1" ] && break
  sleep 1
done
found=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
check "$found" "1" "基线前开始、基线后提交的事务被复制流补捕获，无遗漏"
sleep 2
inflight_lsn=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/directory?point=$inflight_lsn" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "1" "跨基线事务在其提交点目录中可见"

# ------------------------------------------------------------- 3. 多行事务整体可见 + 删除后重用编号
echo "=== 3. 多行事务原子性与编号重用 ==="
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":3001,\"service_name\":\"g1\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"insert\",\"artifact_no\":3002,\"service_name\":\"g2\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":1002}
 ]}" >/dev/null
sleep 2
points=$(api /api/points | jq '[.points[]|select(.kind=="TXN" and .change_count>=3)]|length')
[ "$points" -ge 1 ] && ok "三行操作只产生一个提交点（整体可见）" || bad "多行事务被拆分"
latest=$(api /api/points | jq -r '.points[-1].lsn')
dir3001=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==3001)]|length')
dir1002=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==1002)]|length')
check "$dir3001" "1" "同事务插入的 3001 在提交点可见"
check "$dir1002" "0" "同事务删除的 1002 在提交点消失"
# 重用 1002
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":1002,\"service_name\":\"reborn\",\"version\":\"2\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
chain=$(api /api/history/1002 | jq -r '[.history[].change_type]|join(",")')
case "$chain" in
  *BASELINE*DELETE*INSERT*) ok "删除后重用编号，旧历史完整保留: $chain" ;;
  *) bad "编号重用历史链异常: $chain" ;;
esac

# ------------------------------------------------------------- 4. 失败整组回滚 + 输入校验
echo "=== 4. 回滚与固定结构校验 ==="
code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":9001,\"service_name\":\"ghost\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":999999}
 ]}")
check "$code" "400" "组内失败返回 400"
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from artifacts where artifact_no=9001")" "0" "失败整组回滚，源库无残留"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d '{"operations":[{"op":"insert","artifact_no":1,"service_name":"x","version":"1","sha256":"bad"}]}')
check "$code" "422" "非法 sha256 被 pydantic 拒绝"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=not-a-lsn")
check "$code" "400" "非法 LSN 参数 400"

# ------------------------------------------------------------- 5. 强杀应用后续接：无重复、无丢失、槽位不超前落盘
echo "=== 5. 强杀重启一致性 ==="
before_txns=$(api /api/progress | jq .captured_transactions)
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"7.7.7\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
after_txns=$(api /api/progress | jq .captured_transactions)
[ "$after_txns" -gt "$before_txns" ] && ok "新事务已捕获 ($before_txns -> $after_txns)" || bad "新事务未捕获"
kill -9 "$(app_pid)" 2>/dev/null || true; sleep 2
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
sleep 3
check "$(api /api/progress | jq .captured_transactions)" "$after_txns" "强杀重启后事务数不重复"
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="7.7.7")]|length')" "1" "强杀前最后一次更新仍在"
flush=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last=$(api /api/progress | jq -r .last_commit_lsn)
if [ -n "$flush" ] && [ -n "$last" ]; then
  fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
  lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
  [ "$fnum" -le "$lnum" ] && ok "槽 confirmed_flush_lsn($flush) 不超前已落盘提交点($last)" \
                            || bad "槽位超前落盘位置: $flush > $last"
fi
# 重启后再写一条，验证续接后的流仍然工作
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"8.8.8\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="8.8.8")]|length')" "1" "续接后新变更继续被捕获"

# ------------------------------------------------------------- 6. 源库断连恢复
echo "=== 6. 源库重启后自动重连续接 ==="
pg_ctl_q restart -w >/dev/null
sleep 1
for _ in $(seq 1 20); do
  api /api/progress | jq -e '.phase=="STREAMING"' >/dev/null && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "STREAMING" "源库重启后应用自动恢复 STREAMING"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"9.9.9\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="9.9.9")]|length')" "1" "断连恢复后的变更未丢失"

# ------------------------------------------------------------- 7. 持久化校验
echo "=== 7. 两侧持久化 ==="
[ -f "$SQLDIR/history.db" ] && ok "SQLite 历史库落盘: $SQLDIR/history.db" || bad "SQLite 未落盘"
[ -d "$PGDATA/pg_wal" ] && ok "PostgreSQL 数据目录持久化: $PGDATA" || bad "PG 数据目录缺失"

# ------------------------------------------------- 8. 逐事务可靠通知全链路
echo "=== 8. 事务性发件箱可靠通知 ==="
# 通知 seq 在 1~7 节的事务中已累计；本节动态取当前 seq 边界。
# 重启接收端清空演示账本/队列，使本节断言只针对本节产生的通知。
start_receiver
seq_hi(){ api /api/notifications | jq '[.notifications[].seq] | max // 0'; }
SEQ_BEFORE=$(seq_hi)
S1=$((SEQ_BEFORE + 1)); S2=$((SEQ_BEFORE + 2)); S3=$((SEQ_BEFORE + 3)); S4=$((SEQ_BEFORE + 4))
# 8.0 整笔事务一条通知，按提交顺序 ACK
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":5001,\"service_name\":\"n1\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"insert\",\"artifact_no\":5002,\"service_name\":\"n2\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":1003}
 ]}" >/dev/null
wait_notif_status "$S1" SENT 15 && ok "首笔通知(seq=$S1)按序送达并 ACK" || bad "首笔通知未确认"
n1=$(api /api/notifications | jq ".notifications[]|select(.seq==$S1)")
check "$(echo "$n1" | jq '.payload.changes|length')" "3" "同一源事务的增删改整笔送达（一条通知含 3 变更）"
check "$(curl -s "$REC/receiver/pending" | jq -r '[.pending[].artifact_no]|sort|join(",")')" \
      "1003,5001,5002" "接收端待重建编号与通知一致"

# 8.1 接收端“已处理但响应丢失”：fail-mode 下处理落盘后回 500，发送端重发幂等
curl -s -X POST "$REC/receiver/fail-mode" -H 'Content-Type: application/json' -d '{"status":500}' >/dev/null
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":5003,\"service_name\":\"n3\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
# 紧随其后的第三笔：head-of-line 未确认前不得越过
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":5004,\"service_name\":\"n4\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 4
check "$(api /api/notifications | jq -r ".notifications[]|select(.seq==$S2)|.status")" \
      "FAILED" "响应丢失后通知(seq=$S2)保持未确认（FAILED 退避重试）"
check "$(api /api/notifications | jq -r ".notifications[]|select(.seq==$S3)|.attempts")" \
      "0" "后续事务未越过未确认通知（head-of-line，attempts=0）"
# 失败模式下接收端已把 S2 处理落盘（账本恰好一行），但发送端始终未获 ACK
check "$(curl -s "$REC/receiver/processed" | jq "[.processed[]|select(.notification_seq==$S2)]|length")" \
      "1" "seq=$S2 在接收端已处理落盘（账本一行），重发不新增账本行"
check "$(curl -s "$REC/receiver/pending" | jq "[.pending[]|select(.artifact_no==5003)]|length")" \
      "1" "失败模式下 5003 仅入队一次"
# 恢复接收端：退避后重发 S2，接收端走 duplicate 分支不重复入队；随后 S3 才送达
curl -s -X POST "$REC/receiver/fail-mode" -H 'Content-Type: application/json' -d '{"status":0}' >/dev/null
wait_notif_status "$S2" SENT 30 && ok "响应恢复后 seq=$S2 重发获 ACK" || bad "seq=$S2 重发未收敛"
wait_notif_status "$S3" SENT 15 && ok "seq=$S3 在 seq=$S2 确认后按序送达" || bad "seq=$S3 未按序送达"
dup2=$(curl -s "$REC/receiver/processed" | jq -r ".processed[]|select(.notification_seq==$S2)|.duplicate_count")
[ "$dup2" -ge 1 ] && ok "重发被识别为重复（duplicate_count=$dup2），只安排一次重建" \
                      || bad "重发幂等计数异常: $dup2"
check "$(curl -s "$REC/receiver/pending" | jq '[.pending[]|select(.artifact_no==5003)]|length')" \
      "1" "5003 在待重建队列只出现一次（无重复重建安排）"
check "$(curl -s "$REC/receiver/pending" | jq '[.pending[]|select(.artifact_no==5004)]|length')" \
      "1" "5004 在队首恢复后入队"

# 8.2 接收端暂不可用 + 手动重试 + 故障不影响历史查询
stop_receiver
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":5005,\"service_name\":\"n5\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 3
nid4=$(api /api/notifications | jq ".notifications[]|select(.seq==$S4)|.id")
b=$(api /api/notifications | jq ".notifications[]|select(.seq==$S4)|.attempts")
curl -s -X POST "$BASE/api/notifications/$nid4/retry" >/dev/null
sleep 1
a=$(api /api/notifications | jq ".notifications[]|select(.seq==$S4)|.attempts")
[ "$a" -gt "$b" ] && ok "手动重试立即打断退避再投 ($b -> $a)" || bad "手动重试未立即触发"
pts_before=$(api /api/points | jq '.points|length')
[ "$pts_before" -gt 0 ] && ok "投递故障期间历史查询接口仍可用（提交点 $pts_before 个）" || bad "历史查询受投递故障影响"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/receiver/pending")
check "$code" "503" "接收端宕机时代理接口 503，不拖垮主服务"

# 8.3 接收端重启（状态持久化）+ 发送端强杀重启（未确认续投、重放不造第二笔）
start_receiver
before_total=$(api /api/notifications | jq '.stats.total')
force_kill_app
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
wait_notif_status "$S4" SENT 30 && ok "发送端重启后未确认通知(seq=$S4)自动续投并 ACK" || bad "重启后续投失败"
check "$(api /api/notifications | jq '.stats.total')" "$before_total" \
      "捕获重放未制造第二笔通知（通知总数不变=$before_total）"
check "$(api /api/progress | jq '.notifications.PENDING + .notifications.FAILED')" \
      "0" "重启后无残留未确认通知"
# 重启后再写一笔，链路仍工作
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":5001,\"service_name\":\"n1\",\"version\":\"2\",\"sha256\":\"$SHA64\"}]}" >/dev/null
next_seq=$((before_total + 1))
wait_notif_status "$next_seq" SENT 15 && ok "重启后新事务通知 seq=$next_seq 正常送达" || bad "重启后通知链路中断"
check "$(curl -s "$REC/receiver/pending" | jq '[.pending[]|select(.artifact_no==5001)]|length')" \
      "1" "同编号多次变更在待重建队列仍只占一行"
# 重建演示：队列转历史
drained=$(curl -s -X POST "$BASE/api/receiver/drain" | jq '.rebuilt|length')
[ "$drained" -ge 1 ] && ok "接收端执行重建：$drained 个编号转入历史并清空队列" || bad "重建执行失败"
check "$(curl -s "$BASE/api/receiver/pending" | jq '.pending|length')" "0" "重建后待重建队列为空"
[ -f "$RECDB/receiver.db" ] && ok "接收端状态 SQLite 持久化: $RECDB/receiver.db" || bad "接收端未持久化"

echo
echo "================ 自测结果: $PASS passed / $FAIL failed ================"
[ "$FAIL" -eq 0 ]
