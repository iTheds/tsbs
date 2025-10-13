#!/usr/bin/env bash
set -euo pipefail

# 通用参数
QUERY_COUNT="${QUERY_COUNT:-1000}"
WORKERS="${WORKERS:-8}"
INFLUX_URLS="${INFLUX_URLS:-http://localhost:8086}"
INFLUX_DB="${INFLUX_DB:-tsbs_devops}"
QUESTDB_URLS="${QUESTDB_URLS:-http://localhost:9000}"
PRINT_INTERVAL="${PRINT_INTERVAL:-100}"
DRY_RUN=0
IGNORE_ERROR=0
PARALLEL=1
ONLY_GENERATE=0
DBS_RAW="influx"
TYPES_FILE_INFLUX=""
TYPES_FILE_QUESTDB=""
TYPES_FILE_TIMESCALE=""
TYPES_FILE_TDENGINE=""
TS_DB_NAME="${TS_DB_NAME:-${PGDATABASE:-$INFLUX_DB}}"

# TDengine 连接参数
TDENGINE_HOST="${TDENGINE_HOST:-127.0.0.1}"
TDENGINE_PORT="${TDENGINE_PORT:-6030}"
TDENGINE_USER="${TDENGINE_USER:-root}"
TDENGINE_PASS="${TDENGINE_PASS:-taosdata}"
TDENGINE_DB="${TDENGINE_DB:-$INFLUX_DB}"

# ============== IOT 查询类型默认集 ==============
# 下列名称是社区常见 iot 场景的 query types，若与你的 tsbs 分支不一致，请据实际修改。
QUERY_TYPES_INFLUX_DEFAULT=(
cpu-max-all-1
cpu-max-all-8
cpu-max-all-32-24
high-cpu-1
high-cpu-all
double-groupby-1
double-groupby-5
double-groupby-all
single-groupby-1-1-1
single-groupby-1-1-12
single-groupby-5-1-1
single-groupby-5-1-12
single-groupby-1-8-1
single-groupby-5-8-1
groupby-orderby-limit
lastpoint
)

QUERY_TYPES_QUESTDB_DEFAULT=(
high-cpu-1
high-cpu-all
single-groupby-1-1-1
single-groupby-1-1-12
single-groupby-5-1-1
single-groupby-5-1-12
single-groupby-1-8-1
single-groupby-5-8-1
groupby-orderby-limit
lastpoint
)

QUERY_TYPES_TIMESCALED_DEFAULT=(
cpu-max-all-1
cpu-max-all-8
cpu-max-all-32-24
high-cpu-1
high-cpu-all
double-groupby-1
double-groupby-5
double-groupby-all
single-groupby-1-1-1
single-groupby-1-1-12
single-groupby-5-1-1
single-groupby-5-1-12
single-groupby-1-8-1
single-groupby-5-8-1
groupby-orderby-limit
lastpoint
)
# TDengine 默认查询类型（这里与 Influx/Timescale 保持一致，可自行删减）
QUERY_TYPES_TDENGINE_DEFAULT=(
cpu-max-all-1
cpu-max-all-8
cpu-max-all-32-24
high-cpu-1
high-cpu-all
double-groupby-1
double-groupby-5
double-groupby-all
single-groupby-1-1-1
single-groupby-1-1-12
single-groupby-5-1-1
single-groupby-5-1-12
single-groupby-1-8-1
single-groupby-5-8-1
groupby-orderby-limit
lastpoint
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --ignore-error) IGNORE_ERROR=1; shift ;;
    --parallel) PARALLEL="${2:-1}"; shift 2 ;;
    --no-run) ONLY_GENERATE=1; shift ;;
    --dbs) DBS_RAW="$2"; shift 2 ;;
    --types-file-influx) TYPES_FILE_INFLUX="$2"; shift 2 ;;
    --types-file-questdb) TYPES_FILE_QUESTDB="$2"; shift 2 ;;
    --types-file-timescaledb) TYPES_FILE_TIMESCALE="$2"; shift 2 ;;
    --types-file-tdengine) TYPES_FILE_TDENGINE="$2"; shift 2 ;;
    -h|--help) echo "Usage: ..."; exit 0 ;;
    *) echo "未知参数: $1"; exit 1 ;;
  esac
done

IFS=',' read -r -a DBS_ARRAY <<<"$DBS_RAW"
for ((i=0;i<${#DBS_ARRAY[@]};i++)); do DBS_ARRAY[$i]="${DBS_ARRAY[$i],,}"; done

need_var() { local name="$1"; [[ -n "${!name:-}" ]] || { echo "错误: 需要 $name"; exit 1; }; }
for v in SEED SCALE START END QUERY_DIR RESULT_DIR; do need_var "$v"; done

mkdir -p "$QUERY_DIR" "$RESULT_DIR"
TMP_LOG_DIR="$RESULT_DIR/tmp_logs"; mkdir -p "$TMP_LOG_DIR"

GEN_BIN=./bin/tsbs_generate_queries
RUN_BIN_INFLUX=./bin/tsbs_run_queries_influx
RUN_BIN_QUEST=./bin/tsbs_run_queries_questdb
RUN_BIN_TIMESCALE=./bin/tsbs_run_queries_timescaledb
RUN_BIN_TDENGINE=./bin/tsbs_run_queries_tdengine
for b in "$GEN_BIN"; do [[ -x $b ]] || { echo "缺少可执行: $b"; exit 1; }; done

valid_backend() { case "$1" in influx|questdb|timescaledb|tdengine) return 0;; *) return 1;; esac; }
for db in "${DBS_ARRAY[@]}"; do valid_backend "$db" || { echo "不支持后端: $db"; exit 1; }; done

load_types_for_backend() {
  local backend="$1" types_file="$2" default_array_name="$3" out_array_name="$4"
  local backend_upper=${backend^^}
  local primary_var="QUERY_TYPES_${backend_upper}"
  local alt_var=""
  [[ "$backend" == "timescaledb" ]] && alt_var="QUERY_TYPES_TIMESCALEDB"
  local -a result=()
  if [[ -n "$types_file" ]]; then
    mapfile -t result < <(grep -v '^[[:space:]]*$' "$types_file")
  else
    local desc
    desc="$(declare -p "$primary_var" 2>/dev/null || true)"
    if [[ $desc == "declare -a"* ]]; then
      eval "result=(\"\${${primary_var}[@]}\")"
    elif [[ -n "$alt_var" ]]; then
      desc="$(declare -p "$alt_var" 2>/dev/null || true)"
      if [[ $desc == "declare -a"* ]]; then
        eval "result=(\"\${${alt_var}[@]}\")"
      elif [[ -n "${!alt_var:-}" ]]; then result=(${!alt_var}); fi
    fi
    if [[ ${#result[@]} -eq 0 ]]; then
      if [[ -n "${!primary_var:-}" && ! $desc == "declare -a"* ]]; then
        result=(${!primary_var})
      else
        eval "result=(\"\${${default_array_name}[@]}\")"
      fi
    fi
  fi
  declare -A seen=(); local -a dedup=()
  for t in "${result[@]}"; do [[ -z "$t" ]] && continue; [[ -n "${seen[$t]:-}" ]] && continue; dedup+=("$t"); seen[$t]=1; done
  eval "$out_array_name=(\"\${dedup[@]}\")"
}

load_types_for_backend "influx"      "$TYPES_FILE_INFLUX"     "QUERY_TYPES_INFLUX_DEFAULT"     "QT_INFLUX"
load_types_for_backend "questdb"     "$TYPES_FILE_QUESTDB"    "QUERY_TYPES_QUESTDB_DEFAULT"    "QT_QUESTDB"
load_types_for_backend "timescaledb" "$TYPES_FILE_TIMESCALE"  "QUERY_TYPES_TIMESCALED_DEFAULT" "QT_TIMESCALE"
load_types_for_backend "tdengine"    "$TYPES_FILE_TDENGINE"   "QUERY_TYPES_TDENGINE_DEFAULT"   "QT_TDENGINE"

show_types() { local name="$1" arr="$2"; eval "local c=\${#${arr}[@]}"; eval "local items=(\"\${${arr}[@]}\")"; echo "[INFO] $name query types ($c): ${items[*]:-<none>}"; }
show_types influx QT_INFLUX
show_types questdb QT_QUESTDB
show_types timescaledb QT_TIMESCALE
show_types tdengine QT_TDENGINE

for db in "${DBS_ARRAY[@]}"; do
  case "$db" in
    influx)      [[ ${#QT_INFLUX[@]}    -gt 0 ]] || { echo "influx 无查询类型"; exit 1; } ;;
    questdb)     [[ ${#QT_QUESTDB[@]}   -gt 0 ]] || { echo "questdb 无查询类型"; exit 1; } ;;
    timescaledb) [[ ${#QT_TIMESCALE[@]} -gt 0 ]] || { echo "timescaledb 无查询类型"; exit 1; } ;;
    tdengine)    [[ ${#QT_TDENGINE[@]}  -gt 0 ]] || { echo "tdengine 无查询类型"; exit 1; } ;;
  esac
done

run_and_capture() {
  local header="$1"; shift
  local result_file="$1"; shift
  local backend="$1"; shift
  local cmd=( "$@" )
  echo "[RUN][$backend] $header"
  if [[ $DRY_RUN -eq 1 ]]; then
    printf '[DRY-RUN] CMD: %q ' "${cmd[@]}"; echo
    {
      echo "=== $backend :: $header ==="
      echo "Command: ${cmd[*]}"
      echo "(Dry-run, 未执行)"
      echo
    } >> "$result_file"
    return 0
  fi
  {
    echo "=== $backend :: $header ==="
    echo "Start: $(date -u +%FT%TZ)"
    echo "Command: ${cmd[*]}"
    "${cmd[@]}"
    local st=$?
    echo "Exit: $st"
    echo "End: $(date -u +%FT%TZ)"
    echo
    return $st
  } >> "$result_file" 2>&1
  return $?
}

parallel_generate() {
  local backend="$1" func="$2" arr_name="$3"
  eval "local arr=(\"\${${arr_name}[@]}\")"
  echo "[INFO][$backend] 生成 ${#arr[@]} 个文件 (并行=$PARALLEL)"
  if [[ $PARALLEL -le 1 ]]; then
    for qt in "${arr[@]}"; do
      "$func" "$qt" || {
        if [[ $IGNORE_ERROR -eq 1 ]]; then echo "[WARN][$backend] 生成失败忽略: $qt"; else echo "[FATAL][$backend] 生成失败: $qt"; exit 1; fi
      }
    done
  else
    local fifo; fifo=$(mktemp -u); mkfifo "$fifo"; exec 9<>"$fifo"; rm -f "$fifo"
    for ((i=0;i<PARALLEL;i++)); do printf '.' >&9; done
    declare -a pids=()
    eval "local arr_len=\${#${arr_name}[@]}"
    for qt in "${arr[@]}"; do
      read -r -n1 _ <&9
      {
        "$func" "$qt" || [[ $IGNORE_ERROR -eq 1 ]] || echo ABORT > "$TMP_LOG_DIR/${backend}_abort"
        printf '.' >&9
      } &
      pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done
    [[ -f "$TMP_LOG_DIR/${backend}_abort" ]] && { echo "[FATAL][$backend] 生成阶段中止"; exit 1; }
  fi
  echo "[INFO][$backend] 生成完成"
}

generate_influx_one() {
  local qtype="$1"
  local outfile="$QUERY_DIR/queries_influx_${qtype}.gz"
  local cmd=("$GEN_BIN" --use-case=devops --seed="$SEED" --scale="$SCALE" \
    --timestamp-start="$START" --timestamp-end="$END" \
    --queries="$QUERY_COUNT" --format=influx --query-type="$qtype" --file="$outfile")
  echo "[GENERATE][influx] $qtype -> $outfile"
  [[ $DRY_RUN -eq 1 ]] && { printf '[DRY-RUN] CMD: %q ' "${cmd[@]}"; echo; return 0; }
  "${cmd[@]}"
}
run_influx_all() {
  local result_file="$RESULT_DIR/queries_influx.txt"
  : > "$result_file"
  {
    echo "=============================="
    echo "Backend: influx"
    echo "Start: $(date -u +%FT%TZ)"
    echo "Note: 若只看到这一段说明没有任何查询执行成功。"
    echo "=============================="
    echo
  } >> "$result_file"
  for qt in "${QT_INFLUX[@]}"; do
    local infile="$QUERY_DIR/queries_influx_${qt}.gz"
    [[ -f "$infile" ]] || {
      echo "[ERROR][influx] 缺少文件: $infile"
      [[ $IGNORE_ERROR -eq 1 ]] && continue || exit 1
    }
    local cmd=("$RUN_BIN_INFLUX" --urls="$INFLUX_URLS" --db-name="$INFLUX_DB" --workers="$WORKERS" --file="$infile")
    run_and_capture "QueryType=$qt" "$result_file" "influx" "${cmd[@]}" || {
      if [[ $IGNORE_ERROR -eq 1 ]]; then echo "[WARN][influx] 执行失败忽略: $qt"; else echo "[FATAL][influx] 执行失败: $qt"; exit 1; fi
    }
  done
  echo "[INFO][influx] 执行完成 -> $result_file"
}

generate_questdb_one() {
  local qtype="$1"
  local outfile="$QUERY_DIR/queries_questdb_${qtype}.gz"
  local cmd=("$GEN_BIN" --use-case=devops --seed="$SEED" --scale="$SCALE" \
    --timestamp-start="$START" --timestamp-end="$END" \
    --queries="$QUERY_COUNT" --format=questdb --query-type="$qtype" --file="$outfile")
  echo "[GENERATE][questdb] $qtype -> $outfile"
  [[ $DRY_RUN -eq 1 ]] && { printf '[DRY-RUN] CMD: %q ' "${cmd[@]}"; echo; return 0; }
  "${cmd[@]}"
}
run_questdb_all() {
  local result_file="$RESULT_DIR/queries_questdb.txt"
  : > "$result_file"
  {
    echo "=============================="
    echo "Backend: questdb"
    echo "Start: $(date -u +%FT%TZ)"
    echo "=============================="
    echo
  } >> "$result_file"
  for qt in "${QT_QUESTDB[@]}"; do
    local infile="$QUERY_DIR/queries_questdb_${qt}.gz"
    [[ -f "$infile" ]] || { echo "[ERROR][questdb] 缺少文件: $infile"; [[ $IGNORE_ERROR -eq 1 ]] && continue || exit 1; }
    local cmd=("$RUN_BIN_QUEST" --urls="$QUESTDB_URLS" --workers="$WORKERS" --file="$infile" --print-interval="$PRINT_INTERVAL")
    run_and_capture "QueryType=$qt" "$result_file" "questdb" "${cmd[@]}" || {
      if [[ $IGNORE_ERROR -eq 1 ]]; then echo "[WARN][questdb] 执行失败忽略: $qt"; else echo "[FATAL][questdb] 执行失败: $qt"; exit 1; fi
    }
  done
  echo "[INFO][questdb] 执行完成 -> $result_file"
}

generate_timescaledb_one() {
  local qtype="$1"
  local outfile="$QUERY_DIR/queries_timescaledb_${qtype}.gz"
  local cmd=("$GEN_BIN" --use-case=devops --seed="$SEED" --scale="$SCALE" \
    --timestamp-start="$START" --timestamp-end="$END" \
    --queries="$QUERY_COUNT" --format=timescaledb --query-type="$qtype" --file="$outfile")
  echo "[GENERATE][timescaledb] $qtype -> $outfile"
  [[ $DRY_RUN -eq 1 ]] && { printf '[DRY-RUN] CMD: %q ' "${cmd[@]}"; echo; return 0; }
  "${cmd[@]}"
}
run_timescaledb_all() {
  local result_file="$RESULT_DIR/queries_timescaledb.txt"
  : > "$result_file"
  {
    echo "=============================="
    echo "Backend: timescaledb"
    echo "Start: $(date -u +%FT%TZ)"
    echo "=============================="
    echo
  } >> "$result_file"
  for v in PGHOST PGPORT PGUSER PGPASSWORD; do [[ -n "${!v:-}" ]] || { echo "错误: 缺少 $v"; exit 1; }; done
  local conn="host=$PGHOST port=$PGPORT user=$PGUSER password=$PGPASSWORD dbname=$TS_DB_NAME sslmode=disable"
  for qt in "${QT_TIMESCALE[@]}"; do
    local infile="$QUERY_DIR/queries_timescaledb_${qt}.gz"
    [[ -f "$infile" ]] || { echo "[ERROR][timescaledb] 缺少文件: $infile"; [[ $IGNORE_ERROR -eq 1 ]] && continue || exit 1; }
    local cmd=("$RUN_BIN_TIMESCALE" --postgres="$conn" --workers="$WORKERS" --file="$infile")
    run_and_capture "QueryType=$qt" "$result_file" "timescaledb" "${cmd[@]}" || {
      if [[ $IGNORE_ERROR -eq 1 ]]; then echo "[WARN][timescaledb] 执行失败忽略: $qt"; else echo "[FATAL][timescaledb] 执行失败: $qt"; exit 1; fi
    }
  done
  echo "[INFO][timescaledb] 执行完成 -> $result_file"
}

generate_tdengine_one() {
  local qtype="$1"
  local outfile="$QUERY_DIR/queries_tdengine_${qtype}.gz"
  # 注意：根据你给出的示例格式写为 TDengine，如果实际 tsbs 接口要求小写，请改成 --format=tdengine
  local cmd=("$GEN_BIN" --use-case=devops --seed="$SEED" --scale="$SCALE" \
    --timestamp-start="$START" --timestamp-end="$END" \
    --queries="$QUERY_COUNT" --format=TDengine --query-type="$qtype" --file="$outfile")
  echo "[GENERATE][tdengine] $qtype -> $outfile"
  [[ $DRY_RUN -eq 1 ]] && { printf '[DRY-RUN] CMD: %q ' "${cmd[@]}"; echo; return 0; }
  "${cmd[@]}"
}

run_tdengine_all() {
  local result_file="$RESULT_DIR/queries_tdengine.txt"
  : > "$result_file"
  {
    echo "=============================="
    echo "Backend: tdengine"
    echo "Start: $(date -u +%FT%TZ)"
    echo "=============================="
    echo
  } >> "$result_file"

  # 检查必要连接变量
  for v in TDENGINE_HOST TDENGINE_PORT TDENGINE_USER TDENGINE_PASS TDENGINE_DB; do
    [[ -n "${!v:-}" ]] || { echo "错误: 缺少 $v"; exit 1; }
  done

  for qt in "${QT_TDENGINE[@]}"; do
    local infile="$QUERY_DIR/queries_tdengine_${qt}.gz"
    [[ -f "$infile" ]] || { echo "[ERROR][tdengine] 缺少文件: $infile"; [[ $IGNORE_ERROR -eq 1 ]] && continue || exit 1; }
    local cmd=("$RUN_BIN_TDENGINE" \
      --host="$TDENGINE_HOST" \
      --port="$TDENGINE_PORT" \
      --user="$TDENGINE_USER" \
      --pass="$TDENGINE_PASS" \
      --db-name="$TDENGINE_DB" \
      --workers="$WORKERS" \
      --file="$infile")
    run_and_capture "QueryType=$qt" "$result_file" "tdengine" "${cmd[@]}" || {
      if [[ $IGNORE_ERROR -eq 1 ]]; then echo "[WARN][tdengine] 执行失败忽略: $qt"; else echo "[FATAL][tdengine] 执行失败: $qt"; exit 1; fi
    }
  done
  echo "[INFO][tdengine] 执行完成 -> $result_file"
}

for db in "${DBS_ARRAY[@]}"; do
  case "$db" in
    influx)
      [[ -x $RUN_BIN_INFLUX ]] || { [[ $ONLY_GENERATE -eq 1 ]] || { echo "缺少 $RUN_BIN_INFLUX"; exit 1; }; }
      echo "[STAGE][influx] 生成开始"
      parallel_generate influx generate_influx_one QT_INFLUX
      if [[ $ONLY_GENERATE -eq 0 ]]; then
        echo "[STAGE][influx] 执行开始"
        run_influx_all
      else
        echo "[STAGE][influx] 跳过执行 (--no-run)"
      fi
      ;;
    questdb)
      [[ -x $RUN_BIN_QUEST ]] || { [[ $ONLY_GENERATE -eq 1 ]] || { echo "缺少 $RUN_BIN_QUEST"; exit 1; }; }
      echo "[STAGE][questdb] 生成开始"
      parallel_generate questdb generate_questdb_one QT_QUESTDB
      if [[ $ONLY_GENERATE -eq 0 ]]; then
        echo "[STAGE][questdb] 执行开始"
        run_questdb_all
      else
        echo "[STAGE][questdb] 跳过执行 (--no-run)"
      fi
      ;;
    timescaledb)
      [[ -x $RUN_BIN_TIMESCALE ]] || { [[ $ONLY_GENERATE -eq 1 ]] || { echo "缺少 $RUN_BIN_TIMESCALE"; exit 1; }; }
      echo "[STAGE][timescaledb] 生成开始"
      parallel_generate timescaledb generate_timescaledb_one QT_TIMESCALE
      if [[ $ONLY_GENERATE -eq 0 ]]; then
        echo "[STAGE][timescaledb] 执行开始"
        run_timescaledb_all
      else
        echo "[STAGE][timescaledb] 跳过执行 (--no-run)"
      fi
      ;;
    tdengine)
      [[ -x $RUN_BIN_TDENGINE ]] || { [[ $ONLY_GENERATE -eq 1 ]] || { echo "缺少 $RUN_BIN_TDENGINE"; exit 1; }; }
      echo "[STAGE][tdengine] 生成开始"
      parallel_generate tdengine generate_tdengine_one QT_TDENGINE
      if [[ $ONLY_GENERATE -eq 0 ]]; then
        echo "[STAGE][tdengine] 执行开始"
        run_tdengine_all
      else
        echo "[STAGE][tdengine] 跳过执行 (--no-run)"
      fi
      ;;
  esac
done

echo "[DONE] 全部流程结束"
