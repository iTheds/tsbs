#!/usr/bin/env bash
set -euo pipefail

# ============== 配置参数 ==============
WORKERS_ARRAY=()
DB_TYPE=""
BATCH_SIZE="${BATCH_SIZE:-10000}"
DATA_DIR="${DATA_DIR:-./data_iot}"
RESULT_DIR_BASE="${RESULT_DIR:-./results_iot}"
SKIP_LOAD=0
SKIP_QUERY=0
FIRST_RUN=1

# 数据库连接参数
INFLUX_URL="${INFLUX_URL:-http://127.0.0.1:8086}"
QUESTDB_ILP_BIND="${QUESTDB_ILP_BIND:-127.0.0.1:9009}"
QUESTDB_URL="${QUESTDB_URL:-http://127.0.0.1:9000/}"
TDENGINE_HOST="${TDENGINE_HOST:-127.0.0.1}"
TDENGINE_PORT="${TDENGINE_PORT:-6030}"
TDENGINE_USER="${TDENGINE_USER:-root}"
TDENGINE_PASS="${TDENGINE_PASS:-taosdata}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-tsbs}"

# 查询参数
SEED="${SEED:-12345}"
SCALE="${SCALE:-4000}"
START="${START:-2016-01-01T00:00:00Z}"
END="${END:-2016-01-02T00:00:00Z}"
QUERY_COUNT="${QUERY_COUNT:-1000}"

# ============== 帮助信息 ==============
show_help() {
  cat << EOF
用法: $0 --workers <worker_list> --dbs <database> [选项]

必需参数:
  --workers <list>    Worker 数量列表，逗号分隔，如: 4,8,12,16
  --dbs <database>    数据库类型: influx, questdb, tdengine, timescaledb

可选参数:
  --skip-load         跳过数据导入，仅运行查询
  --skip-query        跳过查询执行，仅运行导入
  --batch-size <N>    批处理大小 (默认: 10000)
  --data-dir <path>   数据文件目录 (默认: ./data_iot)
  --result-dir <path> 结果输出目录 (默认: ./results_iot)
  --seed <N>          数据生成种子 (默认: 12345)
  --scale <N>         数据规模 (默认: 4000)
  -h, --help          显示此帮助信息

示例:
  $0 --workers 4,8,12,16 --dbs tdengine
  $0 --workers 8 --dbs influx --skip-load
  $0 --workers 4,8 --dbs questdb --batch-size 5000

EOF
}

# ============== 参数解析 ==============
while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) WORKERS_ARRAY=(${2//,/ }); shift 2 ;;
    --dbs) DB_TYPE="${2,,}"; shift 2 ;;
    --skip-load) SKIP_LOAD=1; shift ;;
    --skip-query) SKIP_QUERY=1; shift ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --result-dir) RESULT_DIR_BASE="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    -h|--help) show_help; exit 0 ;;
    *) echo "未知参数: $1"; exit 1 ;;
  esac
done

# ============== 参数验证 ==============
if [[ ${#WORKERS_ARRAY[@]} -eq 0 ]]; then
  echo "错误: 必须指定 --workers 参数"
  show_help
  exit 1
fi

if [[ -z "$DB_TYPE" ]]; then
  echo "错误: 必须指定 --dbs 参数"
  show_help
  exit 1
fi

case "$DB_TYPE" in
  influx|questdb|tdengine|timescaledb) ;;
  *) echo "错误: 不支持的数据库类型: $DB_TYPE"; exit 1 ;;
esac

# ============== 辅助函数 ==============
log_info() {
  echo "[INFO] $@"
}

log_error() {
  echo "[ERROR] $@" >&2
}

run_load_influx() {
  local workers=$1
  local result_dir=$2
  
  log_info "[influx] 创建数据库"
  curl -G "$INFLUX_URL/query" --data-urlencode "q=CREATE DATABASE tsbs_iot" || true
  
  log_info "[influx] 开始导入 (workers=$workers)"
  ./bin/tsbs_load_influx \
    --urls="$INFLUX_URL" \
    --db-name=tsbs_iot \
    --replication-factor=1 \
    --workers="$workers" \
    --batch-size="$BATCH_SIZE" \
    --file="$DATA_DIR/iot_influx.gz" \
    | tee "$result_dir/load_influx.log"
}

run_load_questdb() {
  local workers=$1
  local result_dir=$2
  
  log_info "[questdb] 开始导入 (workers=$workers)"
  ./bin/tsbs_load_questdb \
    --file="$DATA_DIR/iot_questdb.gz" \
    --workers="$workers" \
    --batch-size="$BATCH_SIZE" \
    --ilp-bind-to="$QUESTDB_ILP_BIND" \
    --url="$QUESTDB_URL" \
    | tee "$result_dir/load_questdb.log"
}

run_load_tdengine() {
  local workers=$1
  local result_dir=$2
  
  log_info "[tdengine] 开始导入 (workers=$workers)"
  ./bin/tsbs_load_tdengine \
    --host="$TDENGINE_HOST" \
    --port="$TDENGINE_PORT" \
    --user="$TDENGINE_USER" \
    --pass="$TDENGINE_PASS" \
    --db-name=tsbs_iot \
    --workers="$workers" \
    --batch-size="$BATCH_SIZE" \
    --file="$DATA_DIR/iot_tdengine.gz" \
    | tee "$result_dir/load_tdengine.log"
}

run_load_timescaledb() {
  local workers=$1
  local result_dir=$2
  
  log_info "[timescaledb] 开始导入 (workers=$workers)"
  ./bin/tsbs_load_timescaledb \
    --host="$PGHOST" \
    --port="$PGPORT" \
    --user="$PGUSER" \
    --pass="$PGPASSWORD" \
    --db-name=tsbs_iot \
    --workers="$workers" \
    --batch-size="$BATCH_SIZE" \
    --file="$DATA_DIR/iot_timescaledb.gz" \
    | tee "$result_dir/load_timescaledb.log"
}


# ============== 主流程 ==============
log_info "=========================================="
log_info "TSBS IoT 基准测试"
log_info "=========================================="
log_info "数据库: $DB_TYPE"
log_info "Worker 列表: ${WORKERS_ARRAY[*]}"
log_info "批处理大小: $BATCH_SIZE"
log_info "数据目录: $DATA_DIR"
log_info "结果目录: $RESULT_DIR_BASE"
log_info "=========================================="

# 检查必要的文件
if [[ $SKIP_LOAD -eq 0 ]]; then
  if [[ ! -f "$DATA_DIR/iot_${DB_TYPE}.gz" ]]; then
    log_error "数据文件不存在: $DATA_DIR/iot_${DB_TYPE}.gz"
    exit 1
  fi
fi

# 检查必要的二进制文件
if [[ $SKIP_LOAD -eq 0 ]]; then
  if [[ ! -x "./bin/tsbs_load_${DB_TYPE}" ]]; then
    log_error "加载工具不存在: ./bin/tsbs_load_${DB_TYPE}"
    exit 1
  fi
fi

if [[ $SKIP_QUERY -eq 0 ]]; then
  if [[ ! -x "./tsbs_iot_query_manager.sh" ]]; then
    log_error "查询脚本不存在: ./tsbs_iot_query_manager.sh"
    exit 1
  fi
fi

# 为每个 worker 数量运行测试
for workers in "${WORKERS_ARRAY[@]}"; do
  log_info ""
  log_info "========== Worker=$workers =========="
  
  # 创建结果目录
  result_dir="${RESULT_DIR_BASE}_${workers}_worker"
  mkdir -p "$result_dir"
  
  # 运行导入
  if [[ $SKIP_LOAD -eq 0 ]]; then
    log_info "开始导入数据..."
    case "$DB_TYPE" in
      influx) run_load_influx "$workers" "$result_dir" ;;
      questdb) run_load_questdb "$workers" "$result_dir" ;;
      tdengine) run_load_tdengine "$workers" "$result_dir" ;;
      timescaledb) run_load_timescaledb "$workers" "$result_dir" ;;
    esac
    
    if [[ $? -ne 0 ]]; then
      log_error "导入失败"
      exit 1
    fi
    log_info "导入完成"
  fi
  
  # 运行查询
  if [[ $SKIP_QUERY -eq 0 ]]; then
    log_info "开始查询测试..."
    
    # 第一次运行需要生成查询，后续运行跳过生成
    skip_gen=$([[ $FIRST_RUN -eq 1 ]] && echo 0 || echo 1)
    FIRST_RUN=0
    
    # 设置环境变量并运行查询脚本
    export SEED SCALE START END QUERY_COUNT
    export RESULT_DIR="$result_dir"
    export WORKERS_NUM="$workers"
    export PGHOST PGPORT PGUSER PGPASSWORD
    export TDENGINE_HOST TDENGINE_PORT TDENGINE_USER TDENGINE_PASS
    
    # 调用查询脚本（在同一目录）
    if [[ $skip_gen -eq 1 ]]; then
      ./tsbs_iot_query_manager.sh --dbs "$DB_TYPE" --workers "$workers" --no-gen
    else
      ./tsbs_iot_query_manager.sh --dbs "$DB_TYPE" --workers "$workers"
    fi
    
    if [[ $? -ne 0 ]]; then
      log_error "查询测试失败"
      exit 1
    fi
    log_info "查询测试完成"
  fi
done

log_info ""
log_info "=========================================="
log_info "所有测试完成！"
log_info "结果保存在: ${RESULT_DIR_BASE}_*_worker"
log_info "=========================================="
