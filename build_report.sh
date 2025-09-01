cat > $RESULT_DIR/parse_single_type.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

INF_FILE="$RESULT_DIR/queries_influx.txt"
QDB_FILE="$RESULT_DIR/queries_questdb.txt"
OUT_CSV="$RESULT_DIR/report_single_type.csv"
OUT_MD="$RESULT_DIR/report_single_type.md"

parse_file () {
  local file="$1"
  local db="$2"
  # 变量：overall_qps, min, med, mean, max, stddev, sum, count
  awk -v DB="$db" '
    /Run complete after .* Overall query rate/ {
        # 取 Overall query rate X queries/sec
        match($0, /Overall query rate[[:space:]]+([0-9.]+)[[:space:]]+queries\/sec/, m)
        if(m[1]!=""){overall_qps=m[1]}
    }
    /last row per host:/ {
        mode="stats_next"
        next
    }
    mode=="stats_next" && /min:/ {
        # 示例: min:  11.65ms, med: 14.69ms, mean: 15.03ms, max: 21.66ms, stddev: 2.17ms, sum: 15.0sec, count: 1000
        gsub(/\r/,"")
        # 提取各字段
        match($0, /min:[[:space:]]*([0-9.]+)ms/, a)
        match($0, /med:[[:space:]]*([0-9.]+)ms/, b)
        match($0, /mean:[[:space:]]*([0-9.]+)ms/, c)
        match($0, /max:[[:space:]]*([0-9.]+)ms/, d)
        match($0, /stddev:[[:space:]]*([0-9.]+)ms/, e)
        match($0, /sum:[[:space:]]*([0-9.]+)sec/, f)
        match($0, /count:[[:space:]]*([0-9]+)/, g)
        if(a[1]!=""){
          min=a[1]; med=b[1]; mean=c[1]; max=d[1]; stddev=e[1]; sum_s=f[1]; count=g[1];
          # 每碰到一个 block 就输出（覆盖前一次 -> 保留最后一次）
          printf("%s\tlast_row_per_host\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", DB, min, med, mean, max, stddev, sum_s, count)
        }
        mode=""
    }
  ' "$file" | tail -n 1  # 如果多次，保留最后一次
}

# 生成 TSV
{
  echo -e "db\tquery_type\tmin_ms\tmed_ms\tmean_ms\tmax_ms\tstddev_ms\tsum_sec\tcount\toverall_qps"
  # 将 overall_qps 拼接：用 join 思路（awk 合并）
  parse_file "$INF_FILE" "Influx" > $OUT_CSV.tmp1
  parse_file "$QDB_FILE" "QuestDB" > $OUT_CSV.tmp2

  # 读取 overall qps 从各自文件
  get_qps () {
    local file="$1"
    awk '
      /Run complete after .* Overall query rate/ {
        match($0, /Overall query rate[[:space:]]+([0-9.]+)/, m);
        if(m[1]!=""){qps=m[1]}
      }
      END{if(qps!="")print qps}
    ' "$file"
  }

  INF_QPS=$(get_qps "$INF_FILE")
  QDB_QPS=$(get_qps "$QDB_FILE")

  awk -v QPS="$INF_QPS" 'BEGIN{OFS="\t"} {print $0, QPS}' $OUT_CSV.tmp1
  awk -v QPS="$QDB_QPS" 'BEGIN{OFS="\t"} {print $0, QPS}' $OUT_CSV.tmp2
} > $OUT_CSV.tsv

# 计算对比并输出最终 CSV/MD
awk -F'\t' '
  NR==1 {next}
  {
    db=$1; qt=$2;
    if(db=="Influx"){
      imin=$3; imed=$4; imean=$5; imax=$6; istd=$7; isum=$8; icount=$9; iqps=$10;
    } else if(db=="QuestDB"){
      qmin=$3; qmed=$4; qmean=$5; qmax=$6; qstd=$7; qsum=$8; qcount=$9; qqps=$10;
    }
  }
  END{
    # 只处理 last_row_per_host 单类型
    print "query_type,Influx_mean_ms,QuestDB_mean_ms,Influx_QPS,QuestDB_QPS,QPS_ratio_QDB_over_Influx,Mean_latency_ratio_Influx_over_QDB"
    if(imean!="" && qmean!="" && iqps!="" && qqps!=""){
      qps_ratio = qqps/iqps
      lat_ratio = imean/qmean
      print "last_row_per_host," imean "," qmean "," iqps "," qqps "," qps_ratio "," lat_ratio
    }
  }
' $OUT_CSV.tsv > $RESULT_DIR/report_ratio.csv

# Markdown
{
  echo "| Query | Influx mean(ms) | QuestDB mean(ms) | Influx QPS | QuestDB QPS | QPS ratio (QDB/Influx) | Mean latency ratio (Influx/QDB) |"
  echo "|-------|------------------|------------------|------------|-------------|------------------------|---------------------------------|"
  tail -n +2 $RESULT_DIR/report_ratio.csv | awk -F',' '{printf("| %s | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f |\n",$1,$2,$3,$4,$5,$6,$7)}'
} > $RESULT_DIR/report_ratio.md

echo "生成文件："
echo "  明细 TSV: $OUT_CSV.tsv"
echo "  对比 CSV: $RESULT_DIR/report_ratio.csv"
echo "  Markdown: $RESULT_DIR/report_ratio.md"
EOF

chmod +x $RESULT_DIR/parse_single_type.sh
$RESULT_DIR/parse_single_type.sh

# 查看结果
echo "====== CSV ======"
cat $RESULT_DIR/report_ratio.csv
echo "====== Markdown ======"
cat $RESULT_DIR/report_ratio.md

