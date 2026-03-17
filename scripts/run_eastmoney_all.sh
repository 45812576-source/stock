#!/bin/bash
# 全量推进 eastmoney 管线，3批并行滚动，自动推进直到无剩余
# 用法：bash scripts/run_eastmoney_all.sh [full|d_only|both]
# 每轮从 offset=0 拉未处理记录，处理完自动消失，不会重复/跳过

BASE="$(cd "$(dirname "$0")/.." && pwd)"
PY="PYTHONPATH=$BASE python3 $BASE/scripts/push_eastmoney_pipeline.py"
LOG="$BASE/logs"
BATCH=50
PARALLEL=3  # 每轮并发批次数
MODE=${1:-both}

run_round() {
  local mode=$1
  local round=$2
  local pids=()
  local logfiles=()

  # 3批并行，offset=0/50/100（每批拉各自不重叠的段）
  for i in 0 1 2; do
    local offset=$((i * BATCH))
    local logfile="$LOG/eastmoney_${mode}_r${round}_${i}.log"
    logfiles+=("$logfile")
    eval "$PY --mode=$mode --offset=$offset --batch=$BATCH > $logfile 2>&1" &
    pids+=($!)
  done

  for pid in "${pids[@]}"; do
    wait $pid
  done

  # 打印本轮摘要
  for logfile in "${logfiles[@]}"; do
    tail -1 "$logfile" 2>/dev/null
  done
}

run_mode() {
  local mode=$1
  echo "[$(date '+%H:%M:%S')] ===== 开始 $mode 全量 ====="
  local round=1

  while true; do
    echo "[$(date '+%H:%M:%S')] --- $mode 第 $round 轮 ---"
    run_round $mode $round

    # 检查是否还有剩余
    remaining=$(PYTHONPATH=$BASE python3 -c "
from utils.db_utils import execute_cloud_query
family2 = ['research_report','strategy_report','roadshow_notes','feature_news']
if '$mode' == 'full':
    rows = execute_cloud_query('''
        SELECT COUNT(*) as cnt
        FROM extracted_texts et
        LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
        LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE sd.source = 'eastmoney_report'
          AND et.extract_quality != 'fail'
          AND (cs.id IS NULL OR et.kg_status IS NULL OR et.kg_status != 'done')
    ''')
else:
    placeholders = ','.join(['%s']*len(family2))
    rows = execute_cloud_query(f'''
        SELECT COUNT(*) as cnt
        FROM extracted_texts et
        JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE sd.source = 'eastmoney_report'
          AND et.summary_status = 'done'
          AND et.kg_status = 'done'
          AND sd.doc_type IN ({placeholders})
          AND (et.semantic_clean_status IS NULL OR et.semantic_clean_status != 'd_done')
    ''', family2)
print(int(rows[0]['cnt']))
" 2>/dev/null)

    echo "[$(date '+%H:%M:%S')] $mode 剩余: $remaining 条"
    if [ -z "$remaining" ] || [ "$remaining" = "0" ] || ([ "$remaining" -eq "$remaining" ] 2>/dev/null && [ "$remaining" -le 0 ]); then
      echo "[$(date '+%H:%M:%S')] ===== $mode 全部完成 ====="
      break
    fi
    round=$((round+1))
  done
}

if [ "$MODE" = "both" ]; then
  run_mode d_only &
  run_mode full &
  wait
elif [ "$MODE" = "d_only" ]; then
  run_mode d_only
else
  run_mode full
fi

echo "[$(date '+%H:%M:%S')] ===== 所有任务完成 ====="
