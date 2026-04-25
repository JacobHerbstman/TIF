#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

OUT_DIR="$SCRIPT_DIR/../output"
PID_FILE="$OUT_DIR/elms_auto.pid"
LOG_FILE="$OUT_DIR/elms_auto.log"
BATCH_LOG="$OUT_DIR/tif_elms_batch_run_log.csv"
TERM_COUNTS="$OUT_DIR/tif_elms_search_term_counts.csv"

usage() {
  cat <<'EOF'
Usage:
  run_elms_auto_resume.sh start [TERM_START_OVERRIDE] [START_SKIP_OVERRIDE]
  run_elms_auto_resume.sh status
  run_elms_auto_resume.sh stop
  run_elms_auto_resume.sh logs
  run_elms_auto_resume.sh plan

Behavior:
  - start: resume from the latest checkpoint in tif_elms_batch_run_log.csv
  - status: show process, pid file, and latest checkpoint
  - stop: stop background run started by this wrapper
  - logs: tail the nohup log
  - plan: print the command that start would run (no execution)

Env overrides:
  TERM_END (default 4)
  MAX_BATCHES_PER_TERM (default 500)
  MAX_MATTERS (default 120)
  MAX_DETAIL_CALLS (default 80)
  MAX_ELMS_PDF (default 80)
  RESUME_FROM_CSV (default ../output/tif_elms_matters.csv)
  RETRY_FAILURES (default 8)
  RETRY_PAUSE_SECONDS (default 5)
  STOP_ON_ERROR (default 0)
  SLEEP_SECONDS (default 0)
EOF
}

is_running_pid() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  kill -0 "$pid" 2>/dev/null
}

latest_checkpoint() {
  local term_batch=0
  local skip_batch=0
  local term_counts=0
  local skip_counts=0

  if [[ -s "$BATCH_LOG" ]]; then
    local row
    row="$(awk 'NR>1 {line=$0} END {print line}' "$BATCH_LOG")"
    if [[ -n "$row" ]]; then
      local t s
      t="$(echo "$row" | awk -F, '{print $2}')"
      s="$(echo "$row" | awk -F, '{print $6}')"
      if [[ "$t" =~ ^[0-9]+$ ]]; then
        term_batch="$t"
      fi
      if [[ "$s" =~ ^[0-9]+$ ]]; then
        skip_batch="$s"
      fi
    fi
  fi

  if [[ -s "$TERM_COUNTS" ]]; then
    local row2
    row2="$(awk 'NR>1 {line=$0} END {print line}' "$TERM_COUNTS")"
    if [[ -n "$row2" ]]; then
      local t2 s2
      t2="$(echo "$row2" | awk -F, '{print $1}')"
      s2="$(echo "$row2" | awk -F, '{print $4}')"
      if [[ "$t2" =~ ^[0-9]+$ ]]; then
        term_counts="$t2"
      fi
      if [[ "$s2" =~ ^[0-9]+$ ]]; then
        skip_counts="$s2"
      fi
    fi
  fi

  # Prefer the most advanced checkpoint.
  if (( term_counts > term_batch )); then
    echo "${term_counts},${skip_counts}"
  elif (( term_counts == term_batch && skip_counts > skip_batch )); then
    echo "${term_counts},${skip_counts}"
  else
    echo "${term_batch},${skip_batch}"
  fi
}

cmd_start() {
  mkdir -p "$OUT_DIR"

  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if is_running_pid "$pid"; then
      echo "Already running with PID ${pid}. Use status/logs/stop."
      return 0
    fi
    rm -f "$PID_FILE"
  fi

  if ps -ax 2>/dev/null | grep -q '[r]un_elms_batches.py'; then
    echo "Detected an existing run_elms_batches.py process. Stop it before starting a new one."
    return 1
  fi

  local cp term_start start_skip
  cp="$(latest_checkpoint)"
  term_start="${cp%%,*}"
  start_skip="${cp##*,}"

  if [[ -n "${1:-}" ]]; then
    term_start="$1"
  fi
  if [[ -n "${2:-}" ]]; then
    start_skip="$2"
  fi

  if [[ ! "$term_start" =~ ^[0-9]+$ ]]; then
    echo "Invalid TERM_START: $term_start"
    return 1
  fi
  if [[ ! "$start_skip" =~ ^[0-9]+$ ]]; then
    echo "Invalid START_SKIP: $start_skip"
    return 1
  fi

  local term_end="${TERM_END:-4}"
  local max_batches="${MAX_BATCHES_PER_TERM:-500}"
  local max_matters="${MAX_MATTERS:-120}"
  local max_detail="${MAX_DETAIL_CALLS:-80}"
  local max_pdf="${MAX_ELMS_PDF:-80}"
  local resume_csv="${RESUME_FROM_CSV:-../output/tif_elms_matters.csv}"
  local retry_failures="${RETRY_FAILURES:-8}"
  local retry_pause="${RETRY_PAUSE_SECONDS:-5}"
  local stop_on_error="${STOP_ON_ERROR:-0}"
  local sleep_seconds="${SLEEP_SECONDS:-0}"

  local cmd=(
    make
    collect-docs-auto
    "TERM_START=${term_start}"
    "TERM_END=${term_end}"
    "START_SKIP=${start_skip}"
    "MAX_BATCHES_PER_TERM=${max_batches}"
    "MAX_MATTERS=${max_matters}"
    "MAX_DETAIL_CALLS=${max_detail}"
    "MAX_ELMS_PDF=${max_pdf}"
    "RESUME_FROM_CSV=${resume_csv}"
    "RETRY_FAILURES=${retry_failures}"
    "RETRY_PAUSE_SECONDS=${retry_pause}"
    "STOP_ON_ERROR=${stop_on_error}"
    "SLEEP_SECONDS=${sleep_seconds}"
  )

  nohup "${cmd[@]}" >"$LOG_FILE" 2>&1 &
  local pid="$!"
  echo "$pid" >"$PID_FILE"

  echo "Started eLMS auto-run."
  echo "PID: $pid"
  echo "TERM_START=$term_start START_SKIP=$start_skip"
  echo "Log: $LOG_FILE"
}

cmd_status() {
  local cp term_start start_skip
  cp="$(latest_checkpoint)"
  term_start="${cp%%,*}"
  start_skip="${cp##*,}"
  echo "Latest checkpoint: term_index=${term_start}, next_skip=${start_skip}"

  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if is_running_pid "$pid"; then
      echo "Wrapper PID file: running (PID $pid)"
      ps -p "$pid" -o pid,etime,time,command
    else
      echo "Wrapper PID file exists but process is not running."
    fi
  else
    echo "No wrapper PID file."
  fi

  echo "Matching processes:"
  ps -ax 2>/dev/null | grep -E '[r]un_elms_batches.py|[c]ollect_tif_documents.py' || true
}

cmd_stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "No PID file at $PID_FILE"
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if is_running_pid "$pid"; then
    kill "$pid"
    echo "Stopped PID $pid"
  else
    echo "PID file existed but process was not running."
  fi
  rm -f "$PID_FILE"
}

cmd_logs() {
  if [[ ! -f "$LOG_FILE" ]]; then
    echo "No log file: $LOG_FILE"
    return 1
  fi
  tail -n 50 "$LOG_FILE"
}

cmd_plan() {
  local cp term_start start_skip
  cp="$(latest_checkpoint)"
  term_start="${cp%%,*}"
  start_skip="${cp##*,}"

  if [[ -n "${1:-}" ]]; then
    term_start="$1"
  fi
  if [[ -n "${2:-}" ]]; then
    start_skip="$2"
  fi

  echo "Would run:"
  echo "make collect-docs-auto TERM_START=${term_start} TERM_END=${TERM_END:-4} START_SKIP=${start_skip} MAX_BATCHES_PER_TERM=${MAX_BATCHES_PER_TERM:-500} MAX_MATTERS=${MAX_MATTERS:-120} MAX_DETAIL_CALLS=${MAX_DETAIL_CALLS:-80} MAX_ELMS_PDF=${MAX_ELMS_PDF:-80} RESUME_FROM_CSV=${RESUME_FROM_CSV:-../output/tif_elms_matters.csv} RETRY_FAILURES=${RETRY_FAILURES:-8} RETRY_PAUSE_SECONDS=${RETRY_PAUSE_SECONDS:-5} STOP_ON_ERROR=${STOP_ON_ERROR:-0} SLEEP_SECONDS=${SLEEP_SECONDS:-0}"
}

main() {
  local action="${1:-start}"
  shift || true

  case "$action" in
    start) cmd_start "$@" ;;
    status) cmd_status ;;
    stop) cmd_stop ;;
    logs) cmd_logs ;;
    plan) cmd_plan "$@" ;;
    help|-h|--help) usage ;;
    *)
      echo "Unknown action: $action"
      usage
      return 1
      ;;
  esac
}

main "$@"
