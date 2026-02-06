#!/usr/bin/env python3
import argparse
import csv
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ELMS_SEARCH_TERMS = [
    "tax increment financing",
    "redevelopment agreement",
    "redevelopment area",
    "amendment to redevelopment agreement",
    "intergovernmental agreement TIF",
]


def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except Exception:
        return default


def parse_args():
    p = argparse.ArgumentParser(description="Run collect_tif_documents.py in resumable eLMS batches")
    p.add_argument("--input-dir", default="../input")
    p.add_argument("--output-dir", default="../output")
    p.add_argument("--start-term-index", type=int, default=0)
    p.add_argument("--end-term-index", type=int, default=len(ELMS_SEARCH_TERMS) - 1)
    p.add_argument("--start-skip", type=int, default=0)
    p.add_argument("--max-batches-per-term", type=int, default=200)
    p.add_argument("--max-matters-per-run", type=int, default=200)
    p.add_argument("--max-detail-calls", type=int, default=100)
    p.add_argument("--max-elms-pdf", type=int, default=100)
    p.add_argument("--elms-fetch-details", type=int, default=1)
    p.add_argument("--resume-from-csv", default="../output/tif_elms_matters.csv")
    p.add_argument("--skip-annual-reports", type=int, default=1)
    p.add_argument("--min-report-year", type=int, default=2010)
    p.add_argument("--max-report-pdf", type=int, default=1)
    p.add_argument("--retry-failures", type=int, default=2)
    p.add_argument("--retry-pause-seconds", type=float, default=2.0)
    p.add_argument("--sleep-seconds", type=float, default=0.0)
    p.add_argument("--stop-on-error", type=int, default=1)
    p.add_argument("--log-csv", default="../output/tif_elms_batch_run_log.csv")
    return p.parse_args()


def append_log(path, row):
    fields = [
        "run_utc",
        "term_index",
        "term",
        "batch_number",
        "start_skip",
        "next_skip",
        "rows_fetched",
        "new_matters_added",
        "status",
        "note",
        "command_returncode",
        "command_attempts",
    ]
    write_header = not path.exists() or path.stat().st_size == 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fields})


def read_term_counts_row(path, term_index):
    if not path.exists() or path.stat().st_size == 0:
        return None
    rows = list(csv.DictReader(path.open("r", newline="", encoding="utf-8-sig")))
    if not rows:
        return None
    matching = [r for r in rows if safe_int(r.get("term_index"), -999) == term_index]
    if matching:
        return matching[-1]
    return rows[-1]


def build_collect_cmd(args, term_index, start_skip):
    script_path = Path(__file__).resolve().with_name("collect_tif_documents.py")
    cmd = [
        sys.executable,
        str(script_path),
        "--input-dir",
        args.input_dir,
        "--output-dir",
        args.output_dir,
        "--max-matters",
        str(args.max_matters_per_run),
        "--term-index",
        str(term_index),
        "--start-skip",
        str(start_skip),
        "--max-detail-calls",
        str(args.max_detail_calls),
        "--elms-fetch-details",
        str(args.elms_fetch_details),
        "--max-elms-pdf",
        str(args.max_elms_pdf),
        "--min-report-year",
        str(args.min_report_year),
        "--max-report-pdf",
        str(args.max_report_pdf),
        "--skip-annual-reports",
        str(args.skip_annual_reports),
    ]
    if args.resume_from_csv:
        cmd += ["--resume-from-csv", args.resume_from_csv]
    return cmd


def run_collect_with_retries(cmd, retries, pause_seconds):
    attempts = 0
    while True:
        attempts += 1
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            return proc, attempts
        if attempts > retries + 1:
            return proc, attempts
        time.sleep(max(0.0, pause_seconds))


def main():
    args = parse_args()

    if args.start_term_index < 0 or args.end_term_index >= len(ELMS_SEARCH_TERMS):
        raise SystemExit("term index range is out of bounds")
    if args.end_term_index < args.start_term_index:
        raise SystemExit("end-term-index must be >= start-term-index")

    output_dir = Path(args.output_dir).resolve()
    term_counts_csv = output_dir / "tif_elms_search_term_counts.csv"
    log_csv = Path(args.log_csv).resolve()

    for term_index in range(args.start_term_index, args.end_term_index + 1):
        term = ELMS_SEARCH_TERMS[term_index]
        current_skip = args.start_skip if term_index == args.start_term_index else 0

        for batch_number in range(1, args.max_batches_per_term + 1):
            cmd = build_collect_cmd(args, term_index, current_skip)
            print(f"[term {term_index}] batch {batch_number}: skip={current_skip}")
            proc, attempts = run_collect_with_retries(cmd, args.retry_failures, args.retry_pause_seconds)

            now_utc = datetime.now(timezone.utc).isoformat()
            if proc.returncode != 0:
                note = (proc.stderr or proc.stdout or "").strip().replace("\n", " ")[:1000]
                append_log(
                    log_csv,
                    {
                        "run_utc": now_utc,
                        "term_index": term_index,
                        "term": term,
                        "batch_number": batch_number,
                        "start_skip": current_skip,
                        "next_skip": "",
                        "rows_fetched": "",
                        "new_matters_added": "",
                        "status": "command_failed",
                        "note": note,
                        "command_returncode": proc.returncode,
                        "command_attempts": attempts,
                    },
                )
                print(f"[term {term_index}] failed after {attempts} attempt(s): returncode={proc.returncode}")
                if args.stop_on_error == 1:
                    return 1
                break

            row = read_term_counts_row(term_counts_csv, term_index)
            if row is None:
                append_log(
                    log_csv,
                    {
                        "run_utc": now_utc,
                        "term_index": term_index,
                        "term": term,
                        "batch_number": batch_number,
                        "start_skip": current_skip,
                        "next_skip": "",
                        "rows_fetched": "",
                        "new_matters_added": "",
                        "status": "missing_term_counts",
                        "note": "tif_elms_search_term_counts.csv not found or empty",
                        "command_returncode": 0,
                        "command_attempts": attempts,
                    },
                )
                if args.stop_on_error == 1:
                    return 1
                break

            rows_fetched = safe_int(row.get("rows_fetched"), 0)
            new_matters = safe_int(row.get("new_matters_added"), 0)
            next_skip = safe_int(row.get("next_skip"), current_skip + rows_fetched)
            status = (row.get("status") or "").strip()
            note = (row.get("note") or "").strip()

            append_log(
                log_csv,
                {
                    "run_utc": now_utc,
                    "term_index": term_index,
                    "term": term,
                    "batch_number": batch_number,
                    "start_skip": current_skip,
                    "next_skip": next_skip,
                    "rows_fetched": rows_fetched,
                    "new_matters_added": new_matters,
                    "status": status or "ok",
                    "note": note,
                    "command_returncode": 0,
                    "command_attempts": attempts,
                },
            )

            print(
                f"[term {term_index}] rows_fetched={rows_fetched} new_matters={new_matters} "
                f"next_skip={next_skip} status={status or 'ok'}"
            )

            # Term exhausted or non-OK term status.
            if status != "ok" or rows_fetched <= 0 or new_matters <= 0:
                break

            if next_skip <= current_skip:
                next_skip = current_skip + max(1, args.max_matters_per_run)
            current_skip = next_skip

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    print(f"Batch runner finished. Log: {log_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
