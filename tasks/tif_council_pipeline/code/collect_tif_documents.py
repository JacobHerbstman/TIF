#!/usr/bin/env python3
import argparse
import csv
import json
import re
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse

ELMS_BASE = "https://api.chicityclerkelms.chicago.gov"
ANNUAL_REPORTS_INDEX = "https://www.chicago.gov/city/en/depts/dcd/supp_info/tif-district-annual-reports-2004-present.html"

ELMS_SEARCH_TERMS = [
    "tax increment financing",
    "redevelopment agreement",
    "redevelopment area",
    "amendment to redevelopment agreement",
    "intergovernmental agreement TIF",
]

DATASET_COUNTS = [
    ("mex4-ppfc", "tif_funded_rda_iga_projects"),
    ("72uz-ikdv", "tif_annual_report_projects"),
    ("iekz-rtng", "tif_funded_economic_development_projects"),
    ("umwj-yc4m", "tif_itemized_expenditures"),
    ("fpsv-qjg3", "tif_projections_2025_2034"),
    ("nm3d-wkdd", "tif_investment_committee_decisions"),
]


def run_cmd(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def run_curl(url, output_path=None, retries=5, pause=0.9, max_time=35):
    err = ""
    for attempt in range(retries):
        cmd = [
            "curl",
            "-sSL",
            "--fail",
            "--connect-timeout",
            "8",
            "--max-time",
            str(max_time),
            url,
        ]
        if output_path is not None:
            cmd += ["-o", str(output_path)]
        proc = run_cmd(cmd)
        if proc.returncode == 0:
            return proc.stdout
        err = (proc.stderr or "").strip()
        if attempt < retries - 1:
            time.sleep(pause * (attempt + 1))
    raise RuntimeError(err or f"curl failed: {url}")


def fetch_json(url):
    return json.loads(run_curl(url))


def ensure_dirs(*dirs):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def write_json(path, payload):
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def append_csv(path, rows, fieldnames):
    if not rows:
        return 0
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    return len(rows)


def append_csv_unique(path, rows, fieldnames, key_fields):
    existing = set()
    if path.exists() and path.stat().st_size > 0:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                key = tuple(str(r.get(k, "")).strip() for k in key_fields)
                if any(x != "" for x in key):
                    existing.add(key)

    fresh = []
    for r in rows:
        key = tuple(str(r.get(k, "")).strip() for k in key_fields)
        if not any(x != "" for x in key):
            continue
        if key in existing:
            continue
        existing.add(key)
        fresh.append(r)

    appended = append_csv(path, fresh, fieldnames)
    skipped = len(rows) - appended
    return appended, skipped


def is_truthy(value):
    s = str(value or "").strip().lower()
    return s in {"1", "true", "yes", "y", "t"}


def load_seen_matter_ids(path):
    seen = set()
    if not path.exists() or path.stat().st_size == 0:
        return seen

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        rows = csv.DictReader(f)
        for r in rows:
            mid = (r.get("matter_id") or r.get("MatterId") or "").strip()
            if mid == "":
                continue
            if "detail_fetched" in r and not is_truthy(r.get("detail_fetched")):
                continue
            seen.add(mid)
    return seen


def load_attachment_matter_ids(path):
    seen = set()
    if not path.exists() or path.stat().st_size == 0:
        return seen
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            mid = (r.get("matter_id") or r.get("MatterId") or "").strip()
            if mid:
                seen.add(mid)
    return seen


def extract_hrefs(html):
    return re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)


def normalize_chicago_url(href, base_url):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("javascript:") or href.startswith("#") or href.startswith("mailto:"):
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://www.chicago.gov" + href
    return urljoin(base_url, href)


def safe_filename(text, default="item", max_len=140):
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or "")).strip("_")
    if s == "":
        s = default
    return s[:max_len]


def looks_like_pdf(path_or_url):
    if not path_or_url:
        return False
    return ".pdf" in path_or_url.lower()


def safe_int(value):
    if value is None:
        return None
    m = re.search(r"-?\d+", str(value))
    return int(m.group(0)) if m else None


def is_valid_pdf(path):
    if not path.exists() or path.stat().st_size < 500:
        return False, "file_missing_or_too_small"
    with path.open("rb") as f:
        magic = f.read(5)
    if magic != b"%PDF-":
        return False, "not_pdf_magic"
    return True, ""


def fetch_dataset_counts():
    rows = []
    for dataset_id, slug in DATASET_COUNTS:
        url = f"https://data.cityofchicago.org/resource/{dataset_id}.json?$select=count(*)%20as%20count"
        status = "ok"
        count = None
        note = ""
        try:
            payload = fetch_json(url)
            if isinstance(payload, list) and payload:
                count = int(payload[0].get("count", 0))
            else:
                status = "failed"
                note = "unexpected payload"
        except Exception as exc:
            status = "failed"
            note = str(exc)
        rows.append(
            {
                "source": "socrata",
                "slug": slug,
                "dataset_id": dataset_id,
                "count": count,
                "status": status,
                "note": note,
            }
        )
    return rows


def fetch_elms_search_rows(term, top=100, max_rows=5000, start_skip=0):
    rows = []
    skip = max(0, start_skip)
    meta_count = None

    while True:
        url = f"{ELMS_BASE}/matter?search={quote(term)}&top={top}&skip={skip}"
        payload = fetch_json(url)
        batch = payload.get("data", []) if isinstance(payload, dict) else []
        meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
        if meta_count is None:
            meta_count = meta.get("count")

        if not batch:
            break

        rows.extend(batch)
        if len(batch) < top:
            break
        if len(rows) >= max_rows:
            break
        skip += top

    rows = rows[:max_rows]
    return rows, meta_count, start_skip + len(rows)

def collect_elms_matters(max_matters, term_index=-1, start_skip=0, seen_matter_ids=None):
    if term_index >= len(ELMS_SEARCH_TERMS):
        raise ValueError(f"term-index out of range: {term_index}")

    selected_terms = list(enumerate(ELMS_SEARCH_TERMS))
    if term_index >= 0:
        selected_terms = [(term_index, ELMS_SEARCH_TERMS[term_index])]

    seen_matter_ids = seen_matter_ids or set()
    matter_map = {}
    keyword_hits = defaultdict(set)
    term_rows = []
    term_meta = []

    for idx, term in selected_terms:
        if len(matter_map) >= max_matters:
            break

        status = "ok"
        err = ""
        found = 0
        meta_count = None
        next_skip = start_skip
        skipped_pre2010 = 0
        skipped_seen = 0
        new_matters = 0

        try:
            rows, meta_count, next_skip = fetch_elms_search_rows(
                term,
                max_rows=max(1, max_matters - len(matter_map)),
                start_skip=start_skip,
            )
            found = len(rows)
            for r in rows:
                year = safe_int(r.get("fileYear"))
                if year is not None and year < 2010:
                    skipped_pre2010 += 1
                    continue

                mid = (r.get("matterId") or "").strip()
                if mid == "":
                    continue
                if mid in seen_matter_ids:
                    skipped_seen += 1
                    continue

                if mid not in matter_map:
                    if len(matter_map) >= max_matters:
                        break
                    matter_map[mid] = r
                    new_matters += 1

                keyword_hits[mid].add(term)
                term_rows.append(
                    {
                        "term_index": idx,
                        "search_term": term,
                        "matter_id": mid,
                        "record_number": r.get("recordNumber", ""),
                    }
                )
        except Exception as exc:
            status = "failed"
            err = str(exc)

        term_meta.append(
            {
                "term_index": idx,
                "search_term": term,
                "start_skip": start_skip,
                "next_skip": next_skip,
                "status": status,
                "meta_count": meta_count,
                "rows_fetched": found,
                "new_matters_added": new_matters,
                "rows_skipped_seen": skipped_seen,
                "rows_skipped_pre2010": skipped_pre2010,
                "note": err,
            }
        )

    return matter_map, keyword_hits, term_rows, term_meta

def build_elms_matter_and_attachment_tables(matter_map, keyword_hits, fetch_details, max_detail_calls=0):
    matter_rows = []
    attachment_rows = []
    matter_json = []
    detail_failures = 0
    detail_calls = 0
    detail_skipped_cap = 0

    for mid, summary in matter_map.items():
        detail = summary if isinstance(summary, dict) else {}
        fetched = 0

        can_fetch = fetch_details and (max_detail_calls <= 0 or detail_calls < max_detail_calls)
        if can_fetch:
            try:
                detail = json.loads(run_curl(f"{ELMS_BASE}/matter/{mid}", retries=1, max_time=4))
                fetched = 1
                detail_calls += 1
            except Exception:
                detail = summary if isinstance(summary, dict) else {}
                detail_failures += 1
        elif fetch_details:
            detail_skipped_cap += 1

        if not isinstance(detail, dict):
            detail = {}
            detail_failures += 1

        matter_json.append(detail)

        attachments = detail.get("attachments") or []
        if not isinstance(attachments, list):
            attachments = []

        record_number = detail.get("recordNumber") or ""

        matter_rows.append(
            {
                "matter_id": mid,
                "record_number": record_number,
                "legacy_record_number": detail.get("legacyRecordNumber", ""),
                "original_record_number": detail.get("originalRecordNumber", ""),
                "title": detail.get("title", ""),
                "short_title": detail.get("shortTitle", ""),
                "type": detail.get("type", ""),
                "status": detail.get("status", ""),
                "sub_status": detail.get("subStatus", ""),
                "controlling_body": detail.get("controllingBody", ""),
                "filing_sponsor": detail.get("filingSponsor", ""),
                "file_year": detail.get("fileYear", ""),
                "introduction_date": detail.get("introductionDate", ""),
                "final_action_date": detail.get("finalActionDate", ""),
                "attachments_count": len(attachments),
                "detail_fetched": fetched,
                "keyword_hits": " | ".join(sorted(keyword_hits.get(mid, set()))),
            }
        )

        for idx, a in enumerate(attachments, start=1):
            if not isinstance(a, dict):
                continue
            path = (a.get("path") or a.get("url") or a.get("hyperlink") or "").strip()
            if path == "":
                continue
            url = path if path.startswith("http") else normalize_chicago_url(path, "https://www.chicago.gov/")
            parsed = urlparse(url)
            tail = Path(parsed.path).name
            attachment_uid = (a.get("id") or "").strip() if isinstance(a.get("id"), str) else a.get("id")
            if attachment_uid in (None, ""):
                attachment_uid = tail if tail else f"{mid}_{idx}"

            attachment_rows.append(
                {
                    "matter_id": mid,
                    "record_number": record_number,
                    "attachment_index": idx,
                    "attachment_uid": str(attachment_uid),
                    "attachment_name": a.get("name", ""),
                    "attachment_path": path,
                    "attachment_url": url,
                    "is_pdf": 1 if looks_like_pdf(url) else 0,
                    "attachment_file": tail,
                }
            )

    return matter_rows, attachment_rows, matter_json, detail_failures, detail_calls, detail_skipped_cap


def download_elms_pdfs(attachment_rows, pdf_dir, max_pdf):
    ensure_dirs(pdf_dir)
    status_rows = []

    candidates = [r for r in attachment_rows if int(r.get("is_pdf", 0)) == 1]
    attempted = 0
    downloaded = 0

    for r in candidates:
        if max_pdf > 0 and attempted >= max_pdf:
            break

        attempted += 1
        mid = r.get("matter_id", "")
        rec = r.get("record_number", "")
        uid = r.get("attachment_uid", "")
        url = r.get("attachment_url", "")

        file_stub = f"elms_{safe_filename(rec, default=safe_filename(mid))}_{safe_filename(uid)}"
        out = pdf_dir / f"{file_stub}.pdf"

        if out.exists():
            ok, err = is_valid_pdf(out)
            if ok:
                downloaded += 1
                status_rows.append(
                    {
                        "matter_id": mid,
                        "record_number": rec,
                        "attachment_uid": uid,
                        "url": url,
                        "status": "already_exists",
                        "error": "",
                        "bytes": out.stat().st_size,
                        "local_path": str(out),
                    }
                )
                continue

        try:
            run_curl(url, output_path=out, retries=5, pause=0.9, max_time=90)
            ok, err = is_valid_pdf(out)
            if ok:
                downloaded += 1
                status_rows.append(
                    {
                        "matter_id": mid,
                        "record_number": rec,
                        "attachment_uid": uid,
                        "url": url,
                        "status": "downloaded",
                        "error": "",
                        "bytes": out.stat().st_size,
                        "local_path": str(out),
                    }
                )
            else:
                if out.exists():
                    out.unlink()
                status_rows.append(
                    {
                        "matter_id": mid,
                        "record_number": rec,
                        "attachment_uid": uid,
                        "url": url,
                        "status": "failed",
                        "error": err,
                        "bytes": 0,
                        "local_path": "",
                    }
                )
        except Exception as exc:
            if out.exists():
                out.unlink()
            status_rows.append(
                {
                    "matter_id": mid,
                    "record_number": rec,
                    "attachment_uid": uid,
                    "url": url,
                    "status": "failed",
                    "error": str(exc),
                    "bytes": 0,
                    "local_path": "",
                }
            )

    failed = sum(1 for r in status_rows if r["status"] == "failed")
    return status_rows, attempted, downloaded, failed


def extract_report_year(text):
    m = re.search(r"(20[0-9]{2}|19[0-9]{2})", text or "")
    if m:
        return int(m.group(1))
    return None


def collect_annual_report_pdf_links(min_year):
    index_html = run_curl(ANNUAL_REPORTS_INDEX)
    hrefs = extract_hrefs(index_html)

    year_page_rows = []
    pdf_link_rows = []
    seen_pages = set()
    seen_pdf = set()

    for h in hrefs:
        u = normalize_chicago_url(h, ANNUAL_REPORTS_INDEX)
        if u == "":
            continue

        low = u.lower()
        if "district" not in low or "annual" not in low or "report" not in low or not low.endswith(".html"):
            continue

        y = extract_report_year(u)
        if y is not None and y < min_year:
            continue

        if u in seen_pages:
            continue
        seen_pages.add(u)

        status = "ok"
        note = ""
        pdf_count = 0

        try:
            html = run_curl(u)
            links = extract_hrefs(html)
            for lh in links:
                if ".pdf" not in lh.lower():
                    continue
                pu = normalize_chicago_url(lh, u)
                if pu == "" or pu in seen_pdf:
                    continue
                seen_pdf.add(pu)
                pdf_count += 1
                pdf_link_rows.append(
                    {
                        "year": extract_report_year(pu) or y,
                        "year_page_url": u,
                        "pdf_url": pu,
                    }
                )
        except Exception as exc:
            status = "failed"
            note = str(exc)

        year_page_rows.append(
            {
                "year": y,
                "year_page_url": u,
                "status": status,
                "pdf_links_found": pdf_count,
                "note": note,
            }
        )

    return year_page_rows, pdf_link_rows


def download_annual_report_pdfs(pdf_link_rows, pdf_dir, max_pdf):
    status_rows = []
    ensure_dirs(pdf_dir)

    attempted = 0
    downloaded = 0

    for r in pdf_link_rows:
        if max_pdf > 0 and attempted >= max_pdf:
            break

        attempted += 1
        year = r.get("year")
        url = r.get("pdf_url", "")
        page_url = r.get("year_page_url", "")
        parsed = urlparse(url)
        tail = Path(parsed.path).name or safe_filename(url, default="annual_report") + ".pdf"
        year_folder = str(year) if year is not None else "unknown_year"
        out_dir = pdf_dir / year_folder
        ensure_dirs(out_dir)
        out = out_dir / safe_filename(tail, default="annual_report.pdf")

        if out.exists():
            ok, err = is_valid_pdf(out)
            if ok:
                downloaded += 1
                status_rows.append(
                    {
                        "year": year,
                        "year_page_url": page_url,
                        "pdf_url": url,
                        "status": "already_exists",
                        "error": "",
                        "bytes": out.stat().st_size,
                        "local_path": str(out),
                    }
                )
                continue

        try:
            run_curl(url, output_path=out, retries=5, pause=0.9, max_time=120)
            ok, err = is_valid_pdf(out)
            if ok:
                downloaded += 1
                status_rows.append(
                    {
                        "year": year,
                        "year_page_url": page_url,
                        "pdf_url": url,
                        "status": "downloaded",
                        "error": "",
                        "bytes": out.stat().st_size,
                        "local_path": str(out),
                    }
                )
            else:
                if out.exists():
                    out.unlink()
                status_rows.append(
                    {
                        "year": year,
                        "year_page_url": page_url,
                        "pdf_url": url,
                        "status": "failed",
                        "error": err,
                        "bytes": 0,
                        "local_path": "",
                    }
                )
        except Exception as exc:
            if out.exists():
                out.unlink()
            status_rows.append(
                {
                    "year": year,
                    "year_page_url": page_url,
                    "pdf_url": url,
                    "status": "failed",
                    "error": str(exc),
                    "bytes": 0,
                    "local_path": "",
                }
            )

    failed = sum(1 for r in status_rows if r["status"] == "failed")
    return status_rows, attempted, downloaded, failed


def parse_args():
    p = argparse.ArgumentParser(description="Collect TIF legislation and annual-report PDFs")
    p.add_argument("--input-dir", default="../input")
    p.add_argument("--output-dir", default="../output")
    p.add_argument("--max-matters", type=int, default=3000)
    p.add_argument("--term-index", type=int, default=-1, help="Use one eLMS term index (default -1 runs all terms)")
    p.add_argument("--start-skip", type=int, default=0, help="eLMS /matter pagination skip offset")
    p.add_argument("--max-detail-calls", type=int, default=0, help="Cap /matter/{id} calls per run (0 = no cap)")
    p.add_argument("--resume-from-csv", default="", help="Optional CSV path used to skip matter_ids already detailed")
    p.add_argument("--elms-fetch-details", type=int, default=1, help="1 to call /matter/{id} for attachments")
    p.add_argument("--max-elms-pdf", type=int, default=0, help="0 means no limit")
    p.add_argument("--min-report-year", type=int, default=2010)
    p.add_argument("--max-report-pdf", type=int, default=0, help="0 means no limit")
    p.add_argument("--skip-annual-reports", type=int, default=0, help="1 to skip annual report crawl/download")
    return p.parse_args()


def main():
    args = parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    elms_dir = input_dir / "elms"
    elms_pdf_dir = elms_dir / "pdf"
    reports_dir = input_dir / "annual_reports"
    reports_pdf_dir = reports_dir / "pdf"
    out_elms_matters = output_dir / "tif_elms_matters.csv"
    out_elms_attachments = output_dir / "tif_elms_attachments.csv"
    out_elms_pdf_status = output_dir / "tif_elms_pdf_download_status.csv"

    ensure_dirs(input_dir, output_dir, elms_dir, elms_pdf_dir, reports_dir, reports_pdf_dir)

    dataset_counts = fetch_dataset_counts()
    write_csv(
        output_dir / "tif_external_dataset_counts.csv",
        dataset_counts,
        ["source", "slug", "dataset_id", "count", "status", "note"],
    )

    resume_ids = set()
    resume_ids.update(load_seen_matter_ids(out_elms_matters))
    resume_ids.update(load_attachment_matter_ids(out_elms_attachments))
    if args.resume_from_csv:
        resume_ids.update(load_seen_matter_ids(Path(args.resume_from_csv).resolve()))

    matter_map, keyword_hits, term_rows, term_meta = collect_elms_matters(
        args.max_matters,
        term_index=args.term_index,
        start_skip=args.start_skip,
        seen_matter_ids=resume_ids,
    )
    matter_rows, attachment_rows, matter_json, detail_failures, detail_calls, detail_skipped_cap = (
        build_elms_matter_and_attachment_tables(
            matter_map,
            keyword_hits,
            fetch_details=(args.elms_fetch_details == 1),
            max_detail_calls=args.max_detail_calls,
        )
    )

    write_json(elms_dir / "tif_elms_matters_raw.json", matter_json)
    write_csv(
        output_dir / "tif_elms_search_term_counts.csv",
        term_meta,
        [
            "term_index",
            "search_term",
            "start_skip",
            "next_skip",
            "status",
            "meta_count",
            "rows_fetched",
            "new_matters_added",
            "rows_skipped_seen",
            "rows_skipped_pre2010",
            "note",
        ],
    )
    write_csv(
        output_dir / "tif_elms_search_hits.csv",
        term_rows,
        ["term_index", "search_term", "matter_id", "record_number"],
    )
    matters_fields = [
        "matter_id",
        "record_number",
        "legacy_record_number",
        "original_record_number",
        "title",
        "short_title",
        "type",
        "status",
        "sub_status",
        "controlling_body",
        "filing_sponsor",
        "file_year",
        "introduction_date",
        "final_action_date",
        "attachments_count",
        "detail_fetched",
        "keyword_hits",
    ]
    attachments_fields = [
        "matter_id",
        "record_number",
        "attachment_index",
        "attachment_uid",
        "attachment_name",
        "attachment_path",
        "attachment_url",
        "is_pdf",
        "attachment_file",
    ]

    matters_appended, matters_skipped = append_csv_unique(
        out_elms_matters,
        matter_rows,
        matters_fields,
        ["matter_id", "detail_fetched"],
    )
    attachments_appended, attachments_skipped = append_csv_unique(
        out_elms_attachments,
        attachment_rows,
        attachments_fields,
        ["matter_id", "attachment_uid"],
    )

    elms_pdf_status, elms_pdf_attempted, elms_pdf_downloaded, elms_pdf_failed = download_elms_pdfs(
        attachment_rows, elms_pdf_dir, args.max_elms_pdf
    )
    elms_pdf_status_fields = ["matter_id", "record_number", "attachment_uid", "url", "status", "error", "bytes", "local_path"]
    elms_pdf_status_appended, elms_pdf_status_skipped = append_csv_unique(
        out_elms_pdf_status,
        elms_pdf_status,
        elms_pdf_status_fields,
        ["matter_id", "attachment_uid", "status"],
    )

    if args.skip_annual_reports == 1:
        year_pages = []
        report_links = []
        report_pdf_attempted = 0
        report_pdf_downloaded = 0
        report_pdf_failed = 0
    else:
        year_pages, report_links = collect_annual_report_pdf_links(args.min_report_year)
        write_csv(
            output_dir / "tif_annual_report_pages.csv",
            year_pages,
            ["year", "year_page_url", "status", "pdf_links_found", "note"],
        )
        write_csv(
            output_dir / "tif_annual_report_pdf_links.csv",
            report_links,
            ["year", "year_page_url", "pdf_url"],
        )

        report_pdf_status, report_pdf_attempted, report_pdf_downloaded, report_pdf_failed = download_annual_report_pdfs(
            report_links, reports_pdf_dir, args.max_report_pdf
        )
        write_csv(
            output_dir / "tif_annual_report_pdf_download_status.csv",
            report_pdf_status,
            ["year", "year_page_url", "pdf_url", "status", "error", "bytes", "local_path"],
        )

    summary_rows = [
        {"metric": "elms_resume_seen_matter_ids", "value": len(resume_ids)},
        {"metric": "elms_term_index", "value": args.term_index},
        {"metric": "elms_start_skip", "value": args.start_skip},
        {"metric": "elms_unique_matters", "value": len(matter_rows)},
        {"metric": "elms_matters_appended", "value": matters_appended},
        {"metric": "elms_matters_skipped_existing", "value": matters_skipped},
        {"metric": "elms_detail_failures", "value": detail_failures},
        {"metric": "elms_detail_calls_made", "value": detail_calls},
        {"metric": "elms_detail_calls_skipped_cap", "value": detail_skipped_cap},
        {"metric": "elms_attachments_total", "value": len(attachment_rows)},
        {"metric": "elms_attachments_appended", "value": attachments_appended},
        {"metric": "elms_attachments_skipped_existing", "value": attachments_skipped},
        {"metric": "elms_attachments_pdf_candidates", "value": sum(1 for r in attachment_rows if int(r.get("is_pdf", 0)) == 1)},
        {"metric": "elms_pdf_attempted", "value": elms_pdf_attempted},
        {"metric": "elms_pdf_downloaded_or_existing", "value": elms_pdf_downloaded},
        {"metric": "elms_pdf_failed", "value": elms_pdf_failed},
        {"metric": "elms_pdf_status_rows_appended", "value": elms_pdf_status_appended},
        {"metric": "elms_pdf_status_rows_skipped_existing", "value": elms_pdf_status_skipped},
        {"metric": "annual_report_year_pages", "value": len(year_pages)},
        {"metric": "annual_report_pdf_links", "value": len(report_links)},
        {"metric": "annual_report_pdf_attempted", "value": report_pdf_attempted},
        {"metric": "annual_report_pdf_downloaded_or_existing", "value": report_pdf_downloaded},
        {"metric": "annual_report_pdf_failed", "value": report_pdf_failed},
    ]

    for r in dataset_counts:
        if r.get("status") == "ok":
            summary_rows.append({"metric": f"dataset_count_{r['slug']}", "value": r.get("count")})

    write_csv(output_dir / "tif_document_harvest_summary.csv", summary_rows, ["metric", "value"])

    print("Document harvest finished")
    print(f"Output summary: {output_dir / 'tif_document_harvest_summary.csv'}")
    print(f"eLMS matters appended: {matters_appended}; attachments appended: {attachments_appended}; pdf status appended: {elms_pdf_status_appended}")


if __name__ == "__main__":
    main()
