#!/usr/bin/env python3
import argparse
import csv
import re
from collections import Counter, defaultdict
from pathlib import Path

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


PROJECTED_KEYWORDS = [
    "projected",
    "estimate",
    "estimated",
    "budget",
    "budgeted",
    "anticipated",
    "expected",
    "proposed",
    "planned",
    "target",
    "projection",
]

REALIZED_KEYWORDS = [
    "actual",
    "realized",
    "final",
    "completed",
    "completion",
    "incurred",
    "spent",
    "expenditure",
    "delivered",
]

TIME_HINT_KEYWORDS = [
    "month",
    "months",
    "year",
    "years",
    "schedule",
    "timeline",
    "duration",
    "completion",
    "deadline",
]

MONEY_RE = re.compile(
    r"(\$+\s*\(?\d[\d,]*(?:\.\d+)?\)?\s*(?:billion|million|thousand|bn|m|k)?)|"
    r"(\b\d[\d,]*(?:\.\d+)?\s*(?:billion|million|thousand|bn|m|k)\b)",
    re.IGNORECASE,
)
TIME_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(month|months|mo|mos|year|years|yr|yrs)\b", re.IGNORECASE)
MATTER_FILE_RE = re.compile(r"matter_(\d+)_attachment_(\d+)\.pdf$", re.IGNORECASE)


def parse_args():
    p = argparse.ArgumentParser(description="Extract projected vs realized cost/time mentions from TIF PDFs")
    p.add_argument("--pdf-dir", default="../input/pdfs")
    p.add_argument("--attachments-csv", default="../input/tif_attachments.csv")
    p.add_argument("--matters-csv", default="../input/tif_matters.csv")
    p.add_argument("--projects-master-csv", default="../input/tif_projects_master.csv")
    p.add_argument("--projects-annual-csv", default="../input/tif_projects_by_district_year.csv")
    p.add_argument("--output-mentions", default="../output/tif_pdf_projected_realized_mentions.csv")
    p.add_argument("--output-pairs", default="../output/tif_pdf_projected_realized_pairs.csv")
    p.add_argument("--output-summary", default="../output/tif_pdf_projected_realized_summary.csv")
    p.add_argument("--max-pdfs", type=int, default=0)
    p.add_argument("--min-project-name-len", type=int, default=12)
    return p.parse_args()


def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def read_csv(path):
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def normalize_text(value):
    s = (value or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def safe_filename(text, default="item", max_len=140):
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or "")).strip("_")
    if s == "":
        s = default
    return s[:max_len]


def safe_float(value):
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_tif_number(value):
    s = str(value or "").strip().upper()
    m = re.search(r"(\d{1,3})", s)
    if not m:
        return ""
    return f"T-{int(m.group(1)):03d}"


def parse_money_value(raw):
    token = raw.strip().lower()
    cleaned = token.replace("$", "").replace(",", "").replace("(", "").replace(")", "").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not m:
        return None, None
    value = float(m.group(0))

    unit = "dollars"
    if "billion" in cleaned or re.search(r"\bbn\b", cleaned):
        value *= 1_000_000_000
        unit = "dollars"
    elif "million" in cleaned or re.search(r"\bm\b", cleaned):
        value *= 1_000_000
        unit = "dollars"
    elif "thousand" in cleaned or re.search(r"\bk\b", cleaned):
        value *= 1_000
        unit = "dollars"

    return value, unit


def parse_time_months(value, unit):
    x = safe_float(value)
    if x is None:
        return None
    u = unit.lower()
    if u in {"year", "years", "yr", "yrs"}:
        return x * 12.0
    return x


def extract_pdf_pages(pdf_path):
    if PdfReader is None:
        return [], "pypdf_not_installed"

    pages = []
    try:
        reader = PdfReader(str(pdf_path))
        for i, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            pages.append((i, text))
        return pages, ""
    except Exception as exc:
        return [], str(exc)


def keyword_positions(text, keywords):
    positions = []
    lower = text.lower()
    for kw in keywords:
        start = 0
        while True:
            idx = lower.find(kw, start)
            if idx < 0:
                break
            positions.append((idx, kw))
            start = idx + 1
    positions.sort(key=lambda x: x[0])
    return positions


def infer_status(text, token_pos):
    all_positions = keyword_positions(text, PROJECTED_KEYWORDS) + keyword_positions(text, REALIZED_KEYWORDS)
    if not all_positions:
        return ""

    best_kw = ""
    best_dist = 10**9
    for pos, kw in all_positions:
        dist = abs(pos - token_pos)
        if dist < best_dist:
            best_dist = dist
            best_kw = kw

    if best_kw in PROJECTED_KEYWORDS:
        return "projected"
    if best_kw in REALIZED_KEYWORDS:
        return "realized"
    return ""


def infer_keyword_bucket(text, token_pos):
    positions = keyword_positions(text, PROJECTED_KEYWORDS + REALIZED_KEYWORDS + TIME_HINT_KEYWORDS)
    if not positions:
        return ""
    best = min(positions, key=lambda x: abs(x[0] - token_pos))
    return best[1]


def looks_like_time_context(text):
    lower = text.lower()
    return any(kw in lower for kw in TIME_HINT_KEYWORDS)


def extract_mentions_from_snippet(snippet, page_number, pdf_name, matter_meta, project_guess):
    rows = []
    text = re.sub(r"\s+", " ", snippet).strip()
    if text == "":
        return rows

    for m in MONEY_RE.finditer(text):
        raw = m.group(0).strip()
        value, unit = parse_money_value(raw)
        if value is None:
            continue
        status = infer_status(text, m.start())
        if status == "":
            continue

        rows.append(
            {
                "pdf_file": pdf_name,
                "matter_id": matter_meta["matter_id"],
                "attachment_id": matter_meta["attachment_id"],
                "matter_file": matter_meta["matter_file"],
                "matter_title": matter_meta["matter_title"],
                "page_number": page_number,
                "metric_type": "cost",
                "status": status,
                "value_raw": raw,
                "value_numeric": value,
                "unit": unit,
                "keyword_bucket": infer_keyword_bucket(text, m.start()),
                "snippet": text[:500],
                "project_name_guess": project_guess["project_name_guess"],
                "tif_number_guess": project_guess["tif_number_guess"],
            }
        )

    if looks_like_time_context(text):
        for m in TIME_RE.finditer(text):
            n = m.group(1)
            u = m.group(2)
            months = parse_time_months(n, u)
            if months is None:
                continue
            status = infer_status(text, m.start())
            if status == "":
                continue

            rows.append(
                {
                    "pdf_file": pdf_name,
                    "matter_id": matter_meta["matter_id"],
                    "attachment_id": matter_meta["attachment_id"],
                    "matter_file": matter_meta["matter_file"],
                    "matter_title": matter_meta["matter_title"],
                    "page_number": page_number,
                    "metric_type": "time_months",
                    "status": status,
                    "value_raw": m.group(0),
                    "value_numeric": months,
                    "unit": "months",
                    "keyword_bucket": infer_keyword_bucket(text, m.start()),
                    "snippet": text[:500],
                    "project_name_guess": project_guess["project_name_guess"],
                    "tif_number_guess": project_guess["tif_number_guess"],
                }
            )

    return rows


def build_matter_lookup(attachments_rows, matters_rows):
    matter_lookup_by_ids = {}
    matter_lookup_by_file = {}

    # Legistar-style matter table
    matters_by_old = {}
    matters_by_new = {}
    for r in matters_rows:
        old_id = str(r.get("MatterId", "")).strip()
        if old_id:
            matters_by_old[old_id] = r
        new_id = str(r.get("matter_id", "")).strip()
        if new_id:
            matters_by_new[new_id] = r

    for r in attachments_rows:
        # Legistar-style attachments
        mid_old = str(r.get("MatterId", "")).strip()
        aid_old = str(r.get("MatterAttachmentId", "")).strip()
        if mid_old and aid_old:
            m = matters_by_old.get(mid_old, {})
            matter_lookup_by_ids[(mid_old, aid_old)] = {
                "matter_id": mid_old,
                "attachment_id": aid_old,
                "matter_file": m.get("MatterFile", r.get("MatterFile", "")),
                "matter_title": m.get("MatterTitle", r.get("MatterTitle", "")),
            }

        # eLMS-style attachments
        mid_new = str(r.get("matter_id", "")).strip()
        uid = str(r.get("attachment_uid", "")).strip()
        record_number = str(r.get("record_number", "")).strip()
        if mid_new and uid:
            m = matters_by_new.get(mid_new, {})
            pdf_name = f"elms_{safe_filename(record_number, default=safe_filename(mid_new))}_{safe_filename(uid)}.pdf".lower()
            matter_lookup_by_file[pdf_name] = {
                "matter_id": mid_new,
                "attachment_id": uid,
                "matter_file": record_number,
                "matter_title": m.get("title", "") or m.get("short_title", "") or "",
            }

    return matter_lookup_by_ids, matter_lookup_by_file

def build_project_lookup(projects_master_rows, projects_annual_rows, min_name_len):
    project_rows = []
    for r in projects_master_rows:
        name = (r.get("project_name") or "").strip()
        if name:
            project_rows.append(
                {
                    "project_name": name,
                    "tif_number": normalize_tif_number(r.get("tif_district", "")),
                    "source": "master",
                }
            )

    for r in projects_annual_rows:
        name = (r.get("project_name") or "").strip()
        tif = normalize_tif_number(r.get("tif_number", ""))
        if name and tif:
            project_rows.append(
                {
                    "project_name": name,
                    "tif_number": tif,
                    "source": "annual",
                }
            )

    dedup = {}
    for r in project_rows:
        key = normalize_text(r["project_name"])
        if len(key) < min_name_len:
            continue
        if key not in dedup:
            dedup[key] = r

    candidates = sorted(dedup.items(), key=lambda kv: len(kv[0]), reverse=True)
    return candidates


def guess_project_for_document(full_text, project_candidates):
    norm_doc = normalize_text(full_text)
    for key, row in project_candidates:
        if key in norm_doc:
            return {
                "project_name_guess": row.get("project_name", ""),
                "tif_number_guess": row.get("tif_number", ""),
            }
    return {"project_name_guess": "", "tif_number_guess": ""}


def pair_projected_realized(mentions):
    grouped = defaultdict(list)
    for r in mentions:
        key = (r["pdf_file"], r["matter_id"], r["attachment_id"], r["metric_type"])
        grouped[key].append(r)

    pairs = []
    for key, rows in grouped.items():
        projected = [r for r in rows if r["status"] == "projected"]
        realized = [r for r in rows if r["status"] == "realized"]
        projected.sort(key=lambda x: (int(x["page_number"]), x["snippet"]))
        realized.sort(key=lambda x: (int(x["page_number"]), x["snippet"]))

        used = set()
        for p in projected:
            best_idx = None
            best_dist = 10**9
            for i, a in enumerate(realized):
                if i in used:
                    continue
                dist = abs(int(p["page_number"]) - int(a["page_number"]))
                if dist < best_dist:
                    best_dist = dist
                    best_idx = i
            if best_idx is None:
                continue

            a = realized[best_idx]
            used.add(best_idx)
            pv = safe_float(p["value_numeric"])
            av = safe_float(a["value_numeric"])
            delta = av - pv if pv is not None and av is not None else None
            delta_pct = (delta / pv) if pv not in (None, 0) and delta is not None else None

            pairs.append(
                {
                    "pdf_file": p["pdf_file"],
                    "matter_id": p["matter_id"],
                    "attachment_id": p["attachment_id"],
                    "matter_file": p["matter_file"],
                    "matter_title": p["matter_title"],
                    "project_name_guess": p["project_name_guess"],
                    "tif_number_guess": p["tif_number_guess"],
                    "metric_type": p["metric_type"],
                    "projected_value": pv,
                    "realized_value": av,
                    "delta_value": delta,
                    "delta_pct": delta_pct,
                    "unit": p["unit"],
                    "projected_page": p["page_number"],
                    "realized_page": a["page_number"],
                    "projected_snippet": p["snippet"][:500],
                    "realized_snippet": a["snippet"][:500],
                }
            )

    return pairs


def filename_to_ids(name):
    m = MATTER_FILE_RE.search(name)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def build_summary(pdf_count, parsed_count, parse_failures, mentions, pairs):
    mentions_by_metric = Counter(r["metric_type"] for r in mentions)
    mentions_by_status = Counter(r["status"] for r in mentions)
    pairs_by_metric = Counter(r["metric_type"] for r in pairs)
    unique_docs_with_mentions = len({r["pdf_file"] for r in mentions})

    rows = [
        {"metric": "pdf_files_seen", "value": pdf_count},
        {"metric": "pdf_files_parsed", "value": parsed_count},
        {"metric": "pdf_files_failed_parse", "value": parse_failures},
        {"metric": "docs_with_mentions", "value": unique_docs_with_mentions},
        {"metric": "mention_rows_total", "value": len(mentions)},
        {"metric": "mention_rows_cost", "value": mentions_by_metric.get("cost", 0)},
        {"metric": "mention_rows_time_months", "value": mentions_by_metric.get("time_months", 0)},
        {"metric": "mention_rows_projected", "value": mentions_by_status.get("projected", 0)},
        {"metric": "mention_rows_realized", "value": mentions_by_status.get("realized", 0)},
        {"metric": "pair_rows_total", "value": len(pairs)},
        {"metric": "pair_rows_cost", "value": pairs_by_metric.get("cost", 0)},
        {"metric": "pair_rows_time_months", "value": pairs_by_metric.get("time_months", 0)},
    ]
    return rows


def main():
    args = parse_args()

    pdf_dir = Path(args.pdf_dir).resolve()
    attachments_csv = Path(args.attachments_csv).resolve()
    matters_csv = Path(args.matters_csv).resolve()
    projects_master_csv = Path(args.projects_master_csv).resolve()
    projects_annual_csv = Path(args.projects_annual_csv).resolve()
    output_mentions = Path(args.output_mentions).resolve()
    output_pairs = Path(args.output_pairs).resolve()
    output_summary = Path(args.output_summary).resolve()

    attachments_rows = read_csv(attachments_csv)
    matters_rows = read_csv(matters_csv)
    projects_master_rows = read_csv(projects_master_csv)
    projects_annual_rows = read_csv(projects_annual_csv)

    matter_lookup_by_ids, matter_lookup_by_file = build_matter_lookup(attachments_rows, matters_rows)
    project_lookup = build_project_lookup(projects_master_rows, projects_annual_rows, args.min_project_name_len)

    pdf_files = []
    if pdf_dir.exists():
        pdf_files = sorted([p for p in pdf_dir.rglob("*.pdf") if p.is_file()])
    if args.max_pdfs and args.max_pdfs > 0:
        pdf_files = pdf_files[: args.max_pdfs]

    mentions = []
    parse_failures = 0
    parsed_count = 0

    for pdf_path in pdf_files:
        matter_meta = matter_lookup_by_file.get(pdf_path.name.lower())
        if matter_meta is None:
            matter_id, attachment_id = filename_to_ids(pdf_path.name)
            matter_meta = matter_lookup_by_ids.get(
                (matter_id, attachment_id),
                {
                    "matter_id": matter_id,
                    "attachment_id": attachment_id,
                    "matter_file": "",
                    "matter_title": "",
                },
            )

        pages, err = extract_pdf_pages(pdf_path)
        if err:
            parse_failures += 1
            continue
        parsed_count += 1

        full_text = "\n".join(text for _, text in pages)
        project_guess = guess_project_for_document(full_text, project_lookup)

        for page_number, page_text in pages:
            if not page_text:
                continue
            lines = [re.sub(r"\s+", " ", x).strip() for x in page_text.splitlines()]
            lines = [x for x in lines if x]
            if not lines:
                continue

            for i, line in enumerate(lines):
                snippet = line
                if i + 1 < len(lines) and len(snippet) < 220:
                    snippet = f"{snippet} {lines[i + 1]}"
                mentions.extend(
                    extract_mentions_from_snippet(
                        snippet=snippet,
                        page_number=page_number,
                        pdf_name=pdf_path.name,
                        matter_meta=matter_meta,
                        project_guess=project_guess,
                    )
                )

    pairs = pair_projected_realized(mentions)
    summary_rows = build_summary(len(pdf_files), parsed_count, parse_failures, mentions, pairs)

    mention_fields = [
        "pdf_file",
        "matter_id",
        "attachment_id",
        "matter_file",
        "matter_title",
        "page_number",
        "metric_type",
        "status",
        "value_raw",
        "value_numeric",
        "unit",
        "keyword_bucket",
        "snippet",
        "project_name_guess",
        "tif_number_guess",
    ]

    pair_fields = [
        "pdf_file",
        "matter_id",
        "attachment_id",
        "matter_file",
        "matter_title",
        "project_name_guess",
        "tif_number_guess",
        "metric_type",
        "projected_value",
        "realized_value",
        "delta_value",
        "delta_pct",
        "unit",
        "projected_page",
        "realized_page",
        "projected_snippet",
        "realized_snippet",
    ]

    write_csv(output_mentions, mentions, mention_fields)
    write_csv(output_pairs, pairs, pair_fields)
    write_csv(output_summary, summary_rows, ["metric", "value"])

    print(f"PDF files considered: {len(pdf_files)}")
    print(f"PDF files parsed: {parsed_count}")
    print(f"Mention rows: {len(mentions)}")
    print(f"Pair rows: {len(pairs)}")
    print(f"Summary: {output_summary}")


if __name__ == "__main__":
    main()
