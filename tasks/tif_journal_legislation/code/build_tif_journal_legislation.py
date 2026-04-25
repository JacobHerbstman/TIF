#!/usr/bin/env python3
import argparse
import csv
import hashlib
import os
import re
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

csv.field_size_limit(50_000_000)

try:
    import fitz
except Exception:  # pragma: no cover - handled at runtime
    fitz = None


DEFAULT_ALDERMAN_OUTPUT = Path("/Users/jacobherbstman/Desktop/alderman_data/tasks/clerk_journals_download/output")
DEFAULT_MANIFESTS = [
    DEFAULT_ALDERMAN_OUTPUT / "journal_manifest_1980_1998.csv",
    DEFAULT_ALDERMAN_OUTPUT / "journal_manifest_1999_2010.csv",
]
DEFAULT_JOURNAL_ROOT = DEFAULT_ALDERMAN_OUTPUT / "journals"

TIF_TERM_PATTERNS = [
    ("tax increment allocation financing", r"\btax\s+increment\s+allocation\s+financing\b"),
    ("tax increment revenue bonds", r"\btax\s+increment\s+revenue\s+bonds?\b"),
    ("tax increment", r"\btax\s+increment\b"),
    ("redevelopment plan and project", r"\bredevelopment\s+plan\s+and\s+project\b"),
    ("redevelopment project area", r"\bredevelopment\s+project\s+area\b"),
    ("redevelopment agreement", r"\bredevelopment\s+agreement\b"),
    ("intergovernmental agreement", r"\bintergovernmental\s+agreement\b"),
    ("allocation notes", r"\ballocation\s+notes?\b"),
    ("TIF", r"\bTIF\b|\bT\.I\.F\.?\b|\bT\.\s+I\.\s+F\.?\b"),
    ("amendment", r"\bamend(?:ment|ed|ing)?\b"),
    ("extension", r"\bexten(?:sion|ded|ding)\b"),
    ("not to exceed", r"\bnot\s+to\s+exceed\b"),
    ("public investment", r"\bpublic\s+investment\b"),
    ("private investment", r"\bprivate\s+investment\b"),
]

STRONG_TERMS = {
    "tax increment allocation financing",
    "tax increment revenue bonds",
    "tax increment",
    "redevelopment plan and project",
    "redevelopment project area",
    "redevelopment agreement",
    "intergovernmental agreement",
    "allocation notes",
}

ADDRESS_RE = re.compile(
    r"\b\d{1,5}\s+"
    r"(?:(?:N|S|E|W|North|South|East|West)\.?\s+)?"
    r"[A-Z][A-Za-z0-9.'-]+"
    r"(?:\s+[A-Z][A-Za-z0-9.'-]+){0,5}\s+"
    r"(?:Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Boulevard|Blvd\.?|Drive|Dr\.?|"
    r"Place|Pl\.?|Court|Ct\.?|Parkway|Pkwy\.?|Way|Lane|Ln\.?)\b",
    flags=re.I,
)

DOLLAR_RE = re.compile(
    r"\$\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:million|billion))?",
    flags=re.I,
)

ORDINANCE_DATE_RE = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}\b",
    flags=re.I,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build pre-2010 TIF legislation evidence queues from City Clerk journal PDFs."
    )
    parser.add_argument("--manifest-csv", action="append", help="Journal manifest CSV. Repeat for multiple files.")
    parser.add_argument("--journal-pdf-root", default=str(DEFAULT_JOURNAL_ROOT))
    parser.add_argument("--output-dir", default="../output")
    parser.add_argument("--pipeline-output-dir", default="../../tif_council_pipeline/output")
    parser.add_argument("--year-start", type=int, default=1981)
    parser.add_argument("--year-end", type=int, default=2010)
    parser.add_argument("--meeting-date", action="append", help="Restrict to one ISO date. Repeatable.")
    parser.add_argument("--max-journals", type=int, default=0, help="0 means all selected journals.")
    parser.add_argument("--adjacent-pages", type=int, default=1)
    parser.add_argument("--ocr-empty-pages", type=int, default=0)
    parser.add_argument("--write-page-text", type=int, default=1)
    parser.add_argument("--skip-existing-page-text", type=int, default=1)
    parser.add_argument("--smoke-test", type=int, default=0)
    return parser.parse_args()


def resolve_task_path(value, base_dir):
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)


def read_csv(path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fieldnames):
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def sha1_text(value):
    return hashlib.sha1((value or "").encode("utf-8", errors="ignore")).hexdigest()


def boolish(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "t"}


def safe_int(value):
    if value is None:
        return None
    match = re.search(r"-?\d+", str(value))
    return int(match.group(0)) if match else None


def norm_text(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def slugify(value, max_len=90):
    text = norm_text(value).replace(" ", "-")
    text = re.sub(r"-+", "-", text).strip("-")
    return (text or "item")[:max_len]


def clean_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def unique_join(values, limit=8):
    out = []
    seen = set()
    for value in values:
        text = clean_space(value)
        key = norm_text(text)
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return "; ".join(out)


def compile_patterns():
    return [(label, re.compile(pattern, flags=re.I)) for label, pattern in TIF_TERM_PATTERNS]


def load_manifest_rows(manifest_paths, journal_root, args):
    if not manifest_paths:
        manifest_paths = [str(path) for path in DEFAULT_MANIFESTS]

    selected_dates = set(args.meeting_date or [])
    rows = []
    seen_paths = set()

    for manifest_path in [Path(p).expanduser() for p in manifest_paths]:
        for row in read_csv(manifest_path):
            year = safe_int(row.get("year"))
            if year is None or year < args.year_start or year > args.year_end:
                continue
            meeting_date = clean_space(row.get("meeting_date"))
            if selected_dates and meeting_date not in selected_dates:
                continue
            rel_path = clean_space(row.get("rel_local_path"))
            local_path = Path(row.get("local_path") or "")
            if not local_path.is_absolute():
                local_path = journal_root / rel_path
            if not local_path.exists():
                continue
            if row.get("is_valid_pdf") not in {"", None} and not boolish(row.get("is_valid_pdf")):
                continue
            resolved = str(local_path.resolve())
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            out = dict(row)
            out["local_path"] = resolved
            out["year"] = year
            out["meeting_date"] = meeting_date
            rows.append(out)

    rows.sort(key=lambda r: (safe_int(r.get("year")) or 0, r.get("meeting_date") or "", r.get("filename") or ""))
    if args.max_journals and args.max_journals > 0:
        rows = rows[: args.max_journals]
    return rows


def ocr_page(doc, page_index):
    page = doc.load_page(page_index)
    pix = page.get_pixmap(matrix=fitz.Matrix(1.7, 1.7), alpha=False)
    fd, tmp_name = tempfile.mkstemp(prefix=f"tif_journal_ocr_{page_index}_", suffix=".png")
    os.close(fd)
    Path(tmp_name).unlink(missing_ok=True)
    pix.save(tmp_name)
    try:
        proc = subprocess.run(
            ["tesseract", tmp_name, "stdout", "--psm", "6"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        return proc.stdout if proc.returncode == 0 else ""
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def infer_section_hint(text):
    head = text[:2500].lower()
    if "committee on finance" in head:
        return "committee_on_finance"
    if "reports of standing committees" in head:
        return "reports_of_standing_committees"
    if "miscellaneous business" in head:
        return "miscellaneous_business"
    if "call of wards" in head or "presentation of petitions" in head:
        return "introduced_legislation"
    if "table of contents" in head:
        return "table_of_contents"
    if "legislative index" in head or re.search(r"\bindex\b", head):
        return "legislative_index"
    return ""


def page_text_path(output_dir, journal_row, page_num):
    year = str(journal_row.get("year") or "unknown")
    stem = Path(journal_row.get("filename") or Path(journal_row.get("local_path", "")).name).stem
    return output_dir / "page_text" / year / f"{slugify(stem)}_p{page_num:04d}.txt"


def load_or_extract_page_text(doc, journal_row, page_index, output_dir, args):
    page_num = page_index + 1
    text_path = page_text_path(output_dir, journal_row, page_num)
    if args.skip_existing_page_text and text_path.exists() and text_path.stat().st_size > 0:
        return text_path.read_text(encoding="utf-8", errors="ignore"), "cached", text_path

    text = doc.load_page(page_index).get_text("text") or ""
    method = "text"
    if len(clean_space(text)) < 25 and args.ocr_empty_pages:
        text = ocr_page(doc, page_index)
        method = "ocr"

    if args.write_page_text:
        ensure_dir(text_path.parent)
        text_path.write_text(text, encoding="utf-8", errors="ignore")
    return text, method, text_path


def matched_terms(text, compiled_patterns):
    labels = []
    for label, pattern in compiled_patterns:
        if pattern.search(text or ""):
            labels.append(label)
    lower = (text or "").lower()
    has_strong = any(label in STRONG_TERMS for label in labels)
    has_tif_context = "TIF" in labels and re.search(
        r"redevelopment|increment|financ|district|agreement|bonds?|notes?|ordinance",
        lower,
    )
    if not has_strong and not has_tif_context:
        return []
    return labels


def best_snippet(text, compiled_patterns, max_chars=650):
    if not text:
        return ""
    best = None
    for label, pattern in compiled_patterns:
        match = pattern.search(text)
        if not match:
            continue
        weight = 2 if label in STRONG_TERMS else 1
        score = (weight, -match.start())
        if best is None or score > best[0]:
            best = (score, match)
    if best is None:
        return clean_space(text[:max_chars])
    match = best[1]
    start = max(0, match.start() - max_chars // 2)
    end = min(len(text), match.end() + max_chars // 2)
    snippet = clean_space(text[start:end])
    if start > 0:
        snippet = "... " + snippet
    if end < len(text):
        snippet = snippet + " ..."
    return snippet


def extract_district_names(text):
    patterns = [
        r"([A-Z][A-Za-z0-9&/.,' -]{2,90})\s+Redevelopment\s+Project\s+Area",
        r"([A-Z][A-Za-z0-9&/.,' -]{2,90})\s+Tax\s+Increment(?:\s+Allocation)?(?:\s+Redevelopment)?",
        r"for\s+the\s+([A-Z][A-Za-z0-9&/.,' -]{2,90})\s+(?:TIF|Redevelopment\s+Project\s+Area)",
    ]
    names = []
    for pattern in patterns:
        for match in re.finditer(pattern, text or ""):
            name = clean_space(match.group(1))
            name = re.sub(r"^(?:the|a|an)\s+", "", name, flags=re.I)
            name = re.sub(r"\b(?:under|pursuant|and|as|in|of|for)$", "", name, flags=re.I).strip(" ,.;:-")
            if len(norm_text(name)) >= 3 and not re.fullmatch(r"(city|ordinance|committee|journal)", norm_text(name)):
                names.append(name)
    return unique_join(names)


def extract_project_names(text):
    names = []
    for match in re.finditer(r"([A-Z][A-Za-z0-9&/.,' -]{3,120})\s+Redevelopment\s+Agreement", text or ""):
        name = clean_space(match.group(1))
        name = re.sub(r"^(?:an?|the|ordinance|substitute ordinance)\s+", "", name, flags=re.I)
        names.append(name.strip(" ,.;:-"))
    for match in re.finditer(r"agreement\s+with\s+([A-Z][A-Za-z0-9&/.,' -]{3,100})", text or "", flags=re.I):
        names.append(clean_space(match.group(1)).strip(" ,.;:-"))
    return unique_join(names)


def extract_addresses(text):
    return unique_join([m.group(0) for m in ADDRESS_RE.finditer(text or "")])


def extract_dollars(text):
    return unique_join([m.group(0) for m in DOLLAR_RE.finditer(text or "")], limit=12)


def extract_ordinance_dates(text):
    return unique_join([m.group(0) for m in ORDINANCE_DATE_RE.finditer(text or "")], limit=8)


def infer_event_type(text, terms):
    lower = (text or "").lower()
    guesses = []
    if re.search(r"redevelopment\s+agreement", lower):
        guesses.append("redevelopment_agreement")
    if re.search(r"intergovernmental\s+agreement|\biga\b", lower):
        guesses.append("intergovernmental_agreement")
    if re.search(r"allocation\s+notes?|revenue\s+bonds?|bonds?", lower) and "tax increment" in lower:
        guesses.append("bond_or_note_authorization")
    if re.search(r"redevelopment\s+plan\s+and\s+project|designation|project\s+area|allocation\s+financing", lower):
        guesses.append("district_plan_designation_or_financing")
    if re.search(r"\bamend(?:ment|ed|ing)?\b|\bexten(?:sion|ded|ding)\b|substitut", lower):
        guesses.append("amendment_or_extension")
    if not guesses and terms:
        guesses.append("general_tif_reference")
    return "; ".join(guesses)


def infer_event_scope(event_type, text):
    has_project = any(x in event_type for x in ["redevelopment_agreement", "intergovernmental", "bond_or_note"])
    has_district = "district_plan_designation_or_financing" in event_type
    if has_project and has_district:
        return "district_and_project"
    if has_project:
        return "project_deal"
    if has_district:
        return "district"
    if re.search(r"\bproject\b|agreement|bonds?|notes?", text or "", flags=re.I):
        return "project_deal"
    return "unknown"


def infer_priority(event_type, text, terms):
    lower = (text or "").lower()
    if any(x in event_type for x in ["redevelopment_agreement", "bond_or_note_authorization"]):
        return "high"
    if "district_plan_designation_or_financing" in event_type and re.search(r"ordinance|adopt|approv|designat", lower):
        return "high"
    if re.search(r"not\s+to\s+exceed\s+\$|\$\s*\d", text or "", flags=re.I) and any(t in terms for t in STRONG_TERMS):
        return "high"
    if any(t in terms for t in STRONG_TERMS):
        return "medium"
    return "low"


def cluster_hit_pages(page_hits, page_count, adjacent_pages):
    hit_pages = sorted(page_hits)
    clusters = []
    for page in hit_pages:
        start = max(1, page - adjacent_pages)
        end = min(page_count, page + adjacent_pages)
        if not clusters or start > clusters[-1]["end"] + 1:
            clusters.append({"start": start, "end": end, "hit_pages": [page]})
        else:
            clusters[-1]["end"] = max(clusters[-1]["end"], end)
            clusters[-1]["hit_pages"].append(page)
    return clusters


def load_project_match_rows(pipeline_output_dir):
    candidates = []
    for path_name in ["tif_project_spine.csv", "tif_projects_master.csv"]:
        path = pipeline_output_dir / path_name
        for row in read_csv(path):
            names = [
                row.get("canonical_project_name"),
                row.get("master_project_name"),
                row.get("project_name"),
                row.get("project_name") if path_name == "tif_projects_master.csv" else "",
            ]
            name = next((clean_space(x) for x in names if clean_space(x)), "")
            if not name:
                continue
            candidates.append(
                {
                    "canonical_project_id": row.get("canonical_project_id") or row.get("id") or "",
                    "master_id": row.get("master_id") or row.get("id") or "",
                    "project_name": name,
                    "developer": row.get("master_developer") or row.get("developer") or "",
                    "address": row.get("master_address") or row.get("address") or "",
                    "tif_district": row.get("tif_district") or row.get("master_tif_district") or "",
                    "name_key": norm_text(name),
                    "district_key": norm_text(row.get("tif_district") or row.get("master_tif_district") or ""),
                }
            )
    return candidates


def best_project_match(candidate, project_rows):
    query_parts = [
        candidate.get("suggested_project", ""),
        candidate.get("suggested_tif_district", ""),
        candidate.get("snippet", "")[:160],
    ]
    query = norm_text(" ".join(query_parts))
    if len(query) < 5 or not project_rows:
        return {"status": "unmatched", "score": "", "name": "", "canonical_project_id": "", "master_id": ""}

    best = None
    for row in project_rows:
        name_score = SequenceMatcher(None, query, row["name_key"]).ratio() if row["name_key"] else 0.0
        district_score = 0.0
        district_query = norm_text(candidate.get("suggested_tif_district", ""))
        if district_query and row["district_key"]:
            district_score = SequenceMatcher(None, district_query, row["district_key"]).ratio()
        score = max(name_score, 0.65 * name_score + 0.35 * district_score)
        if best is None or score > best[0]:
            best = (score, row)

    if best is None or best[0] < 0.72:
        return {"status": "unmatched", "score": f"{best[0]:.3f}" if best else "", "name": "", "canonical_project_id": "", "master_id": ""}
    status = "suggested" if best[0] < 0.90 else "strong_suggestion"
    row = best[1]
    return {
        "status": status,
        "score": f"{best[0]:.3f}",
        "name": row["project_name"],
        "canonical_project_id": row["canonical_project_id"],
        "master_id": row["master_id"],
    }


def process_journal(journal_row, output_dir, args, compiled_patterns, project_rows):
    pdf_path = Path(journal_row["local_path"])
    journal_id = f"journal|{journal_row.get('meeting_date')}|{Path(journal_row.get('filename') or pdf_path.name).stem}"
    page_rows = []
    page_texts = {}
    page_terms = {}

    if fitz is None:
        raise RuntimeError("PyMuPDF/fitz is required for journal extraction.")

    doc = fitz.open(str(pdf_path))
    try:
        page_count = int(doc.page_count)
        for page_index in range(page_count):
            page_num = page_index + 1
            text, method, text_path = load_or_extract_page_text(doc, journal_row, page_index, output_dir, args)
            clean = clean_space(text)
            terms = matched_terms(text, compiled_patterns)
            page_texts[page_num] = text
            if terms:
                page_terms[page_num] = terms
            page_rows.append(
                {
                    "journal_id": journal_id,
                    "year": journal_row.get("year", ""),
                    "meeting_date": journal_row.get("meeting_date", ""),
                    "meeting_type": journal_row.get("meeting_type_norm", ""),
                    "document_title": journal_row.get("document_title", ""),
                    "journal_pdf": str(pdf_path.resolve()),
                    "journal_url": journal_row.get("pdf_url", ""),
                    "pdf_page": page_num,
                    "text_method": method,
                    "char_count": len(clean),
                    "text_hash": sha1_text(text),
                    "section_hint": infer_section_hint(text),
                    "matched_terms": "; ".join(terms),
                    "text_path": str(text_path.resolve()) if args.write_page_text else "",
                }
            )

        candidates = []
        clusters = cluster_hit_pages(page_terms.keys(), page_count, args.adjacent_pages)
        for cluster in clusters:
            start = cluster["start"]
            end = cluster["end"]
            window_text = "\n\n".join(page_texts.get(page_num, "") for page_num in range(start, end + 1))
            terms = []
            for page_num in cluster["hit_pages"]:
                terms.extend(page_terms.get(page_num, []))
            terms = list(dict.fromkeys(terms))
            event_type = infer_event_type(window_text, terms)
            event_scope = infer_event_scope(event_type, window_text)
            candidate_id = f"{slugify(journal_id)}-p{start:04d}-{end:04d}"
            candidate = {
                "candidate_id": candidate_id,
                "event_scope": event_scope,
                "event_type_guess": event_type,
                "meeting_date": journal_row.get("meeting_date", ""),
                "journal_pdf": str(pdf_path.resolve()),
                "journal_url": journal_row.get("pdf_url", ""),
                "start_page": start,
                "end_page": end,
                "first_hit_page": min(cluster["hit_pages"]) if cluster["hit_pages"] else start,
                "matched_terms": "; ".join(terms),
                "snippet": best_snippet(window_text, compiled_patterns),
                "suggested_tif_district": extract_district_names(window_text),
                "suggested_project": extract_project_names(window_text),
                "suggested_addresses": extract_addresses(window_text),
                "suggested_dollar_amounts": extract_dollars(window_text),
                "suggested_ordinance_reference_dates": extract_ordinance_dates(window_text),
                "priority": infer_priority(event_type, window_text, terms),
                "review_status": "needs_review",
            }
            match = best_project_match(candidate, project_rows)
            candidate.update(
                {
                    "suggested_match_status": match["status"],
                    "suggested_match_score": match["score"],
                    "suggested_match_name": match["name"],
                    "suggested_canonical_project_id": match["canonical_project_id"],
                    "suggested_master_id": match["master_id"],
                }
            )
            candidates.append(candidate)
    finally:
        doc.close()

    return page_rows, candidates


def build_journal_inventory(journal_rows, output_dir, pipeline_output_dir):
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    rows = []
    for row in journal_rows:
        local_path = Path(row["local_path"])
        source_id = f"city_clerk_journal|{row.get('meeting_date')}|{Path(row.get('filename') or local_path.name).stem}"
        rows.append(
            {
                "source": "city_clerk_journal",
                "source_id": source_id,
                "document_kind": "journal_pdf",
                "year": row.get("year", ""),
                "tif_number": "",
                "tif_district": "",
                "matter_source": "city_clerk_journal",
                "matter_id": "",
                "attachment_id": "",
                "record_number": row.get("meeting_date", ""),
                "url": row.get("pdf_url", ""),
                "local_path": str(local_path.resolve()),
                "download_status": row.get("download_status") or ("exists_valid" if local_path.exists() else "missing"),
                "http_status": row.get("http_status", ""),
                "content_type": "application/pdf",
                "discovered_via": "city_clerk_journal_manifest",
                "first_seen_run": now,
                "last_checked_run": now,
                "known_missing": 0,
                "note": row.get("document_title", ""),
            }
        )

    fieldnames = [
        "source",
        "source_id",
        "document_kind",
        "year",
        "tif_number",
        "tif_district",
        "matter_source",
        "matter_id",
        "attachment_id",
        "record_number",
        "url",
        "local_path",
        "download_status",
        "http_status",
        "content_type",
        "discovered_via",
        "first_seen_run",
        "last_checked_run",
        "known_missing",
        "note",
    ]
    write_csv(output_dir / "tif_journal_document_inventory.csv", rows, fieldnames)

    pipeline_inventory_path = pipeline_output_dir / "tif_document_inventory.csv"
    existing_rows = read_csv(pipeline_inventory_path)
    existing_fieldnames = fieldnames
    if existing_rows:
        existing_fieldnames = list(existing_rows[0].keys())
        for name in fieldnames:
            if name not in existing_fieldnames:
                existing_fieldnames.append(name)
    keyed = {
        (r.get("source", ""), r.get("source_id", ""), r.get("url", "")): dict(r)
        for r in existing_rows
    }
    for row in rows:
        key = (row["source"], row["source_id"], row["url"])
        prior = keyed.get(key, {})
        merged = dict(row)
        if prior.get("first_seen_run"):
            merged["first_seen_run"] = prior["first_seen_run"]
        keyed[key] = merged
    merged_rows = sorted(keyed.values(), key=lambda r: (r.get("source", ""), r.get("source_id", ""), r.get("url", "")))
    write_csv(pipeline_inventory_path, merged_rows, existing_fieldnames)
    return rows


def build_review_template(output_dir, candidates):
    path = output_dir / "tif_legislation_facts_review.csv"
    existing = {row.get("candidate_id", ""): row for row in read_csv(path)}
    fieldnames = [
        "candidate_id",
        "review_status",
        "event_type",
        "district_name",
        "project_name",
        "counterparty",
        "address",
        "address_raw",
        "address_normalized",
        "public_funding_amount",
        "private_funding_amount",
        "total_project_cost",
        "funding_raw_text",
        "timeline_date",
        "ordinance_reference_date",
        "initial_or_revision",
        "source_meeting_date",
        "journal_pdf",
        "journal_url",
        "source_start_page",
        "source_end_page",
        "source_snippet",
        "source_matched_terms",
        "suggested_tif_district",
        "suggested_project",
        "suggested_addresses",
        "suggested_dollar_amounts",
        "suggested_canonical_project_id",
        "suggested_master_id",
        "suggested_match_status",
        "suggested_match_score",
        "match_status",
        "matched_canonical_project_id",
        "matched_master_id",
        "reviewer_notes",
    ]

    rows = []
    seen = set()
    for candidate in candidates:
        candidate_id = candidate.get("candidate_id", "")
        seen.add(candidate_id)
        existing_status = str(existing.get(candidate_id, {}).get("review_status", "")).strip().lower()
        if candidate_id in existing and existing_status not in {"", "needs_review"}:
            row = dict(existing[candidate_id])
            for key in [
                "source_meeting_date",
                "journal_pdf",
                "journal_url",
                "source_start_page",
                "source_end_page",
                "source_snippet",
                "source_matched_terms",
                "suggested_tif_district",
                "suggested_project",
                "suggested_addresses",
                "suggested_dollar_amounts",
                "suggested_canonical_project_id",
                "suggested_master_id",
                "suggested_match_status",
                "suggested_match_score",
            ]:
                row.setdefault(key, "")
        else:
            row = {
                "candidate_id": candidate_id,
                "review_status": "needs_review",
                "event_type": candidate.get("event_type_guess", ""),
                "district_name": "",
                "project_name": "",
                "counterparty": "",
                "address": "",
                "address_raw": candidate.get("suggested_addresses", ""),
                "address_normalized": "",
                "public_funding_amount": "",
                "private_funding_amount": "",
                "total_project_cost": "",
                "funding_raw_text": candidate.get("suggested_dollar_amounts", ""),
                "timeline_date": candidate.get("meeting_date", ""),
                "ordinance_reference_date": candidate.get("suggested_ordinance_reference_dates", ""),
                "initial_or_revision": "",
                "source_meeting_date": candidate.get("meeting_date", ""),
                "journal_pdf": candidate.get("journal_pdf", ""),
                "journal_url": candidate.get("journal_url", ""),
                "source_start_page": candidate.get("start_page", ""),
                "source_end_page": candidate.get("end_page", ""),
                "source_snippet": candidate.get("snippet", ""),
                "source_matched_terms": candidate.get("matched_terms", ""),
                "suggested_tif_district": candidate.get("suggested_tif_district", ""),
                "suggested_project": candidate.get("suggested_project", ""),
                "suggested_addresses": candidate.get("suggested_addresses", ""),
                "suggested_dollar_amounts": candidate.get("suggested_dollar_amounts", ""),
                "suggested_canonical_project_id": candidate.get("suggested_canonical_project_id", ""),
                "suggested_master_id": candidate.get("suggested_master_id", ""),
                "suggested_match_status": candidate.get("suggested_match_status", ""),
                "suggested_match_score": candidate.get("suggested_match_score", ""),
                "match_status": "",
                "matched_canonical_project_id": "",
                "matched_master_id": "",
                "reviewer_notes": "",
            }
        rows.append(row)

    for candidate_id, row in existing.items():
        status = str(row.get("review_status", "")).strip().lower()
        if candidate_id and candidate_id not in seen and status not in {"", "needs_review"}:
            rows.append(row)

    rows.sort(key=lambda r: (r.get("review_status") != "needs_review", r.get("source_meeting_date", ""), r.get("candidate_id", "")))
    write_csv(path, rows, fieldnames)
    return rows


def is_confirmed(row):
    return str(row.get("review_status", "")).strip().lower() in {"confirmed", "accepted", "reviewed_confirmed"}


def build_derived_timelines(output_dir, review_rows):
    district_rows = []
    project_rows = []
    for row in review_rows:
        if not is_confirmed(row):
            continue
        event_type = row.get("event_type", "")
        common = {
            "candidate_id": row.get("candidate_id", ""),
            "event_type": event_type,
            "timeline_date": row.get("timeline_date") or row.get("source_meeting_date", ""),
            "ordinance_reference_date": row.get("ordinance_reference_date", ""),
            "initial_or_revision": row.get("initial_or_revision", ""),
            "journal_pdf": row.get("journal_pdf", ""),
            "journal_url": row.get("journal_url", ""),
            "source_start_page": row.get("source_start_page", ""),
            "source_end_page": row.get("source_end_page", ""),
            "source_snippet": row.get("source_snippet", ""),
            "review_status": row.get("review_status", ""),
        }
        if row.get("district_name") or "district" in event_type:
            district_rows.append(
                {
                    **common,
                    "district_name": row.get("district_name") or row.get("suggested_tif_district", ""),
                    "public_funding_amount": row.get("public_funding_amount", ""),
                    "private_funding_amount": row.get("private_funding_amount", ""),
                    "total_project_cost": row.get("total_project_cost", ""),
                    "funding_raw_text": row.get("funding_raw_text", ""),
                }
            )
        if row.get("project_name") or row.get("address") or "agreement" in event_type or "bond" in event_type:
            project_rows.append(
                {
                    **common,
                    "district_name": row.get("district_name") or row.get("suggested_tif_district", ""),
                    "project_name": row.get("project_name") or row.get("suggested_project", ""),
                    "counterparty": row.get("counterparty", ""),
                    "address": row.get("address") or row.get("address_normalized") or row.get("address_raw", ""),
                    "public_funding_amount": row.get("public_funding_amount", ""),
                    "private_funding_amount": row.get("private_funding_amount", ""),
                    "total_project_cost": row.get("total_project_cost", ""),
                    "funding_raw_text": row.get("funding_raw_text", ""),
                    "match_status": row.get("match_status", ""),
                    "matched_canonical_project_id": row.get("matched_canonical_project_id", ""),
                    "matched_master_id": row.get("matched_master_id", ""),
                }
            )

    district_fields = [
        "candidate_id",
        "district_name",
        "event_type",
        "timeline_date",
        "ordinance_reference_date",
        "initial_or_revision",
        "public_funding_amount",
        "private_funding_amount",
        "total_project_cost",
        "funding_raw_text",
        "journal_pdf",
        "journal_url",
        "source_start_page",
        "source_end_page",
        "source_snippet",
        "review_status",
    ]
    project_fields = [
        "candidate_id",
        "district_name",
        "project_name",
        "counterparty",
        "address",
        "event_type",
        "timeline_date",
        "ordinance_reference_date",
        "initial_or_revision",
        "public_funding_amount",
        "private_funding_amount",
        "total_project_cost",
        "funding_raw_text",
        "match_status",
        "matched_canonical_project_id",
        "matched_master_id",
        "journal_pdf",
        "journal_url",
        "source_start_page",
        "source_end_page",
        "source_snippet",
        "review_status",
    ]
    write_csv(output_dir / "tif_district_timeline_pre2010.csv", district_rows, district_fields)
    write_csv(output_dir / "tif_project_deal_timeline_pre2010.csv", project_rows, project_fields)
    return district_rows, project_rows


def write_summary(output_dir, journal_rows, page_rows, candidates, review_rows, district_rows, project_rows):
    by_priority = defaultdict(int)
    by_event = defaultdict(int)
    by_year = defaultdict(int)
    for candidate in candidates:
        by_priority[candidate.get("priority", "")] += 1
        for event in str(candidate.get("event_type_guess", "")).split(";"):
            event = clean_space(event)
            if event:
                by_event[event] += 1
        by_year[str(candidate.get("meeting_date", "")[:4])] += 1

    rows = [
        {"metric": "journal_rows_selected", "value": len(journal_rows)},
        {"metric": "page_text_index_rows", "value": len(page_rows)},
        {"metric": "evidence_candidate_rows", "value": len(candidates)},
        {"metric": "facts_review_rows", "value": len(review_rows)},
        {"metric": "confirmed_fact_rows", "value": sum(1 for row in review_rows if is_confirmed(row))},
        {"metric": "district_timeline_rows", "value": len(district_rows)},
        {"metric": "project_deal_timeline_rows", "value": len(project_rows)},
    ]
    for key, value in sorted(by_priority.items()):
        rows.append({"metric": f"candidate_priority_{slugify(key, 40)}", "value": value})
    for key, value in sorted(by_event.items()):
        rows.append({"metric": f"candidate_event_{slugify(key, 60)}", "value": value})
    for key, value in sorted(by_year.items()):
        rows.append({"metric": f"candidate_year_{key}", "value": value})
    write_csv(output_dir / "tif_journal_legislation_summary.csv", rows, ["metric", "value"])
    return rows


def run_smoke_test(output_dir, candidates):
    haystack = "\n".join(
        [
            candidate.get("snippet", "")
            + " "
            + candidate.get("matched_terms", "")
            + " "
            + candidate.get("suggested_tif_district", "")
            + " "
            + candidate.get("suggested_project", "")
            for candidate in candidates
        ]
    ).lower()
    checks = [
        ("edgewater_notes", ["edgewater", "allocation notes"]),
        ("edgewater_redevelopment_agreement", ["edgewater", "redevelopment agreement"]),
        ("chinatown_bond_amendment", ["chinatown", "amend"]),
        ("ryan_garfield_bonds", ["ryan-garfield", "bonds"]),
    ]
    rows = []
    failed = []
    for name, needles in checks:
        passed = all(needle.lower() in haystack for needle in needles)
        rows.append({"check": name, "passed": 1 if passed else 0, "needles": "; ".join(needles)})
        if not passed:
            failed.append(name)
    write_csv(output_dir / "tif_journal_legislation_smoke_test.csv", rows, ["check", "passed", "needles"])
    if failed:
        raise RuntimeError(f"Smoke test failed: {', '.join(failed)}")


def main():
    args = parse_args()
    code_dir = Path(__file__).resolve().parent
    output_dir = resolve_task_path(args.output_dir, code_dir)
    pipeline_output_dir = resolve_task_path(args.pipeline_output_dir, code_dir)
    journal_root = resolve_task_path(args.journal_pdf_root, code_dir)
    ensure_dir(output_dir)
    ensure_dir(pipeline_output_dir)

    manifest_paths = args.manifest_csv or [str(path) for path in DEFAULT_MANIFESTS]
    journal_rows = load_manifest_rows(manifest_paths, journal_root, args)
    build_journal_inventory(journal_rows, output_dir, pipeline_output_dir)

    compiled_patterns = compile_patterns()
    project_rows = load_project_match_rows(pipeline_output_dir)
    all_page_rows = []
    all_candidates = []
    for idx, journal_row in enumerate(journal_rows, start=1):
        print(
            f"[{idx}/{len(journal_rows)}] {journal_row.get('meeting_date')} "
            f"{Path(journal_row.get('local_path', '')).name}",
            flush=True,
        )
        page_rows, candidates = process_journal(journal_row, output_dir, args, compiled_patterns, project_rows)
        all_page_rows.extend(page_rows)
        all_candidates.extend(candidates)

    page_fields = [
        "journal_id",
        "year",
        "meeting_date",
        "meeting_type",
        "document_title",
        "journal_pdf",
        "journal_url",
        "pdf_page",
        "text_method",
        "char_count",
        "text_hash",
        "section_hint",
        "matched_terms",
        "text_path",
    ]
    candidate_fields = [
        "candidate_id",
        "event_scope",
        "event_type_guess",
        "meeting_date",
        "journal_pdf",
        "journal_url",
        "start_page",
        "end_page",
        "first_hit_page",
        "matched_terms",
        "snippet",
        "suggested_tif_district",
        "suggested_project",
        "suggested_addresses",
        "suggested_dollar_amounts",
        "suggested_ordinance_reference_dates",
        "suggested_match_status",
        "suggested_match_score",
        "suggested_match_name",
        "suggested_canonical_project_id",
        "suggested_master_id",
        "priority",
        "review_status",
    ]
    write_csv(output_dir / "tif_journal_page_text_index.csv", all_page_rows, page_fields)
    write_csv(output_dir / "tif_legislation_evidence_queue.csv", all_candidates, candidate_fields)
    review_rows = build_review_template(output_dir, all_candidates)
    district_rows, project_rows_out = build_derived_timelines(output_dir, review_rows)
    write_summary(output_dir, journal_rows, all_page_rows, all_candidates, review_rows, district_rows, project_rows_out)

    if args.smoke_test:
        run_smoke_test(output_dir, all_candidates)

    print(
        f"Wrote {len(all_candidates)} evidence candidates from {len(journal_rows)} journals "
        f"and {len(all_page_rows)} pages to {output_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
