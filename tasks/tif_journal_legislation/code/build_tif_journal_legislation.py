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

FUNDING_CONTEXT_RE = re.compile(
    r"\b(?:"
    r"not\s+to\s+exceed|principal\s+amount|project\s+costs?|redevelopment\s+project\s+costs?|"
    r"eligible\s+costs?|public\s+investment|private\s+investment|total\s+project\s+cost|"
    r"bond(?:s)?|note(?:s)?|allocation\s+financing|appropriat(?:e|ed|ion)|"
    r"reimburse(?:ment)?|grant|loan"
    r")\b",
    flags=re.I,
)

ORDINANCE_DATE_RE = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}\b",
    flags=re.I,
)

DISTRICT_NAME_STOP_KEYS = {
    "",
    "a",
    "an",
    "adopting",
    "area",
    "areas",
    "as",
    "avenue",
    "branch",
    "central",
    "city",
    "city council",
    "committee",
    "commission",
    "community development",
    "department",
    "designation",
    "designation of",
    "east",
    "district",
    "financing",
    "for",
    "commercial",
    "industrial",
    "increment",
    "incremental",
    "journal city council chicago",
    "kedzie",
    "loop",
    "north",
    "ordinance",
    "paulina",
    "project",
    "proposed",
    "policies",
    "real property",
    "redevelopment",
    "south",
    "state",
    "station",
    "tax",
    "the",
    "west",
    "within",
}

DISTRICT_KEY_DROP_WORDS = {
    "a",
    "an",
    "and",
    "area",
    "city",
    "community",
    "corridor",
    "district",
    "financing",
    "increment",
    "project",
    "redevelopment",
    "tax",
    "the",
}

DISTRICT_LOOSE_DROP_WORDS = DISTRICT_KEY_DROP_WORDS | {
    "ave",
    "avenue",
    "blvd",
    "boulevard",
    "dr",
    "drive",
    "rd",
    "road",
    "st",
    "street",
}

DISTRICT_NAME_BAD_RE = re.compile(
    r"\b(?:"
    r"act|ad\s+valorem|adopt(?:ed|ing|ion)?|aforesaid|aggregate\s+principal|"
    r"approval|approved|authorities|be\s+employed|blighted|commission|committee|"
    r"contiguous|council|designat(?:e|ed|ing|ion)?|department|eligib(?:le|ility)|"
    r"addition|anticipated|associates|authorization|best\s+interests|boundaries|"
    r"city\s+of\s+chicago|conditions|deposited|describes?|description|elect\s+to\s+issue|"
    r"entire|environment|equalized|existing|"
    r"expanded|facilities|following|formally|fund|goals|here(?:by|tofore)|honorable|"
    r"illinois|incremental\s+real\s+property|initial\s+equalized|issue|issuance|legal\s+description|located|"
    r"mayor|municipal\s+boundaries|must\s+designate|ordinance|original|placed\s+on\s+file|"
    r"pledge|principal\s+amount|private\s+actions|property|public\s+right|purpose\s+of\s+paying|"
    r"pursuant|qualif(?:y|ied)|regarding|revenues?|revitali[sz](?:e|ation)|said|"
    r"section\s+of\s*the|secured\s+by|study\s+area|submitted|thereof|towards?|"
    r"trust\s+indenture|vacant|valuation|well-being|whereas"
    r")\b",
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


def district_match_key(value):
    key = norm_text(value)
    if not key:
        return ""
    words = [word for word in key.split() if word not in DISTRICT_KEY_DROP_WORDS]
    return " ".join(words)


def district_loose_match_key(value):
    key = norm_text(value)
    if not key:
        return ""
    words = [word for word in key.split() if word not in DISTRICT_LOOSE_DROP_WORDS]
    return " ".join(words)


def clean_district_name(value):
    text = clean_space(value)
    text = re.sub(r"^[^A-Za-z0-9]+", "", text)
    text = re.sub(r"^\d{4}\s+", "", text)
    text = re.sub(r"^(?:referred\s*[-:]?\s*)", "", text, flags=re.I)
    text = re.sub(r"^(?:the|a|an)\s+", "", text, flags=re.I)
    text = re.sub(
        r"^(?:for|to|within|in|of|as|and|including|concerning|project\s+for)\s+(?:the\s+)?",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"^(?:city's|citys)\s+", "", text, flags=re.I)
    text = re.sub(
        r"^(?:designate|designating|designated|known\s+as|entitled|called)\s+(?:the\s+)?",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"^(?:redevelopment\s+of|development\s+of)\s+(?:the\s+)?", "", text, flags=re.I)
    text = re.sub(
        r"\b(?:tax\s+increment(?:\s+allocation)?(?:\s+redevelopment)?|redevelopment\s+project\s+area|"
        r"tax\s+increment\s+redevelopment\s+area|redevelopment\s+area|special\s+tax\s+allocation\s+fund|"
        r"redevelopment\s+plan\s+and\s+project|allocation\s+financing)\b.*$",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\b(?:area|project)\s+as\s+a\b.*$", "", text, flags=re.I)
    text = re.sub(r"\b(?:area|project|district)$", "", text, flags=re.I)
    text = clean_space(text.strip(" .,:;-'\"()[]{}"))
    key = district_match_key(text)
    if key in DISTRICT_NAME_STOP_KEYS or len(key) < 3:
        return ""
    if DISTRICT_NAME_BAD_RE.search(text):
        return ""
    alpha_words = [
        word
        for word in re.findall(r"[A-Za-z]+", text)
        if word.lower() not in {"st", "nd", "rd", "th"}
    ]
    if alpha_words:
        allowed_lower = {"and", "of", "the", "at", "in", "on"}
        titleish = sum(
            1
            for word in alpha_words
            if word.lower() in allowed_lower or word[0].isupper() or word.isupper()
        )
        if titleish / len(alpha_words) < 0.8:
            return ""
    key_words = key.split()
    if len(key_words) == 1 and len(key_words[0]) < 6 and not re.search(r"\d|[/&-]", text):
        return ""
    if len(key_words) > 10:
        return ""
    if len(text) > 100:
        return ""
    return text


def extract_district_name_candidates(text):
    raw_lines = [clean_space(line) for line in str(text or "").splitlines()]
    scan_units = []
    for idx, line in enumerate(raw_lines):
        lower = line.lower()
        if not any(
            term in lower
            for term in [
                "approval given",
                "tax increment",
                "redevelopment plan",
                "redevelopment project area",
                "special tax allocation fund",
                "allocation financing",
            ]
        ):
            continue
        unit = line
        if idx + 1 < len(raw_lines):
            unit = f"{unit} {raw_lines[idx + 1]}"
        scan_units.append(unit)
        if len(scan_units) >= 2000:
            break
    scan_text = "\n".join(scan_units)

    patterns = [
        r"APPROVAL\s+GIVEN\s+TO\s+(?:INTENT\s+TO\s+USE\s+)?(?:TAX\s+INCREMENT\s+(?:RE)?DEVELOPMENT\s+PLAN|TAX\s+INCREMENT\s+FINANCING)\s+FOR\s+([A-Z0-9][A-Za-z0-9&/.,'() -]{2,100}?)(?:\s+AREA|\s+REDEVELOPMENT|\s+TAX|\s+PROJECT|\.|\n)",
        r"(?:Tax\s+Increment\s+)?Redevelopment\s+Plan\s+and\s+Project\s+for\s+(?:the\s+)?([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)(?:\s+Redevelopment|\s+Tax\s+Increment|\s+Area|\.|\n)",
        r"(?:Tax\s+Increment\s+)?Redevelopment\s+Plan\s+for\s+(?:the\s+)?([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)(?:\s+Redevelopment|\s+Tax\s+Increment|\s+Area|\.|\n)",
        r"([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)\s+(?:Tax\s+Increment\s+Redevelopment\s+Area|Redevelopment\s+Project\s+Area)\s+Special\s+Tax\s+Allocation\s+Fund",
        r"special\s+fund\s+(?:to\s+be\s+known\s+as\s+)?(?:entitled|called|known\s+as)?\s+[\"']?(?:\d{4}\s+)?([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)(?:\s+Redevelopment|\s+Tax\s+Increment|\s+Special\s+Tax)",
        r"for\s+the\s+([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)\s+(?:Redevelopment\s+Project\s+Area|Tax\s+Increment\s+Redevelopment\s+Area|TIF)",
        r"([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)\s+Redevelopment\s+Project\s+Area",
        r"([A-Z][A-Za-z0-9&/.,'() -]{2,100}?)\s+Tax\s+Increment\s+(?:Allocation\s+)?(?:Redevelopment\s+)?(?:Area|Bonds?|Notes?)",
    ]
    names = []
    for pattern in patterns:
        for match in re.finditer(pattern, scan_text, flags=re.I):
            name = clean_district_name(match.group(1))
            if name:
                names.append(name)
    return unique_join(names, limit=12)


def parse_dollar_amount(value):
    text = str(value or "").lower().replace(",", "")
    match = re.search(r"\$\s*(\d+(?:\.\d+)?)\s*(million|billion)?", text)
    if not match:
        return None
    amount = float(match.group(1))
    if match.group(2) == "million":
        amount *= 1_000_000
    elif match.group(2) == "billion":
        amount *= 1_000_000_000
    return int(round(amount))


def extract_not_to_exceed_amounts(text):
    amounts = []
    for match in re.finditer(r"not\s+to\s+exceed\s+(\$\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:million|billion))?)", text or "", flags=re.I):
        amounts.append(match.group(1))
    return unique_join(amounts, limit=8)


def largest_dollar_amount(text):
    values = [parse_dollar_amount(match.group(0)) for match in DOLLAR_RE.finditer(text or "")]
    values = [value for value in values if value is not None]
    return max(values) if values else ""


def funding_amount_guess(text):
    values = []
    for amount in extract_not_to_exceed_amounts(text).split(";"):
        value = parse_dollar_amount(amount)
        if value is not None:
            values.append(value)
    for match in DOLLAR_RE.finditer(text or ""):
        start = max(0, match.start() - 260)
        end = min(len(text or ""), match.end() + 260)
        context = (text or "")[start:end]
        if not FUNDING_CONTEXT_RE.search(context):
            continue
        value = parse_dollar_amount(match.group(0))
        if value is not None:
            values.append(value)
    return max(values) if values else ""


def funding_context_snippet(text, max_chars=700):
    if not text:
        return ""
    patterns = [
        r"not\s+to\s+exceed\s+\$\s*\d",
        r"principal\s+amount",
        r"tax\s+increment\s+(?:allocation\s+)?(?:bonds?|notes?)",
        r"public\s+investment",
        r"private\s+investment",
        r"project\s+costs?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            start = max(0, match.start() - max_chars // 2)
            end = min(len(text), match.end() + max_chars // 2)
            return clean_space(("... " if start else "") + text[start:end] + (" ..." if end < len(text) else ""))
    return ""


def infer_district_event_type(text, candidate_event_type):
    lower = (text or "").lower()
    if "intent to use tax increment" in lower:
        return "district_tif_intent"
    has_plan = re.search(r"redevelopment\s+plan\s+and\s+project|tax\s+increment\s+redevelopment\s+plan", lower)
    has_designation = re.search(r"designated\s+as\s+a\s+redevelopment\s+project\s+area|designation\s+of", lower)
    has_financing = re.search(r"tax\s+increment\s+allocation\s+financing\s+(?:is\s+)?(?:hereby\s+)?adopted|special\s+tax\s+allocation\s+fund", lower)
    if has_plan and has_designation and has_financing:
        return "district_plan_designation_financing_adopted"
    if has_plan:
        return "district_plan_approval"
    if has_designation:
        return "district_area_designation"
    if has_financing:
        return "district_allocation_financing"
    if re.search(r"tax\s+increment.*(?:bonds?|notes?)|(?:bonds?|notes?).*tax\s+increment", lower):
        return "district_bond_or_note_authorization"
    if "amendment_or_extension" in candidate_event_type:
        return "district_amendment_or_extension"
    return "district_tif_reference"


def infer_initial_or_revision(text, event_type):
    lower = (text or "").lower()
    if event_type == "district_tif_intent":
        return "initial_intent"
    if re.search(r"\bamend(?:ment|ed|ing)?\b|\bexten(?:sion|ded|ding)\b|substitut", lower):
        return "revised"
    if event_type in {
        "district_plan_designation_financing_adopted",
        "district_plan_approval",
        "district_area_designation",
        "district_allocation_financing",
    }:
        return "initial"
    return ""


def is_district_legislation_window(text, candidate):
    event = candidate.get("event_type_guess", "")
    terms = candidate.get("matched_terms", "")
    lower = (text or "").lower()
    if "district_plan_designation_or_financing" in event:
        return True
    if any(
        phrase in lower
        for phrase in [
            "tax increment allocation financing",
            "redevelopment plan and project",
            "special tax allocation fund",
            "redevelopment project area",
            "tax increment redevelopment area",
        ]
    ):
        return True
    if "tax increment" in terms.lower() and re.search(r"designation|project\s+area|plan\s+and\s+project", lower):
        return True
    return False


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


def load_district_match_rows(pipeline_output_dir):
    rows = []
    for row in read_csv(pipeline_output_dir / "tif_district_universe.csv"):
        tif_number = row.get("tif_number", "")
        variants = [
            row.get("canonical_tif_district", ""),
            *str(row.get("district_name_variants", "")).split(";"),
        ]
        variant_values = []
        variant_seen = set()
        for name in variants:
            variant = clean_space(name)
            variant_key = norm_text(variant)
            if not variant or variant_key in variant_seen:
                continue
            variant_seen.add(variant_key)
            variant_values.append(variant)
        keys = {district_match_key(name) for name in variant_values if clean_space(name)}
        keys = {key for key in keys if key}
        loose_keys = {district_loose_match_key(name) for name in variant_values if clean_space(name)}
        loose_keys = {
            key
            for key in loose_keys
            if key and (len(key.split()) >= 2 or re.search(r"\d", key))
        }
        if not tif_number or not keys:
            continue
        rows.append(
            {
                "tif_number": tif_number,
                "canonical_tif_district": row.get("canonical_tif_district", ""),
                "canonical_key": district_match_key(row.get("canonical_tif_district", "")),
                "canonical_loose_key": district_loose_match_key(row.get("canonical_tif_district", "")),
                "variants": variant_values,
                "keys": keys,
                "loose_keys": loose_keys,
                "first_year": row.get("first_year", ""),
                "last_year": row.get("last_year", ""),
            }
        )
    return rows


def find_known_district_names(text, district_rows):
    normalized_text = f" {norm_text(text)} "
    if not normalized_text.strip():
        return []
    found = []
    seen = set()
    for row in district_rows:
        for variant in row.get("variants", []):
            variant_norm = norm_text(variant)
            if not variant_norm:
                continue
            variant_words = variant_norm.split()
            if len(variant_words) == 1 and len(variant_words[0]) < 6 and not re.search(r"\d", variant_words[0]):
                continue
            if f" {variant_norm} " not in normalized_text:
                continue
            key = row["tif_number"]
            if key in seen:
                continue
            seen.add(key)
            found.append(row.get("canonical_tif_district") or variant)
            break
    return found


def best_district_match(name, district_rows):
    query = district_match_key(name)
    if not query or not district_rows:
        return {"status": "unmatched", "score": "", "tif_number": "", "canonical_tif_district": "", "first_year": "", "last_year": ""}

    query_tokens = set(query.split())
    query_loose = district_loose_match_key(name)
    query_loose_ok = bool(query_loose and (len(query_loose.split()) >= 2 or re.search(r"\d", query_loose)))
    best = None
    for row in district_rows:
        canonical_key = row.get("canonical_key", "")
        canonical_loose_key = row.get("canonical_loose_key", "")
        if query == canonical_key:
            score = 1.02
            if best is None or score > best[0]:
                best = (score, row)
        if query_loose_ok and query_loose == canonical_loose_key:
            score = 0.99
            if best is None or score > best[0]:
                best = (score, row)
        for key in row["keys"]:
            key_tokens = set(key.split())
            if query == key:
                score = 1.0
            elif len(query_tokens) >= 2 and len(key_tokens) >= 2 and (
                f" {query} " in f" {key} " or f" {key} " in f" {query} "
            ):
                score = 0.93
            else:
                seq = SequenceMatcher(None, query, key).ratio()
                jaccard = (len(query_tokens & key_tokens) / len(query_tokens | key_tokens)) if query_tokens and key_tokens else 0.0
                score = max(seq, jaccard)
            if best is None or score > best[0]:
                best = (score, row)
        if query_loose_ok and query_loose in row.get("loose_keys", set()):
            score = 0.98
            if best is None or score > best[0]:
                best = (score, row)

    if best is None:
        return {"status": "unmatched", "score": "", "tif_number": "", "canonical_tif_district": "", "first_year": "", "last_year": ""}
    score, row = best
    if score >= 0.999:
        status = "exact"
    elif score >= 0.86:
        status = "strong_fuzzy"
    elif score >= 0.72:
        status = "fuzzy_review"
    else:
        status = "unmatched"
    if status == "unmatched":
        return {"status": status, "score": f"{score:.3f}", "tif_number": "", "canonical_tif_district": "", "first_year": "", "last_year": ""}
    return {
        "status": status,
        "score": f"{score:.3f}",
        "tif_number": row["tif_number"],
        "canonical_tif_district": row["canonical_tif_district"],
        "first_year": row.get("first_year", ""),
        "last_year": row.get("last_year", ""),
    }


def load_window_text(candidate, page_lookup):
    key = candidate.get("journal_pdf", "")
    start = safe_int(candidate.get("start_page"))
    end = safe_int(candidate.get("end_page"))
    if start is None or end is None:
        return ""
    texts = []
    for page_num in range(start, end + 1):
        text_path = page_lookup.get((key, page_num))
        if text_path:
            path = Path(text_path)
            if path.exists():
                texts.append(path.read_text(encoding="utf-8", errors="ignore"))
    return "\n\n".join(texts)


def add_district_candidate_row(out_rows, candidate, district_name, window_text, district_rows, review_status="", source="candidate"):
    name = clean_district_name(district_name)
    if not name:
        return
    key = district_match_key(name)
    if key in DISTRICT_NAME_STOP_KEYS:
        return
    match = best_district_match(name, district_rows)
    event_type = infer_district_event_type(window_text, candidate.get("event_type_guess", ""))
    initial_or_revision = infer_initial_or_revision(window_text, event_type)
    meeting_date = candidate.get("meeting_date") or candidate.get("source_meeting_date", "")
    meeting_year = safe_int(str(meeting_date)[:4])
    existing_first_year = safe_int(match.get("first_year", ""))
    matched_before_existing = (
        1 if meeting_year is not None and existing_first_year is not None and meeting_year < existing_first_year else 0
    )
    scan_text = (window_text or candidate.get("suggested_dollar_amounts", ""))[:180_000]
    max_amount = funding_amount_guess(scan_text)
    priority = "medium"
    if match["status"] == "unmatched" and initial_or_revision.startswith("initial"):
        priority = "highest"
    elif matched_before_existing and initial_or_revision.startswith("initial"):
        priority = "highest"
    elif initial_or_revision.startswith("initial"):
        priority = "high"
    elif max_amount:
        priority = "high"
    elif match["status"] in {"unmatched", "fuzzy_review"}:
        priority = "high"

    row_id = f"{candidate.get('candidate_id', '')}#{slugify(name, 70)}"
    out_rows.append(
        {
            "district_candidate_id": row_id,
            "candidate_id": candidate.get("candidate_id", ""),
            "source": source,
            "review_status": review_status or candidate.get("review_status", ""),
            "district_event_type_guess": event_type,
            "initial_or_revision_guess": initial_or_revision,
            "meeting_date": meeting_date,
            "meeting_year": str(meeting_year or ""),
            "journal_pdf": candidate.get("journal_pdf", ""),
            "journal_url": candidate.get("journal_url", ""),
            "source_start_page": candidate.get("start_page") or candidate.get("source_start_page", ""),
            "source_end_page": candidate.get("end_page") or candidate.get("source_end_page", ""),
            "matched_terms": candidate.get("matched_terms") or candidate.get("source_matched_terms", ""),
            "district_name_raw": name,
            "district_name_key": key,
            "matched_tif_number": match["tif_number"],
            "matched_canonical_tif_district": match["canonical_tif_district"],
            "district_match_status": match["status"],
            "district_match_score": match["score"],
            "matched_universe_first_year": match["first_year"],
            "matched_universe_last_year": match["last_year"],
            "matched_before_existing_universe": matched_before_existing,
            "suggested_dollar_amounts": extract_dollars(scan_text) or candidate.get("suggested_dollar_amounts", ""),
            "not_to_exceed_amounts": extract_not_to_exceed_amounts(scan_text),
            "max_dollar_amount": max_amount,
            "funding_context_snippet": funding_context_snippet(scan_text),
            "snippet": funding_context_snippet(scan_text) or candidate.get("snippet") or candidate.get("source_snippet", "") or clean_space(scan_text[:650]),
            "review_priority": priority,
        }
    )


def build_district_legislation_candidates(output_dir, pipeline_output_dir, page_rows, candidates, review_rows):
    district_rows = load_district_match_rows(pipeline_output_dir)
    page_lookup = {}
    for row in page_rows:
        path = row.get("text_path", "")
        if path:
            page_lookup[(row.get("journal_pdf", ""), safe_int(row.get("pdf_page")))] = path

    review_by_id = {row.get("candidate_id", ""): row for row in review_rows}
    out_rows = []
    for candidate in candidates:
        prefilter_text = " ".join(
            [
                candidate.get("event_type_guess", ""),
                candidate.get("matched_terms", ""),
                candidate.get("snippet", ""),
                candidate.get("suggested_tif_district", ""),
            ]
        )
        if not is_district_legislation_window(prefilter_text, candidate):
            continue
        window_text = load_window_text(candidate, page_lookup)
        scan_text = (window_text or prefilter_text)[:180_000]
        if not is_district_legislation_window(scan_text, candidate):
            continue
        name_values = []
        for name in find_known_district_names(scan_text, district_rows):
            if name:
                name_values.append(name)
        extracted_names = extract_district_name_candidates(scan_text)
        for name in str(extracted_names or "").split(";"):
            if clean_space(name):
                name_values.append(name)
        if not name_values:
            for name in str(candidate.get("suggested_tif_district", "") or "").split(";"):
                if clean_space(name):
                    name_values.append(name)
        review_status = review_by_id.get(candidate.get("candidate_id", ""), {}).get("review_status", candidate.get("review_status", ""))
        name_seen = set()
        for name in name_values:
            key = district_match_key(name)
            if not key or key in name_seen:
                continue
            name_seen.add(key)
            add_district_candidate_row(out_rows, candidate, name, scan_text, district_rows, review_status)

    existing_row_ids = {row["district_candidate_id"] for row in out_rows}
    for row in review_rows:
        if not is_confirmed(row) or not row.get("district_name"):
            continue
        text = " ".join(
            [
                row.get("source_snippet", ""),
                row.get("funding_raw_text", ""),
                row.get("event_type", ""),
                row.get("district_name", ""),
            ]
        )
        pseudo_candidate = {
            "candidate_id": row.get("candidate_id", ""),
            "event_type_guess": row.get("event_type", ""),
            "meeting_date": row.get("timeline_date") or row.get("source_meeting_date", ""),
            "journal_pdf": row.get("journal_pdf", ""),
            "journal_url": row.get("journal_url", ""),
            "start_page": row.get("source_start_page", ""),
            "end_page": row.get("source_end_page", ""),
            "matched_terms": row.get("source_matched_terms", ""),
            "suggested_dollar_amounts": row.get("funding_raw_text", ""),
            "snippet": row.get("source_snippet", ""),
        }
        before = len(out_rows)
        add_district_candidate_row(out_rows, pseudo_candidate, row.get("district_name", ""), text, district_rows, row.get("review_status", ""), "confirmed_fact")
        if len(out_rows) > before and out_rows[-1]["district_candidate_id"] in existing_row_ids:
            out_rows.pop()
        elif len(out_rows) > before:
            existing_row_ids.add(out_rows[-1]["district_candidate_id"])

    out_rows.sort(
        key=lambda r: (
            {"highest": 0, "high": 1, "medium": 2, "low": 3}.get(r.get("review_priority", ""), 9),
            r.get("meeting_date", ""),
            r.get("district_name_key", ""),
            r.get("candidate_id", ""),
        )
    )
    candidate_fields = [
        "district_candidate_id",
        "candidate_id",
        "source",
        "review_status",
        "district_event_type_guess",
        "initial_or_revision_guess",
        "meeting_date",
        "meeting_year",
        "journal_pdf",
        "journal_url",
        "source_start_page",
        "source_end_page",
        "matched_terms",
        "district_name_raw",
        "district_name_key",
        "matched_tif_number",
        "matched_canonical_tif_district",
        "district_match_status",
        "district_match_score",
        "matched_universe_first_year",
        "matched_universe_last_year",
        "matched_before_existing_universe",
        "suggested_dollar_amounts",
        "not_to_exceed_amounts",
        "max_dollar_amount",
        "funding_context_snippet",
        "snippet",
        "review_priority",
    ]
    write_csv(output_dir / "tif_district_legislation_candidates.csv", out_rows, candidate_fields)

    grouped = {}
    for row in out_rows:
        group_key = row.get("matched_tif_number") or f"unmatched:{row.get('district_name_key')}"
        grouped.setdefault(group_key, []).append(row)

    rollup_rows = []
    for group_key, rows in grouped.items():
        rows_sorted = sorted(rows, key=lambda r: (r.get("meeting_date", ""), r.get("candidate_id", "")))
        initial_rows = [r for r in rows_sorted if str(r.get("initial_or_revision_guess", "")).startswith("initial")]
        funding_rows = [r for r in rows_sorted if safe_int(r.get("max_dollar_amount")) is not None]
        first_initial = initial_rows[0] if initial_rows else rows_sorted[0]
        first_funding = funding_rows[0] if funding_rows else {}
        largest_funding = max(funding_rows, key=lambda r: safe_int(r.get("max_dollar_amount")) or 0) if funding_rows else {}
        variants = sorted({r.get("district_name_raw", "") for r in rows if r.get("district_name_raw")}, key=str.lower)
        match_statuses = sorted({r.get("district_match_status", "") for r in rows if r.get("district_match_status")})
        priorities = [r.get("review_priority", "") for r in rows]
        priority_rank = {"highest": 0, "high": 1, "medium": 2, "low": 3}
        review_priority = min(priorities, key=lambda x: priority_rank.get(x, 9)) if priorities else ""
        rollup_rows.append(
            {
                "district_legislation_key": group_key,
                "matched_tif_number": first_initial.get("matched_tif_number", ""),
                "matched_canonical_tif_district": first_initial.get("matched_canonical_tif_district", ""),
                "district_name_variants": "; ".join(variants),
                "district_name_keys": "; ".join(sorted({r.get("district_name_key", "") for r in rows if r.get("district_name_key", "")})),
                "district_match_statuses": "; ".join(match_statuses),
                "first_journal_date": rows_sorted[0].get("meeting_date", ""),
                "first_initial_date_guess": first_initial.get("meeting_date", ""),
                "first_initial_event_type_guess": first_initial.get("district_event_type_guess", ""),
                "first_initial_candidate_id": first_initial.get("candidate_id", ""),
                "first_initial_source_pages": f"{first_initial.get('source_start_page', '')}-{first_initial.get('source_end_page', '')}",
                "first_funding_date_guess": first_funding.get("meeting_date", ""),
                "first_funding_amount_guess": first_funding.get("max_dollar_amount", ""),
                "first_funding_candidate_id": first_funding.get("candidate_id", ""),
                "largest_funding_amount_guess": largest_funding.get("max_dollar_amount", ""),
                "largest_funding_candidate_id": largest_funding.get("candidate_id", ""),
                "candidate_rows": len(rows),
                "confirmed_fact_rows": sum(1 for r in rows if r.get("source") == "confirmed_fact" or is_confirmed(r)),
                "unmatched_candidate_rows": sum(1 for r in rows if r.get("district_match_status") == "unmatched"),
                "review_priority": review_priority,
                "first_initial_snippet": first_initial.get("snippet", ""),
            }
        )

    rollup_rows.sort(
        key=lambda r: (
            {"highest": 0, "high": 1, "medium": 2, "low": 3}.get(r.get("review_priority", ""), 9),
            r.get("first_initial_date_guess", "") or r.get("first_journal_date", ""),
            r.get("district_legislation_key", ""),
        )
    )
    rollup_fields = [
        "district_legislation_key",
        "matched_tif_number",
        "matched_canonical_tif_district",
        "district_name_variants",
        "district_name_keys",
        "district_match_statuses",
        "first_journal_date",
        "first_initial_date_guess",
        "first_initial_event_type_guess",
        "first_initial_candidate_id",
        "first_initial_source_pages",
        "first_funding_date_guess",
        "first_funding_amount_guess",
        "first_funding_candidate_id",
        "largest_funding_amount_guess",
        "largest_funding_candidate_id",
        "candidate_rows",
        "confirmed_fact_rows",
        "unmatched_candidate_rows",
        "review_priority",
        "first_initial_snippet",
    ]
    write_csv(output_dir / "tif_district_legislation_rollup_pre2010.csv", rollup_rows, rollup_fields)
    return out_rows, rollup_rows


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
    district_candidate_rows, district_rollup_rows = build_district_legislation_candidates(
        output_dir,
        pipeline_output_dir,
        all_page_rows,
        all_candidates,
        review_rows,
    )
    summary_rows = write_summary(output_dir, journal_rows, all_page_rows, all_candidates, review_rows, district_rows, project_rows_out)
    summary_rows.extend(
        [
            {"metric": "district_legislation_candidate_rows", "value": len(district_candidate_rows)},
            {"metric": "district_legislation_rollup_rows", "value": len(district_rollup_rows)},
        ]
    )
    write_csv(output_dir / "tif_journal_legislation_summary.csv", summary_rows, ["metric", "value"])

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
