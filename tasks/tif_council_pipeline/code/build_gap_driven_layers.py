#!/usr/bin/env python3
import argparse
import csv
import os
import re
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

try:
    import fitz
except Exception:
    fitz = None


def parse_args():
    p = argparse.ArgumentParser(description="Build gap-driven TIF collection and validation layers")
    p.add_argument("--input-dir", default="../input")
    p.add_argument("--output-dir", default="../output")
    p.add_argument("--config-dir", default="../config")
    p.add_argument("--legacy-start-year", type=int, default=2010)
    p.add_argument("--legacy-end-year", type=int, default=2016)
    p.add_argument("--max-legacy-pdfs", type=int, default=0, help="0 means all legacy annual-report PDFs")
    p.add_argument("--ocr-search-start-page", type=int, default=12)
    p.add_argument("--ocr-search-end-page", type=int, default=18)
    return p.parse_args()


def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)


def safe_int(value):
    if value is None:
        return None
    m = re.search(r"-?\d+", str(value))
    return int(m.group(0)) if m else None


def safe_float(value):
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    if s == "":
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def normalize_tif_number(value):
    if value is None:
        return ""
    s = str(value).strip().upper()
    if s.startswith("T-T-"):
        s = "T-" + s[4:]
    m = re.search(r"(\d{1,3})", s)
    if m:
        return f"T-{int(m.group(1)):03d}"
    return s


def norm_text(value):
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def slugify(value, max_len=64):
    s = norm_text(value).replace(" ", "-")
    s = re.sub(r"-+", "-", s).strip("-")
    if s == "":
        s = "item"
    return s[:max_len]


def read_csv(path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def load_metric_map(path):
    out = {}
    for row in read_csv(path):
        metric = (row.get("metric") or "").strip()
        if metric:
            out[metric] = row.get("value")
    return out


def choose_boundary_for_year(boundaries, year):
    if not boundaries:
        return None
    if year is not None:
        valid = []
        for b in boundaries:
            start_year = safe_int(b.get("approval_d")) or 0
            end_year = safe_int(b.get("repealed_d"))
            if end_year is None:
                end_year = safe_int(b.get("expiration")) or 9999
            if start_year <= year <= end_year:
                valid.append(b)
        if valid:
            valid.sort(
                key=lambda r: (
                    safe_int(r.get("approval_d")) or 0,
                    safe_float(r.get("shape_area")) or 0.0,
                ),
                reverse=True,
            )
            return valid[0]
    ranked = sorted(
        boundaries,
        key=lambda r: (
            1 if not (r.get("repealed_d") or "").strip() else 0,
            safe_int(r.get("approval_d")) or 0,
            safe_float(r.get("shape_area")) or 0.0,
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def parse_http_status(error_text):
    m = re.search(r"\bhttp\s+(\d{3})\b", str(error_text or ""), flags=re.I)
    return m.group(1) if m else ""


def is_truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "t"}


def list_pdf_paths(root):
    if not root.exists():
        return []
    return sorted([p for p in root.rglob("*.pdf") if p.is_file()])


def ensure_config_templates(config_dir):
    ensure_dir(config_dir)

    project_name_overrides = config_dir / "project_name_overrides.csv"
    if not project_name_overrides.exists():
        write_csv(
            project_name_overrides,
            [],
            [
                "active",
                "tif_number",
                "raw_project_name",
                "canonical_project_id",
                "canonical_project_name",
                "master_id",
                "notes",
            ],
        )

    matter_link_overrides = config_dir / "matter_link_overrides.csv"
    if not matter_link_overrides.exists():
        write_csv(
            matter_link_overrides,
            [],
            [
                "active",
                "canonical_project_id",
                "matter_source",
                "matter_id",
                "attachment_id",
                "record_number",
                "document_role",
                "notes",
            ],
        )

    known_missing_documents = config_dir / "known_missing_documents.csv"
    if not known_missing_documents.exists():
        write_csv(
            known_missing_documents,
            [
                {
                    "active": 1,
                    "source": "annual_report",
                    "source_id": "annual_report|2023|T-067|archer-courts",
                    "year": 2023,
                    "tif_number": "T-067",
                    "tif_district": "Archer Courts",
                    "url": "https://www.chicago.gov/content/dam/city/depts/dcd/tif/23reports/T_067_ArcherCourtsAR23.pdf",
                    "reason": "known_404",
                    "notes": "Known missing Archer Courts annual report link",
                },
                {
                    "active": 1,
                    "source": "annual_report",
                    "source_id": "annual_report|2024|T-067|archer-courts",
                    "year": 2024,
                    "tif_number": "T-067",
                    "tif_district": "Archer Courts",
                    "url": "https://www.chicago.gov/content/dam/city/depts/dcd/tif/24reports/T_067_ArcherCourtsAR24.pdf",
                    "reason": "known_404",
                    "notes": "Known missing Archer Courts annual report link",
                },
            ],
            ["active", "source", "source_id", "year", "tif_number", "tif_district", "url", "reason", "notes"],
        )

    return {
        "project_name_overrides": read_csv(project_name_overrides),
        "matter_link_overrides": read_csv(matter_link_overrides),
        "known_missing_documents": read_csv(known_missing_documents),
    }


def build_document_inventory(input_dir, output_dir, config_rows):
    existing_path = output_dir / "tif_document_inventory.csv"
    existing = {}
    for row in read_csv(existing_path):
        key = (row.get("source", ""), row.get("source_id", ""), row.get("url", ""))
        existing[key] = row

    now_run = datetime.now().strftime("%Y%m%dT%H%M%S")
    inventory = {}

    annual_links = read_csv(output_dir / "tif_annual_report_pdf_links.csv")
    annual_status = {
        row.get("pdf_url", ""): row for row in read_csv(output_dir / "tif_annual_report_pdf_download_status.csv")
    }
    annual_files = {str(p.resolve()): p for p in list_pdf_paths(input_dir / "annual_reports" / "pdf")}

    for row in annual_links:
        url = row.get("pdf_url", "")
        year = safe_int(row.get("year"))
        tif_match = re.search(r"T[_-](\d{1,3})", url, flags=re.I)
        tif_number = f"T-{int(tif_match.group(1)):03d}" if tif_match else ""
        status = annual_status.get(url, {})
        local_path = status.get("local_path", "")
        if local_path == "" and status.get("status") == "already_exists":
            local_path = status.get("local_path", "")
        source_id = f"annual_report|{year or ''}|{tif_number}|{slugify(Path(url).stem)}"
        key = ("annual_report", source_id, url)
        prior = existing.get(key, {})
        known_missing = next(
            (
                r for r in config_rows["known_missing_documents"]
                if is_truthy(r.get("active")) and r.get("source") == "annual_report" and r.get("url") == url
            ),
            None,
        )
        download_status = status.get("status") or ("already_exists" if local_path else "discovered")
        http_status = parse_http_status(status.get("error", ""))
        note = status.get("error", "")
        if known_missing is not None:
            download_status = "known_missing"
            http_status = http_status or "404"
            note = known_missing.get("reason") or note
        inventory[key] = {
            "source": "annual_report",
            "source_id": source_id,
            "document_kind": "annual_report_pdf",
            "year": year,
            "tif_number": tif_number,
            "tif_district": "",
            "matter_source": "",
            "matter_id": "",
            "attachment_id": "",
            "record_number": "",
            "url": url,
            "local_path": local_path,
            "download_status": download_status,
            "http_status": http_status,
            "content_type": "application/pdf",
            "discovered_via": "annual_report_index",
            "first_seen_run": prior.get("first_seen_run", now_run),
            "last_checked_run": now_run,
            "known_missing": 1 if known_missing is not None else 0,
            "note": note,
        }

    elms_status_by_key = {}
    for row in read_csv(output_dir / "tif_elms_pdf_download_status.csv"):
        key = (row.get("matter_id", ""), row.get("attachment_uid", ""))
        elms_status_by_key[key] = row

    for row in read_csv(output_dir / "tif_elms_attachments.csv"):
        if safe_int(row.get("is_pdf")) != 1:
            continue
        matter_id = row.get("matter_id", "")
        attachment_id = row.get("attachment_uid", "")
        status = elms_status_by_key.get((matter_id, attachment_id), {})
        url = row.get("attachment_url", "")
        source_id = f"elms|{matter_id}|{attachment_id}"
        key = ("elms", source_id, url)
        prior = existing.get(key, {})
        inventory[key] = {
            "source": "elms",
            "source_id": source_id,
            "document_kind": "attachment_pdf",
            "year": "",
            "tif_number": "",
            "tif_district": "",
            "matter_source": "elms",
            "matter_id": matter_id,
            "attachment_id": attachment_id,
            "record_number": row.get("record_number", ""),
            "url": url,
            "local_path": status.get("local_path", ""),
            "download_status": status.get("status") or "discovered",
            "http_status": parse_http_status(status.get("error", "")),
            "content_type": "application/pdf",
            "discovered_via": "elms_attachment_detail",
            "first_seen_run": prior.get("first_seen_run", now_run),
            "last_checked_run": now_run,
            "known_missing": 0,
            "note": status.get("error", ""),
        }

    leg_status_by_key = {}
    for row in read_csv(output_dir / "tif_pdf_download_status.csv"):
        key = (str(row.get("MatterId", "")), str(row.get("MatterAttachmentId", "")))
        leg_status_by_key[key] = row

    for row in read_csv(output_dir / "tif_attachments.csv"):
        file_name = (row.get("MatterAttachmentFileName") or "").lower()
        link = (row.get("MatterAttachmentHyperlink") or "").strip()
        if ".pdf" not in file_name and ".pdf" not in link.lower():
            continue
        matter_id = str(row.get("MatterId", ""))
        attachment_id = str(row.get("MatterAttachmentId", ""))
        status = leg_status_by_key.get((matter_id, attachment_id), {})
        url = row.get("MatterAttachmentHyperlink", "")
        source_id = f"legistar|{matter_id}|{attachment_id}"
        key = ("legistar", source_id, url)
        prior = existing.get(key, {})
        inventory[key] = {
            "source": "legistar",
            "source_id": source_id,
            "document_kind": "attachment_pdf",
            "year": "",
            "tif_number": "",
            "tif_district": "",
            "matter_source": "legistar",
            "matter_id": matter_id,
            "attachment_id": attachment_id,
            "record_number": row.get("MatterFile", ""),
            "url": url,
            "local_path": "",
            "download_status": status.get("status") or "discovered",
            "http_status": parse_http_status(status.get("error", "")),
            "content_type": "application/pdf",
            "discovered_via": "legistar_attachment_api",
            "first_seen_run": prior.get("first_seen_run", now_run),
            "last_checked_run": now_run,
            "known_missing": 0,
            "note": status.get("error", ""),
        }

    rows = sorted(inventory.values(), key=lambda r: (r["source"], r["source_id"], r["url"]))
    fieldnames = [
        "source", "source_id", "document_kind", "year", "tif_number", "tif_district",
        "matter_source", "matter_id", "attachment_id", "record_number", "url", "local_path",
        "download_status", "http_status", "content_type", "discovered_via", "first_seen_run",
        "last_checked_run", "known_missing", "note",
    ]
    write_csv(existing_path, rows, fieldnames)
    return rows


def build_inventory_summary(document_inventory, input_dir, output_dir):
    counts = Counter((row.get("source", ""), row.get("download_status", "")) for row in document_inventory)
    annual_files_on_disk = len(list_pdf_paths(input_dir / "annual_reports" / "pdf"))
    elms_files_on_disk = len(list_pdf_paths(input_dir / "elms" / "pdf"))
    legistar_files_on_disk = len(list_pdf_paths(input_dir / "legistar" / "pdf"))

    rows = [
        {"metric": "document_inventory_rows", "value": len(document_inventory)},
        {"metric": "annual_report_pdf_inventory_rows", "value": sum(1 for r in document_inventory if r.get("source") == "annual_report")},
        {"metric": "annual_report_pdf_known_missing", "value": sum(1 for r in document_inventory if r.get("source") == "annual_report" and safe_int(r.get("known_missing")) == 1)},
        {"metric": "annual_report_pdf_files_on_disk", "value": annual_files_on_disk},
        {"metric": "elms_pdf_inventory_rows", "value": sum(1 for r in document_inventory if r.get("source") == "elms")},
        {"metric": "elms_pdf_files_on_disk", "value": elms_files_on_disk},
        {"metric": "legistar_pdf_inventory_rows", "value": sum(1 for r in document_inventory if r.get("source") == "legistar")},
        {"metric": "legistar_pdf_files_on_disk", "value": legistar_files_on_disk},
    ]
    for (source, status), count in sorted(counts.items()):
        rows.append({"metric": f"inventory_{source}_{slugify(status, 40)}", "value": count})
    write_csv(output_dir / "tif_document_inventory_summary.csv", rows, ["metric", "value"])
    return rows


def build_matter_inventory(output_dir):
    rows = []
    seen = set()

    for row in read_csv(output_dir / "tif_elms_matters.csv"):
        key = ("elms", row.get("matter_id", ""))
        if key in seen or key[1] == "":
            continue
        seen.add(key)
        rows.append(
            {
                "matter_source": "elms",
                "matter_id": row.get("matter_id", ""),
                "record_number": row.get("record_number", ""),
                "title": row.get("title", ""),
                "short_title": row.get("short_title", ""),
                "matter_type": row.get("type", ""),
                "status": row.get("status", ""),
                "file_year": row.get("file_year", ""),
                "detail_fetched": row.get("detail_fetched", ""),
                "keyword_hits": row.get("keyword_hits", ""),
            }
        )

    for row in read_csv(output_dir / "tif_matters.csv"):
        key = ("legistar", str(row.get("MatterId", "")))
        if key in seen or key[1] == "":
            continue
        seen.add(key)
        rows.append(
            {
                "matter_source": "legistar",
                "matter_id": str(row.get("MatterId", "")),
                "record_number": row.get("MatterFile", ""),
                "title": row.get("MatterTitle", ""),
                "short_title": row.get("MatterName", ""),
                "matter_type": row.get("MatterTypeName", ""),
                "status": row.get("MatterStatusName", ""),
                "file_year": safe_int(row.get("MatterIntroDate", "")),
                "detail_fetched": 1,
                "keyword_hits": "",
            }
        )

    write_csv(
        output_dir / "tif_matter_inventory.csv",
        rows,
        [
            "matter_source", "matter_id", "record_number", "title", "short_title",
            "matter_type", "status", "file_year", "detail_fetched", "keyword_hits",
        ],
    )
    return rows


def build_attachment_inventory(output_dir):
    rows = []
    seen = set()

    for row in read_csv(output_dir / "tif_elms_attachments.csv"):
        key = ("elms", row.get("matter_id", ""), row.get("attachment_uid", ""))
        if key in seen or key[1] == "" or key[2] == "":
            continue
        seen.add(key)
        rows.append(
            {
                "matter_source": "elms",
                "matter_id": row.get("matter_id", ""),
                "attachment_id": row.get("attachment_uid", ""),
                "record_number": row.get("record_number", ""),
                "attachment_name": row.get("attachment_name", ""),
                "attachment_file": row.get("attachment_file", ""),
                "attachment_url": row.get("attachment_url", ""),
                "is_pdf": row.get("is_pdf", ""),
            }
        )

    for row in read_csv(output_dir / "tif_attachments.csv"):
        key = ("legistar", str(row.get("MatterId", "")), str(row.get("MatterAttachmentId", "")))
        if key in seen or key[1] == "" or key[2] == "":
            continue
        seen.add(key)
        rows.append(
            {
                "matter_source": "legistar",
                "matter_id": str(row.get("MatterId", "")),
                "attachment_id": str(row.get("MatterAttachmentId", "")),
                "record_number": row.get("MatterFile", ""),
                "attachment_name": row.get("MatterAttachmentName", ""),
                "attachment_file": row.get("MatterAttachmentFileName", ""),
                "attachment_url": row.get("MatterAttachmentHyperlink", ""),
                "is_pdf": 1 if ".pdf" in (row.get("MatterAttachmentFileName", "") or "").lower() else 0,
            }
        )

    write_csv(
        output_dir / "tif_attachment_inventory.csv",
        rows,
        [
            "matter_source", "matter_id", "attachment_id", "record_number",
            "attachment_name", "attachment_file", "attachment_url", "is_pdf",
        ],
    )
    return rows


COMMON_NAME_REPLACEMENTS = {
    "develo ment": "development",
    "develo pment": "development",
    "im rovement": "improvement",
    "pro· eel": "project",
    "pro'ect": "project",
    "on oin": "ongoing",
    "com lete": "complete",
    "sstimated": "estimated",
}


def clean_legacy_text(text):
    s = text or ""
    for bad, good in COMMON_NAME_REPLACEMENTS.items():
        s = re.sub(re.escape(bad), good, s, flags=re.I)
    s = s.replace("•", " ").replace("|", " ").replace("[", " ").replace("]", " ")
    s = re.sub(r"[_`~]+", " ", s)
    s = re.sub(r"\r", "\n", s)
    return s


def ocr_page(doc, page_index, cache):
    if page_index in cache:
        return cache[page_index]
    if fitz is None:
        cache[page_index] = ""
        return ""
    page = doc.load_page(page_index)
    pix = page.get_pixmap(matrix=fitz.Matrix(1.7, 1.7), alpha=False)
    fd, tmp_name = tempfile.mkstemp(prefix=f"tif_ocr_{page_index}_", suffix=".png")
    os.close(fd)
    Path(tmp_name).unlink(missing_ok=True)
    pix.save(tmp_name)
    proc = subprocess.run(
        ["tesseract", tmp_name, "stdout", "--psm", "6"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    try:
        Path(tmp_name).unlink()
    except Exception:
        pass
    text = proc.stdout if proc.returncode == 0 else ""
    cache[page_index] = text
    return text


def locate_legacy_section_text(pdf_path, report_year, ocr_search_start_page, ocr_search_end_page):
    if fitz is None:
        return "", "missing_fitz", "", 0

    doc = fitz.open(str(pdf_path))
    cache = {}
    page_count = doc.page_count
    start_page = None
    page_method = "text"

    for idx in range(page_count):
        txt = doc.load_page(idx).get_text() or ""
        if (
            re.search(r"SECTION\s*5", txt, flags=re.I)
            or re.search(r"Project\s+1\s*:", txt, flags=re.I)
            or re.search(r"brief\s+description\s+of\s+each\s+project", txt, flags=re.I)
        ):
            start_page = idx
            page_method = "text"
            break

    if start_page is None:
        page_method = "ocr"
        start = max(0, ocr_search_start_page - 1)
        end = min(page_count, ocr_search_end_page)
        for idx in range(start, end):
            txt = ocr_page(doc, idx, cache)
            if (
                re.search(r"SECTION\s*5", txt, flags=re.I)
                or re.search(r"Project\s+1\s*:", txt, flags=re.I)
                or re.search(r"brief\s+description\s+of\s+each\s+project", txt, flags=re.I)
            ):
                start_page = idx
                break

    if start_page is None:
        return "", "not_found", "", 0

    texts = []
    pages_used = []
    for idx in range(start_page, min(page_count, start_page + 3)):
        txt = (doc.load_page(idx).get_text() or "") if page_method == "text" else ocr_page(doc, idx, cache)
        if idx > start_page and re.search(r"Attachment\s+B|CERTIFICATION", txt, flags=re.I):
            break
        texts.append(txt)
        pages_used.append(str(idx + 1))
        if idx > start_page and not re.search(r"Project\s+\d+\s*:", txt, flags=re.I) and not re.search(r"PAGE\s+[23]", txt, flags=re.I):
            break

    return "\n".join(texts), page_method, ",".join(pages_used), len(pages_used)


def infer_project_iga(raw_name):
    low = norm_text(raw_name)
    return "IGA" if low.startswith("iga") or " intergovernmental " in f" {low} " else "Project"


def infer_project_type(raw_name, project_iga):
    low = norm_text(raw_name)
    if "sbif" in low or "small business improvement fund" in low:
        return "Small Business Improvement Fund Program"
    if "tifworks" in low:
        return "TIF Works Program"
    if project_iga == "IGA":
        return "Intergovernmental Agreement"
    return "Redevelopment Agreement"


def clean_project_name(raw_name):
    s = clean_legacy_text(raw_name)
    s = re.sub(r"\s+", " ", s).strip(" -:;")
    return s


def parse_reported_project_total(text):
    patterns = [
        r"ENTER total number of projects.*?below.*?\n\s*(\d{1,3})\b",
        r"list them in detail below.*?\n\s*(\d{1,3})\b",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I | re.S)
        if m:
            return int(m.group(1))
    return None


def parse_tif_district_name(text, pdf_path):
    patterns = [
        r"TIF\s*NAME\s*:\s*(.+?)\s+Redevelopment Project Area",
        r"FY\s*20\d{2}\s+TIF\s*Name\s*:\s*(.+?)\s+Redevelopment Project Area",
        r"\n([A-Za-z0-9/ ,.'\-]+)\s+Redevelopment Project Area",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
    stem = pdf_path.stem
    stem = re.sub(r"^T[_-]\d{3}_", "", stem)
    stem = re.sub(r"AR\d{2,4}$", "", stem)
    stem = re.sub(r"([a-z])([A-Z])", r"\1 \2", stem)
    return stem.strip()


def make_legacy_project_row(
    tif_number,
    tif_district,
    report_year,
    project_number,
    raw_name,
    status,
    ongoing,
    page_method,
    pdf_path,
    section_pages,
    expected_total,
):
    project_iga = infer_project_iga(raw_name)
    project_type = infer_project_type(raw_name, project_iga)
    return {
        "tif_number": tif_number,
        "tif_district": tif_district,
        "report_year": report_year,
        "project_iga": project_iga,
        "project_type": project_type,
        "project_number": project_number,
        "project_name": raw_name,
        "annual_report_name": raw_name,
        "current_year_new_deals": "",
        "ongoing": ongoing,
        "status": status,
        "current_year_payments": "",
        "estimated_next_year_payments": "",
        "private_funds": "",
        "private_funds_to_completion": "",
        "public_funds": "",
        "public_funds_to_completion": "",
        "source": "annual_report_pdf",
        "extraction_method": page_method,
        "annual_report_pdf": str(pdf_path.resolve()),
        "section_pages": section_pages,
        "legacy_expected_project_total": expected_total if expected_total is not None else "",
    }


def parse_legacy_project_rows(section_text, tif_number, report_year, pdf_path, page_method, section_pages):
    text = clean_legacy_text(section_text)
    if text.strip() == "":
        return [], None

    project_word_pattern = r"pro(?:ject|iect)"
    expected_total = parse_reported_project_total(text)
    tif_district = parse_tif_district_name(text, pdf_path)
    project_anchor = re.search(r"Project\s+\d{1,3}\s*:", text, flags=re.I)
    fallback_anchor = re.search(
        rf"No\s+Projects\s+Were\s+Undertaken|[A-Za-z0-9].{{0,140}}{project_word_pattern}\s+(?:is\s+ongoing|ongoing|is\s+complete|completed?|complete)",
        text,
        flags=re.I,
    )
    search_from = project_anchor.start() if project_anchor else (fallback_anchor.start() if fallback_anchor else 0)
    stop_tokens = ["General Notes", "Attachment B", "CERTIFICATION"]
    cut = len(text)
    for token in stop_tokens:
        pos = text.find(token, search_from)
        if pos >= 0:
            cut = min(cut, pos)
    text = text[:cut]

    parts = re.split(r"Project\s+(\d{1,3})\s*:\s*", text, flags=re.I)
    rows = []
    for i in range(1, len(parts), 2):
        project_number = parts[i]
        block = parts[i + 1]
        lines = [re.sub(r"\s+", " ", x).strip() for x in block.splitlines()]
        lines = [x for x in lines if x]
        if not lines:
            continue

        name_parts = []
        status = ""
        ongoing = ""
        for line in lines:
            low = line.lower()
            if low.startswith("private investment undertaken") or low.startswith("public investment undertaken"):
                break
            if low.startswith("ratio of private/public investment"):
                break
            if re.search(rf"{project_word_pattern}\s+is\s+ongoing|ongoing\s+{project_word_pattern}|{project_word_pattern}\s+ongoing", low):
                left = re.split(rf"{project_word_pattern}\s+is|{project_word_pattern}\s+ongoing|ongoing", line, flags=re.I)[0].strip(" -:")
                if left:
                    name_parts.append(left)
                status = "Active Project"
                ongoing = "true"
                break
            if re.search(rf"{project_word_pattern}\s+is\s+complete|{project_word_pattern}\s+complete|completed\s+{project_word_pattern}|complete\s+{project_word_pattern}", low):
                left = re.split(rf"{project_word_pattern}\s+is|{project_word_pattern}\s+complete|completed|complete", line, flags=re.I)[0].strip(" -:")
                if left:
                    name_parts.append(left)
                status = "Completed Project"
                ongoing = "false"
                break
            if re.fullmatch(r"page\s+[23]", low):
                continue
            name_parts.append(line)

        raw_name = clean_project_name(" ".join(name_parts))
        raw_name = re.sub(r"\bPrivate Investment Undertaken.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(rf"\b{project_word_pattern}\s+is\s+ongoing.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(rf"\b{project_word_pattern}\s+ongoing.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(rf"\b{project_word_pattern}\s+is\s+complete.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(rf"\b{project_word_pattern}\s+complete.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(r"\bongoing\b.*$", "", raw_name, flags=re.I).strip()
        raw_name = re.sub(r"\bcompleted?\b.*$", "", raw_name, flags=re.I).strip()
        if raw_name == "" or raw_name.lower() == "see instructions":
            continue

        if status == "":
            status = "Active Project" if re.search(r"ongoing", block, flags=re.I) else "Unknown"
            ongoing = "true" if status == "Active Project" else ""

        rows.append(
            make_legacy_project_row(
                tif_number,
                tif_district,
                report_year,
                project_number,
                raw_name,
                status,
                ongoing,
                page_method,
                pdf_path,
                section_pages,
                expected_total,
            )
        )

    if not rows and not re.search(r"No\s+Projects\s+Were\s+Undertaken", text, flags=re.I):
        lines = [re.sub(r"\s+", " ", x).strip() for x in text.splitlines()]
        lines = [x for x in lines if x]
        fallback_counter = 1
        for line in lines:
            low = line.lower()
            if (
                low.startswith("section 5")
                or low.startswith("please include a brief description")
                or low.startswith('see "general notes"')
                or low.startswith("total")
                or low.startswith("private investment undertaken")
                or low.startswith("public investment undertaken")
                or low.startswith("ratio of private/public investment")
                or low.startswith("general notes")
                or low.startswith("fy 20")
                or low.startswith("page ")
            ):
                continue
            if not re.search(
                rf"{project_word_pattern}\s+is\s+ongoing|{project_word_pattern}\s+ongoing|{project_word_pattern}\s+completed?|{project_word_pattern}\s+complete",
                low,
            ):
                continue

            status = "Active Project" if "ongoing" in low else "Completed Project"
            ongoing = "true" if status == "Active Project" else "false"
            raw_name = clean_project_name(line)
            raw_name = re.split(
                rf"{project_word_pattern}\s+is\s+ongoing|{project_word_pattern}\s+ongoing|{project_word_pattern}\s+is\s+complete|{project_word_pattern}\s+completed?|{project_word_pattern}\s+complete",
                raw_name,
                flags=re.I,
            )[0].strip(" -:;,.")
            raw_name = clean_project_name(raw_name)
            if raw_name == "":
                continue
            rows.append(
                make_legacy_project_row(
                    tif_number,
                    tif_district,
                    report_year,
                    str(fallback_counter),
                    raw_name,
                    status,
                    ongoing,
                    page_method,
                    pdf_path,
                    section_pages,
                    expected_total,
                )
            )
            fallback_counter += 1

    dedup = []
    seen = set()
    for row in rows:
        key = (row["report_year"], row["tif_number"], row["project_number"], norm_text(row["project_name"]))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(row)
    if page_method == "ocr":
        for idx, row in enumerate(dedup, start=1):
            row["project_number"] = str(idx)
    return dedup, expected_total


def extract_legacy_project_rows(input_dir, output_dir, args):
    reports_pdf_dir = input_dir / "annual_reports" / "pdf"
    pdf_paths = []
    for year in range(args.legacy_start_year, args.legacy_end_year + 1):
        year_dir = reports_pdf_dir / str(year)
        if year_dir.exists():
            pdf_paths.extend(sorted([p for p in year_dir.glob("*.pdf") if p.is_file()]))
    if args.max_legacy_pdfs and args.max_legacy_pdfs > 0:
        pdf_paths = pdf_paths[: args.max_legacy_pdfs]

    rows = []
    summary_rows = []
    total_expected = 0
    total_extracted = 0

    for pdf_path in pdf_paths:
        report_year = safe_int(pdf_path.parent.name)
        tif_number = normalize_tif_number(pdf_path.stem)
        section_text, method, pages, page_count = locate_legacy_section_text(
            pdf_path,
            report_year,
            args.ocr_search_start_page,
            args.ocr_search_end_page,
        )
        extracted_rows, expected_total = parse_legacy_project_rows(
            section_text,
            tif_number,
            report_year,
            pdf_path,
            method,
            pages,
        )
        rows.extend(extracted_rows)
        total_extracted += len(extracted_rows)
        total_expected += expected_total or 0
        summary_rows.append(
            {
                "report_year": report_year,
                "tif_number": tif_number,
                "pdf_path": str(pdf_path.resolve()),
                "extraction_method": method,
                "section_pages": pages,
                "section_page_count": page_count,
                "expected_projects": expected_total if expected_total is not None else "",
                "extracted_projects": len(extracted_rows),
                "status": "ok" if extracted_rows else "no_rows",
            }
        )

    fieldnames = [
        "tif_number", "tif_district", "report_year", "project_iga", "project_type", "project_number",
        "project_name", "annual_report_name", "current_year_new_deals", "ongoing", "status",
        "current_year_payments", "estimated_next_year_payments", "private_funds", "private_funds_to_completion",
        "public_funds", "public_funds_to_completion", "source", "extraction_method", "annual_report_pdf",
        "section_pages", "legacy_expected_project_total",
    ]
    write_csv(output_dir / "tif_legacy_annual_report_projects_2010_2016.csv", rows, fieldnames)
    write_csv(
        output_dir / "tif_legacy_annual_report_extract_summary.csv",
        summary_rows,
        [
            "report_year", "tif_number", "pdf_path", "extraction_method", "section_pages", "section_page_count",
            "expected_projects", "extracted_projects", "status",
        ],
    )
    return rows, summary_rows, total_expected, total_extracted


def load_modern_project_rows(input_dir):
    path = input_dir / "socrata" / "tif_annual_report_projects.csv"
    rows = read_csv(path)
    out = []
    for row in rows:
        out.append(
            {
                "tif_number": normalize_tif_number(row.get("tif_number", "")),
                "tif_district": row.get("tif_district", ""),
                "report_year": safe_int(row.get("report_year")),
                "project_iga": row.get("project_iga", ""),
                "project_type": row.get("project_type", ""),
                "project_number": row.get("project_number", ""),
                "project_name": row.get("project_name", ""),
                "annual_report_name": row.get("annual_report_name", row.get("project_name", "")),
                "current_year_new_deals": row.get("current_year_new_deals", ""),
                "ongoing": row.get("ongoing", ""),
                "status": row.get("status", ""),
                "current_year_payments": row.get("current_year_payments", ""),
                "estimated_next_year_payments": row.get("estimated_next_year_payments", ""),
                "private_funds": row.get("private_funds", ""),
                "private_funds_to_completion": row.get("private_funds_to_completion", ""),
                "public_funds": row.get("public_funds", ""),
                "public_funds_to_completion": row.get("public_funds_to_completion", ""),
                "source": "socrata_annual_projects",
                "extraction_method": "structured_download",
                "annual_report_pdf": "",
                "section_pages": "",
                "legacy_expected_project_total": "",
            }
        )
    return out


def simplify_project_name(value):
    s = norm_text(value)
    s = s.replace("small business improvement fund", "sbif")
    s = s.replace("tax increment financing works", "tifworks")
    tokens = []
    drop = {
        "llc", "inc", "corp", "corporation", "company", "co", "ltd", "development", "mixed",
        "use", "program", "project", "redevelopment", "agreement", "district", "city", "of",
        "the", "and", "for", "phase", "area", "fund", "business",
    }
    for token in s.split():
        if token in drop:
            continue
        tokens.append(token)
    return " ".join(tokens)


def simplify_district_name(value):
    s = norm_text(value)
    s = s.replace("street", "st")
    s = s.replace("streeu", "st")
    s = s.replace("avenue", "ave")
    s = s.replace(" and ", " ")
    s = s.replace(" redevelopement ", " ")
    s = s.replace(" redevelopment project area", "")
    s = s.replace(" project area", "")
    return re.sub(r"\s+", " ", s).strip()


def is_informative_search_term(value):
    norm = norm_text(value)
    simple = simplify_project_name(value)
    if norm == "" and simple == "":
        return False
    if re.fullmatch(r"\d+", norm):
        return False
    alpha_tokens = [tok for tok in norm.split() if re.search(r"[a-z]", tok)]
    if not alpha_tokens:
        return False
    generic = {"iga", "cpd", "cbe", "cta", "sbif", "project", "park", "school", "hospital"}
    if len(alpha_tokens) == 1 and alpha_tokens[0] in generic:
        return False
    return len(simple.replace(" ", "")) >= 5 or len(norm.replace(" ", "")) >= 6


def build_override_lookup(rows):
    lookup = {}
    for row in rows:
        if not is_truthy(row.get("active")):
            continue
        key = (normalize_tif_number(row.get("tif_number", "")), norm_text(row.get("raw_project_name", "")))
        if key[0] and key[1]:
            lookup[key] = row
    return lookup


def build_master_indices(output_dir):
    master_rows = read_csv(output_dir / "tif_projects_master.csv")
    by_tif = defaultdict(list)
    exact = {}
    normalized = {}
    developer_map = {}
    id_map = {}

    for row in master_rows:
        tif_key = simplify_district_name(row.get("tif_district", ""))
        row = dict(row)
        row["tif_key"] = tif_key
        row["project_norm"] = norm_text(row.get("project_name", ""))
        row["project_simple"] = simplify_project_name(row.get("project_name", ""))
        row["developer_norm"] = norm_text(row.get("developer", ""))
        row["developer_simple"] = simplify_project_name(row.get("developer", ""))
        by_tif[tif_key].append(row)
        id_map[row.get("id", "")] = row
        if tif_key and row["project_norm"]:
            exact.setdefault((tif_key, row["project_norm"]), row)
        if tif_key and row["project_simple"]:
            normalized.setdefault((tif_key, row["project_simple"]), row)
        if tif_key and row["developer_simple"]:
            developer_map.setdefault((tif_key, row["developer_simple"]), row)
    return master_rows, by_tif, exact, normalized, developer_map, id_map


def best_fuzzy_candidate(candidates, annual_simple, annual_norm):
    best = None
    best_score = -1.0
    for row in candidates:
        scores = []
        if annual_simple and row.get("project_simple"):
            scores.append(SequenceMatcher(None, annual_simple, row.get("project_simple")).ratio())
        if annual_norm and row.get("project_norm"):
            scores.append(SequenceMatcher(None, annual_norm, row.get("project_norm")).ratio())
        if annual_simple and row.get("developer_simple"):
            scores.append(SequenceMatcher(None, annual_simple, row.get("developer_simple")).ratio())
        if annual_norm and row.get("developer_norm"):
            scores.append(SequenceMatcher(None, annual_norm, row.get("developer_norm")).ratio())
        score = max(scores) if scores else 0.0
        if score > best_score:
            best_score = score
            best = row
    return best, best_score


def match_project_rows(combined_rows, output_dir, config_rows):
    _, master_by_tif, master_exact, master_normalized, developer_map, master_id_map = build_master_indices(output_dir)
    override_lookup = build_override_lookup(config_rows["project_name_overrides"])
    boundaries = read_csv(output_dir / "tif_district_boundaries.csv")
    boundaries_by_tif = defaultdict(list)
    for row in boundaries:
        tif = normalize_tif_number(row.get("tif_number", ""))
        if tif:
            boundaries_by_tif[tif].append(row)

    match_rows = []
    project_year_rows = []
    canonical_groups = defaultdict(list)

    for row in combined_rows:
        tif_number = normalize_tif_number(row.get("tif_number", ""))
        tif_key = simplify_district_name(row.get("tif_district", ""))
        annual_name = row.get("annual_report_name") or row.get("project_name") or ""
        annual_norm = norm_text(annual_name)
        annual_simple = simplify_project_name(annual_name)
        override = override_lookup.get((tif_number, annual_norm))
        matched_master = None
        match_status = "unmatched"
        match_note = ""
        suggested_master_id = ""
        suggested_master_name = ""
        suggested_match_score = ""
        canonical_project_name = row.get("project_name") or annual_name
        canonical_project_id = ""

        if override is not None:
            canonical_project_id = override.get("canonical_project_id", "") or f"manual_{tif_number}_{slugify(override.get('canonical_project_name', annual_name))}"
            canonical_project_name = override.get("canonical_project_name", "") or canonical_project_name
            master_id = override.get("master_id", "")
            matched_master = master_id_map.get(master_id)
            match_status = "manual_override"
            match_note = override.get("notes", "")
        elif (tif_key, annual_norm) in master_exact:
            matched_master = master_exact[(tif_key, annual_norm)]
            match_status = "exact"
        elif annual_simple and (tif_key, annual_simple) in master_normalized:
            matched_master = master_normalized[(tif_key, annual_simple)]
            match_status = "normalized"
        elif annual_simple and (tif_key, annual_simple) in developer_map:
            matched_master = developer_map[(tif_key, annual_simple)]
            match_status = "normalized"
        else:
            candidate, score = best_fuzzy_candidate(master_by_tif.get(tif_key, []), annual_simple, annual_norm)
            if candidate is not None and score >= 0.82:
                matched_master = candidate
                match_status = "fuzzy_review"
                suggested_master_id = candidate.get("id", "")
                suggested_master_name = candidate.get("project_name", "")
                suggested_match_score = round(score, 4)
                match_note = "high-score fuzzy candidate"
            elif candidate is not None:
                suggested_master_id = candidate.get("id", "")
                suggested_master_name = candidate.get("project_name", "")
                suggested_match_score = round(score, 4)
                match_note = "best fuzzy candidate below threshold"

        if matched_master is not None:
            canonical_project_id = canonical_project_id or f"master_{matched_master.get('id', '')}"
            canonical_project_name = matched_master.get("project_name", "") or canonical_project_name
        else:
            canonical_project_id = canonical_project_id or f"proj_{tif_number}_{slugify(canonical_project_name)}"

        boundary = choose_boundary_for_year(boundaries_by_tif.get(tif_number, []), safe_int(row.get("report_year")))
        master_lat = safe_float(matched_master.get("latitude")) if matched_master else None
        master_lon = safe_float(matched_master.get("longitude")) if matched_master else None
        district_lat = safe_float(boundary.get("centroid_lat")) if boundary else None
        district_lon = safe_float(boundary.get("centroid_lon")) if boundary else None
        if master_lat is not None and master_lon is not None:
            lat, lon, geometry_source = master_lat, master_lon, "master_project_point"
        elif district_lat is not None and district_lon is not None:
            lat, lon, geometry_source = district_lat, district_lon, "district_centroid_fallback"
        else:
            lat, lon, geometry_source = None, None, "missing"

        match_row = {
            "canonical_project_id": canonical_project_id,
            "tif_number": tif_number,
            "tif_district": row.get("tif_district", ""),
            "report_year": row.get("report_year", ""),
            "project_number": row.get("project_number", ""),
            "project_name": row.get("project_name", ""),
            "annual_report_name": annual_name,
            "canonical_project_name": canonical_project_name,
            "match_status": match_status,
            "match_note": match_note,
            "master_id": matched_master.get("id", "") if matched_master else "",
            "master_project_name": matched_master.get("project_name", "") if matched_master else "",
            "master_developer": matched_master.get("developer", "") if matched_master else "",
            "master_address": matched_master.get("address", "") if matched_master else "",
            "master_approved_amount": matched_master.get("approved_amount", "") if matched_master else "",
            "suggested_master_id": suggested_master_id,
            "suggested_master_name": suggested_master_name,
            "suggested_match_score": suggested_match_score,
        }
        match_rows.append(match_row)

        out_row = dict(row)
        out_row.update(
            {
                "canonical_project_id": canonical_project_id,
                "canonical_project_name": canonical_project_name,
                "match_status": match_status,
                "match_note": match_note,
                "master_id": matched_master.get("id", "") if matched_master else "",
                "master_project_name": matched_master.get("project_name", "") if matched_master else "",
                "master_developer": matched_master.get("developer", "") if matched_master else "",
                "master_address": matched_master.get("address", "") if matched_master else "",
                "master_approved_amount": matched_master.get("approved_amount", "") if matched_master else "",
                "master_total_project_cost": matched_master.get("total_project_cost", "") if matched_master else "",
                "master_ward": matched_master.get("ward", "") if matched_master else "",
                "master_latitude": matched_master.get("latitude", "") if matched_master else "",
                "master_longitude": matched_master.get("longitude", "") if matched_master else "",
                "suggested_master_id": suggested_master_id,
                "suggested_master_name": suggested_master_name,
                "suggested_match_score": suggested_match_score,
                "latitude": lat if lat is not None else "",
                "longitude": lon if lon is not None else "",
                "geometry_source": geometry_source,
            }
        )
        project_year_rows.append(out_row)
        canonical_groups[canonical_project_id].append(out_row)

    match_fieldnames = [
        "canonical_project_id", "tif_number", "tif_district", "report_year", "project_number",
        "project_name", "annual_report_name", "canonical_project_name", "match_status", "match_note",
        "master_id", "master_project_name", "master_developer", "master_address", "master_approved_amount",
        "suggested_master_id", "suggested_master_name", "suggested_match_score",
    ]
    write_csv(output_dir / "tif_project_match_status.csv", match_rows, match_fieldnames)

    project_year_fieldnames = [
        "canonical_project_id", "canonical_project_name", "tif_number", "tif_district", "report_year",
        "project_iga", "project_type", "project_number", "project_name", "annual_report_name",
        "current_year_new_deals", "ongoing", "status", "current_year_payments",
        "estimated_next_year_payments", "private_funds", "private_funds_to_completion", "public_funds",
        "public_funds_to_completion", "source", "extraction_method", "annual_report_pdf", "section_pages",
        "legacy_expected_project_total", "match_status", "match_note", "master_id", "master_project_name",
        "master_developer", "master_address", "master_approved_amount", "master_total_project_cost", "master_ward",
        "master_latitude", "master_longitude", "suggested_master_id", "suggested_master_name", "suggested_match_score",
        "latitude", "longitude", "geometry_source",
    ]
    write_csv(output_dir / "tif_project_year_spine.csv", project_year_rows, project_year_fieldnames)

    project_spine_rows = []
    priority = {"manual_override": 5, "exact": 4, "normalized": 3, "fuzzy_review": 2, "unmatched": 1}
    for canonical_project_id, rows in sorted(canonical_groups.items()):
        years = sorted([safe_int(r.get("report_year")) for r in rows if safe_int(r.get("report_year")) is not None])
        best_row = max(rows, key=lambda r: priority.get(r.get("match_status", "unmatched"), 0))
        raw_names = sorted({r.get("annual_report_name", "") for r in rows if r.get("annual_report_name", "")})
        project_numbers = sorted({str(r.get("project_number", "")).strip() for r in rows if str(r.get("project_number", "")).strip()})
        project_spine_rows.append(
            {
                "canonical_project_id": canonical_project_id,
                "canonical_project_name": best_row.get("canonical_project_name", ""),
                "tif_number": best_row.get("tif_number", ""),
                "tif_district": best_row.get("tif_district", ""),
                "first_report_year": years[0] if years else "",
                "last_report_year": years[-1] if years else "",
                "years_observed": len(years),
                "project_iga_rollup": best_row.get("project_iga", ""),
                "project_type_rollup": best_row.get("project_type", ""),
                "match_status_rollup": best_row.get("match_status", ""),
                "master_id": best_row.get("master_id", ""),
                "master_project_name": best_row.get("master_project_name", ""),
                "master_developer": best_row.get("master_developer", ""),
                "master_address": best_row.get("master_address", ""),
                "master_approved_amount": best_row.get("master_approved_amount", ""),
                "raw_name_variants": " | ".join(raw_names),
                "project_number_variants": " | ".join(project_numbers),
                "source_variants": " | ".join(sorted({r.get("source", "") for r in rows if r.get("source", "")})),
                "geometry_source_rollup": best_row.get("geometry_source", ""),
                "latitude": best_row.get("latitude", ""),
                "longitude": best_row.get("longitude", ""),
            }
        )

    write_csv(
        output_dir / "tif_project_spine.csv",
        project_spine_rows,
        [
            "canonical_project_id", "canonical_project_name", "tif_number", "tif_district", "first_report_year",
            "last_report_year", "years_observed", "project_iga_rollup", "project_type_rollup", "match_status_rollup",
            "master_id", "master_project_name", "master_developer", "master_address", "master_approved_amount",
            "raw_name_variants", "project_number_variants", "source_variants", "geometry_source_rollup",
            "latitude", "longitude",
        ],
    )
    return project_year_rows, project_spine_rows, match_rows


def build_document_gap_queue(project_spine_rows, project_year_rows, matter_inventory_rows, config_rows, output_dir):
    override_project_ids = {
        row.get("canonical_project_id", "")
        for row in config_rows["matter_link_overrides"]
        if is_truthy(row.get("active")) and row.get("canonical_project_id", "")
    }

    searchable = []
    for row in matter_inventory_rows:
        full_text = " ".join(
            [
                row.get("title", ""),
                row.get("short_title", ""),
                row.get("record_number", ""),
            ]
        )
        searchable.append(
            {
                "matter_source": row.get("matter_source", ""),
                "matter_id": row.get("matter_id", ""),
                "record_number": row.get("record_number", ""),
                "text_norm": norm_text(full_text),
                "text_simple": simplify_project_name(full_text),
            }
        )

    years_by_project = defaultdict(list)
    statuses_by_project = defaultdict(set)
    names_by_project = defaultdict(set)
    for row in project_year_rows:
        pid = row.get("canonical_project_id", "")
        years_by_project[pid].append(safe_int(row.get("report_year")))
        statuses_by_project[pid].add(row.get("status", ""))
        names_by_project[pid].add(row.get("annual_report_name", "") or row.get("project_name", ""))

    queue_rows = []
    for row in project_spine_rows:
        pid = row.get("canonical_project_id", "")
        tif_number = row.get("tif_number", "")
        canonical_name = row.get("canonical_project_name", "")
        raw_names = [x for x in row.get("raw_name_variants", "").split(" | ") if x]
        search_terms = []
        if canonical_name:
            search_terms.append(canonical_name)
        search_terms.extend(raw_names[:2])
        if row.get("master_developer", ""):
            search_terms.append(row.get("master_developer", ""))
        search_terms = [x for x in search_terms if x and is_informative_search_term(x)]
        term_pairs = []
        for term in search_terms:
            term_norm = norm_text(term)
            term_simple = simplify_project_name(term)
            if term_norm or term_simple:
                term_pairs.append((term_norm, term_simple))

        hit_ids = set()
        for item in searchable:
            for term_norm, term_simple in term_pairs:
                if term_simple and len(term_simple.replace(" ", "")) >= 4 and term_simple in item.get("text_simple", ""):
                    hit_ids.add(f"{item.get('matter_source','')}:{item.get('matter_id','')}")
                    break
                if term_norm and len(term_norm.replace(" ", "")) >= 6 and term_norm in item.get("text_norm", ""):
                    hit_ids.add(f"{item.get('matter_source','')}:{item.get('matter_id','')}")
                    break

        years = sorted([y for y in years_by_project.get(pid, []) if y is not None])
        years_observed = len(years)
        needs_approval = 1 if not hit_ids and pid not in override_project_ids else 0
        needs_amendment = 1 if years_observed >= 2 and row.get("match_status_rollup") in {"exact", "normalized", "manual_override"} else 0
        needs_realized = 1 if row.get("match_status_rollup") in {"fuzzy_review", "unmatched"} or "Active Project" in statuses_by_project.get(pid, set()) else 0
        reasons = []
        if row.get("match_status_rollup") in {"fuzzy_review", "unmatched"}:
            reasons.append("matching_gap")
        if needs_approval:
            reasons.append("missing_approval_doc")
        if needs_amendment:
            reasons.append("possible_amendments")
        if needs_realized:
            reasons.append("missing_realized_outcome_doc")
        if not reasons:
            reasons.append("baseline_complete")

        priority = "low"
        if "matching_gap" in reasons or needs_approval:
            priority = "high"
        elif needs_amendment or needs_realized:
            priority = "medium"

        queue_rows.append(
            {
                "canonical_project_id": pid,
                "tif_number": tif_number,
                "tif_district": row.get("tif_district", ""),
                "canonical_project_name": canonical_name,
                "first_report_year": row.get("first_report_year", ""),
                "last_report_year": row.get("last_report_year", ""),
                "years_observed": years_observed,
                "match_status_rollup": row.get("match_status_rollup", ""),
                "master_id": row.get("master_id", ""),
                "existing_matter_hits": len(hit_ids),
                "existing_matter_hit_ids": " | ".join(sorted(hit_ids)),
                "has_matter_link_override": 1 if pid in override_project_ids else 0,
                "needs_approval_document_search": needs_approval,
                "needs_amendment_search": needs_amendment,
                "needs_realized_outcome_search": needs_realized,
                "queue_priority": priority,
                "search_term_primary": search_terms[0] if search_terms else canonical_name,
                "search_term_secondary": search_terms[1] if len(search_terms) > 1 else tif_number,
                "search_term_amendment": f"{search_terms[0]} amendment" if search_terms else (f"{canonical_name} amendment" if canonical_name else ""),
                "search_term_counterparty": row.get("master_developer", ""),
                "queue_reason": " | ".join(reasons),
            }
        )

    priority_rank = {"high": 3, "medium": 2, "low": 1}
    queue_rows.sort(key=lambda r: (-priority_rank.get(r["queue_priority"], 0), r["tif_number"], r["canonical_project_name"]))
    write_csv(
        output_dir / "tif_document_gap_queue.csv",
        queue_rows,
        [
            "canonical_project_id", "tif_number", "tif_district", "canonical_project_name", "first_report_year",
            "last_report_year", "years_observed", "match_status_rollup", "master_id", "existing_matter_hits",
            "existing_matter_hit_ids", "has_matter_link_override", "needs_approval_document_search",
            "needs_amendment_search", "needs_realized_outcome_search", "queue_priority", "search_term_primary",
            "search_term_secondary", "search_term_amendment", "search_term_counterparty", "queue_reason",
        ],
    )
    return queue_rows


def build_validation_summary(output_dir, document_inventory_rows, legacy_extract_rows, legacy_extract_summary, project_year_rows, project_spine_rows, match_rows):
    match_counts = Counter(row.get("match_status", "") for row in match_rows)
    geometry_counts = Counter(row.get("geometry_source", "") for row in project_year_rows)
    years = [safe_int(row.get("report_year")) for row in project_year_rows if safe_int(row.get("report_year")) is not None]
    legacy_expected_total = sum(safe_int(row.get("expected_projects")) or 0 for row in legacy_extract_summary)
    legacy_extracted_total = sum(safe_int(row.get("extracted_projects")) or 0 for row in legacy_extract_summary)

    rows = [
        {"metric": "project_year_spine_rows", "value": len(project_year_rows)},
        {"metric": "project_spine_rows", "value": len(project_spine_rows)},
        {"metric": "project_year_spine_year_min", "value": min(years) if years else ""},
        {"metric": "project_year_spine_year_max", "value": max(years) if years else ""},
        {"metric": "legacy_extract_pdf_rows", "value": len(legacy_extract_summary)},
        {"metric": "legacy_extract_project_rows", "value": len(legacy_extract_rows)},
        {"metric": "legacy_extract_expected_project_total", "value": legacy_expected_total},
        {"metric": "legacy_extract_observed_project_total", "value": legacy_extracted_total},
        {"metric": "document_inventory_rows", "value": len(document_inventory_rows)},
    ]
    for status, count in sorted(match_counts.items()):
        rows.append({"metric": f"match_status_{slugify(status, 40)}", "value": count})
    for source, count in sorted(geometry_counts.items()):
        rows.append({"metric": f"geometry_source_{slugify(source, 40)}", "value": count})

    write_csv(output_dir / "tif_collection_validation_summary.csv", rows, ["metric", "value"])
    return rows


def build_harvest_run_log(
    output_dir,
    args,
    document_inventory_rows,
    matter_inventory_rows,
    attachment_inventory_rows,
    legacy_extract_rows,
    legacy_extract_summary,
    project_year_rows,
    project_spine_rows,
):
    now_run = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_path = output_dir / "tif_harvest_run_log.csv"
    latest_harvest = load_metric_map(output_dir / "tif_document_harvest_summary.csv")
    latest_inventory = load_metric_map(output_dir / "tif_document_inventory_summary.csv")

    row = {
        "run_id": now_run,
        "summary_source": "post_refresh",
        "skip_elms_mode": latest_harvest.get("skip_elms_mode", ""),
        "elms_unique_matters": latest_harvest.get("elms_unique_matters", ""),
        "elms_attachments_total": latest_harvest.get("elms_attachments_total", ""),
        "elms_pdf_attempted": latest_harvest.get("elms_pdf_attempted", ""),
        "annual_report_pdf_links": latest_harvest.get("annual_report_pdf_links", ""),
        "annual_report_pdf_attempted": latest_harvest.get("annual_report_pdf_attempted", ""),
        "document_inventory_rows": latest_inventory.get("document_inventory_rows", ""),
        "matter_inventory_rows": len(matter_inventory_rows),
        "attachment_inventory_rows": len(attachment_inventory_rows),
        "annual_report_pdf_files_on_disk": latest_inventory.get("annual_report_pdf_files_on_disk", ""),
        "elms_pdf_files_on_disk": latest_inventory.get("elms_pdf_files_on_disk", ""),
        "legistar_pdf_files_on_disk": latest_inventory.get("legistar_pdf_files_on_disk", ""),
        "legacy_start_year": args.legacy_start_year,
        "legacy_end_year": args.legacy_end_year,
        "legacy_pdf_limit": args.max_legacy_pdfs,
        "ocr_search_start_page": args.ocr_search_start_page,
        "ocr_search_end_page": args.ocr_search_end_page,
        "legacy_extract_pdf_rows": len(legacy_extract_summary),
        "legacy_extract_project_rows": len(legacy_extract_rows),
        "project_year_spine_rows": len(project_year_rows),
        "project_spine_rows": len(project_spine_rows),
        "stop_reason": "refresh_gap_driven_layers",
    }
    existing = read_csv(log_path)
    existing.append(row)
    write_csv(
        log_path,
        existing,
        [
            "run_id", "summary_source", "skip_elms_mode", "elms_unique_matters", "elms_attachments_total",
            "elms_pdf_attempted", "annual_report_pdf_links", "annual_report_pdf_attempted", "document_inventory_rows",
            "matter_inventory_rows", "attachment_inventory_rows", "annual_report_pdf_files_on_disk",
            "elms_pdf_files_on_disk", "legistar_pdf_files_on_disk", "legacy_start_year", "legacy_end_year",
            "legacy_pdf_limit", "ocr_search_start_page", "ocr_search_end_page", "legacy_extract_pdf_rows",
            "legacy_extract_project_rows", "project_year_spine_rows", "project_spine_rows", "stop_reason",
        ],
    )


def main():
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    config_dir = Path(args.config_dir).resolve()
    ensure_dir(output_dir)

    config_rows = ensure_config_templates(config_dir)
    document_inventory_rows = build_document_inventory(input_dir, output_dir, config_rows)
    build_inventory_summary(document_inventory_rows, input_dir, output_dir)
    matter_inventory_rows = build_matter_inventory(output_dir)
    attachment_inventory_rows = build_attachment_inventory(output_dir)
    legacy_extract_rows, legacy_extract_summary, _, _ = extract_legacy_project_rows(input_dir, output_dir, args)
    modern_rows = load_modern_project_rows(input_dir)
    combined_rows = sorted(
        modern_rows + legacy_extract_rows,
        key=lambda r: (safe_int(r.get("report_year")) or 0, r.get("tif_number", ""), safe_int(r.get("project_number")) or 0, r.get("project_name", "")),
    )
    write_csv(
        output_dir / "tif_projects_by_district_year_2010_2024.csv",
        combined_rows,
        [
            "tif_number", "tif_district", "report_year", "project_iga", "project_type", "project_number",
            "project_name", "annual_report_name", "current_year_new_deals", "ongoing", "status",
            "current_year_payments", "estimated_next_year_payments", "private_funds", "private_funds_to_completion",
            "public_funds", "public_funds_to_completion", "source", "extraction_method", "annual_report_pdf",
            "section_pages", "legacy_expected_project_total",
        ],
    )
    project_year_rows, project_spine_rows, match_rows = match_project_rows(combined_rows, output_dir, config_rows)
    build_document_gap_queue(project_spine_rows, project_year_rows, matter_inventory_rows, config_rows, output_dir)
    build_validation_summary(output_dir, document_inventory_rows, legacy_extract_rows, legacy_extract_summary, project_year_rows, project_spine_rows, match_rows)
    build_harvest_run_log(
        output_dir,
        args,
        document_inventory_rows,
        matter_inventory_rows,
        attachment_inventory_rows,
        legacy_extract_rows,
        legacy_extract_summary,
        project_year_rows,
        project_spine_rows,
    )

    print(f"Document inventory rows: {len(document_inventory_rows)}")
    print(f"Legacy extracted project rows: {len(legacy_extract_rows)}")
    print(f"Project year spine rows: {len(project_year_rows)}")
    print(f"Project spine rows: {len(project_spine_rows)}")
    print(f"Validation summary: {output_dir / 'tif_collection_validation_summary.csv'}")


if __name__ == "__main__":
    main()
