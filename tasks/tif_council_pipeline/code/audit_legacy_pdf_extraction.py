#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import tempfile
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path


def parse_args():
    ap = argparse.ArgumentParser(description="Audit legacy annual-report PDF extraction against direct PDF text/OCR evidence.")
    ap.add_argument("--output-dir", default="../output")
    ap.add_argument("--sample-size", type=int, default=20, help="Max PDFs to audit; 0 means audit every row in the extract summary.")
    return ap.parse_args()


def read_csv(path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def norm_text(value):
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = s.replace("proiect", "project")
    s = s.replace("companj", "company")
    s = re.sub(r"[\*•]+", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_name(value):
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\x0c", " ")
    s = s.replace("•", " ")
    s = s.replace("*", " ")
    s = re.sub(r"\s+", " ", s).strip(" -:;,.")
    return s


def similarity(a, b):
    return SequenceMatcher(None, norm_text(a), norm_text(b)).ratio()


def normalize_status(value):
    low = norm_text(value)
    if re.search(r"\bno projects were undertaken\b", low):
        return "No Projects"
    if re.search(r"\bproject\b.*\bongoing\b|\bongoing\b", low):
        return "Active Project"
    if re.search(r"\bproject\b.*\bcomplete\b|\bproject\b.*\bcompleted\b|\bcompleted\b|\bcomplete\b", low):
        return "Completed Project"
    return ""


def strip_status_from_line(value):
    s = clean_name(value)
    s = re.sub(r"\bPro(?:j|i)ect\s+is\s+Ongoing\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\bOngoing\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\bPro(?:j|i)ect\s+(?:is\s+)?Completed?\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\bCompleted?\b.*$", "", s, flags=re.I).strip()
    return clean_name(s)


def parse_section_pages(value):
    pages = []
    for token in re.findall(r"\d+", str(value or "")):
        pages.append(int(token))
    return pages or [15]


def choose_sample_rows(rows, sample_size):
    if sample_size == 0 or sample_size >= len(rows):
        return sorted(rows, key=lambda r: (r.get("report_year", ""), r.get("tif_number", "")))

    selected = []
    seen = set()

    def add(row):
        key = (row.get("report_year"), row.get("tif_number"))
        if key in seen:
            return
        seen.add(key)
        selected.append(row)

    ocr_rows = [r for r in rows if r.get("extraction_method") == "ocr"]
    no_rows = [r for r in rows if r.get("status") == "no_rows"]
    multi_rows = sorted(
        [r for r in rows if int(r.get("extracted_projects") or 0) >= 3],
        key=lambda r: (-int(r.get("extracted_projects") or 0), r.get("tif_number", "")),
    )
    rest = sorted(
        rows,
        key=lambda r: (
            -int(r.get("extracted_projects") or 0),
            r.get("extraction_method", ""),
            r.get("tif_number", ""),
        ),
    )

    for group in [ocr_rows, no_rows, multi_rows, rest]:
        for row in group:
            if len(selected) >= sample_size:
                break
            add(row)
        if len(selected) >= sample_size:
            break

    return sorted(selected, key=lambda r: (r.get("report_year", ""), r.get("tif_number", "")))


def run_cmd(args):
    result = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    return result.stdout


def read_pdf_layout_text(pdf_path):
    return run_cmd(["pdftotext", "-layout", str(pdf_path), "-"])


def ocr_pdf_pages(pdf_path, section_pages):
    page_texts = []
    with tempfile.TemporaryDirectory(prefix="tif_pdf_audit_") as tmp:
        tmp_dir = Path(tmp)
        for page in section_pages:
            prefix = tmp_dir / f"{pdf_path.stem}_p{page}"
            try:
                subprocess.run(
                    ["pdftoppm", "-f", str(page), "-l", str(page), "-png", str(pdf_path), str(prefix)],
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError:
                continue
            matches = sorted(tmp_dir.glob(f"{prefix.name}-*.png"))
            if not matches:
                continue
            img_path = matches[0]
            try:
                text = run_cmd(["tesseract", str(img_path), "stdout", "--psm", "6"])
            except subprocess.CalledProcessError:
                continue
            page_texts.append(text)
    return "\n".join(page_texts)


def extract_section_text(raw_text):
    text = raw_text.replace("\x0c", "\n")
    start = re.search(r"SECTION\s*5[^\n]*", text, flags=re.I)
    if not start:
        return ""

    section = text[start.start():]
    cut = len(section)
    for token in ["Attachment B", "CERTIFICATION", "STATE OF ILLINOIS"]:
        m = re.search(rf"\b{re.escape(token)}\b", section, flags=re.I)
        if m:
            cut = min(cut, m.start())
    return section[:cut].strip()


def extract_project_evidence(section_text):
    text = section_text or ""
    if text.strip() == "":
        return {"section_detected": False, "no_projects": False, "projects": [], "excerpt": ""}

    excerpt = re.sub(r"\s+", " ", text).strip()[:1200]
    if re.search(r"No\s+Projects\s+Were\s+Undertaken", text, flags=re.I):
        return {"section_detected": True, "no_projects": True, "projects": [], "excerpt": excerpt}

    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    projects = []
    i = 0
    while i < len(lines):
        line = lines[i]
        project_match = re.match(r"Project\s+(\d{1,3})\s*:?\s*$", line, flags=re.I)
        if not project_match:
            i += 1
            continue

        project_number = project_match.group(1)
        evidence_lines = []
        j = i + 1
        while j < len(lines):
            probe = lines[j]
            probe_low = norm_text(probe)
            if re.match(r"Project\s+\d{1,3}\s*:?\s*$", probe, flags=re.I):
                break
            if probe_low.startswith("private investment undertaken"):
                break
            if probe_low.startswith("public investment undertaken"):
                break
            if probe_low.startswith("ratio of private public investment"):
                break
            if probe_low.startswith("general notes"):
                break
            if probe_low.startswith("depending on the particular goals"):
                break
            evidence_lines.append(probe)
            if len(evidence_lines) >= 3:
                break
            j += 1

        joined = clean_name(" ".join(evidence_lines))
        projects.append(
            {
                "project_number": project_number,
                "source_line": joined,
                "source_name": strip_status_from_line(joined),
                "source_status": normalize_status(joined),
            }
        )
        i = j

    if not projects:
        fallback_counter = 1
        for line in lines:
            low = norm_text(line)
            if (
                low.startswith("section 5")
                or low.startswith("please include a brief description")
                or low.startswith("see general notes below")
                or low.startswith("total")
                or low.startswith("private investment undertaken")
                or low.startswith("public investment undertaken")
                or low.startswith("ratio of private public investment")
                or low.startswith("general notes")
                or low.startswith("fy 20")
                or low.startswith("page ")
                or low.startswith("as of the last date of the reporting fiscal year")
                or low.startswith("this project will be reported on the annual report")
                or low.startswith("project will be reported on the annual report")
                or low.startswith("depending on the particular goals")
                or low.startswith("each ultimate grantee")
            ):
                continue
            if not re.search(r"\bproject\b.*\bongoing\b|\bproject\b.*\bcomplete\b|\bproject\b.*\bcompleted\b", low):
                continue
            source_name = strip_status_from_line(line)
            if source_name == "":
                continue
            projects.append(
                {
                    "project_number": str(fallback_counter),
                    "source_line": clean_name(line),
                    "source_name": source_name,
                    "source_status": normalize_status(line),
                }
            )
            fallback_counter += 1

    return {"section_detected": True, "no_projects": False, "projects": projects, "excerpt": excerpt}


def compare_pdf(row, extracted_rows):
    pdf_path = Path(row["pdf_path"])
    section_pages = parse_section_pages(row.get("section_pages"))

    layout_text = read_pdf_layout_text(pdf_path)
    section_text = extract_section_text(layout_text)
    evidence = extract_project_evidence(section_text)
    source_mode = "pdftotext"

    need_ocr = row.get("extraction_method") == "ocr" or not evidence["section_detected"] or (
        not evidence["no_projects"] and len(evidence["projects"]) == 0
    )
    if need_ocr:
        ocr_text = ocr_pdf_pages(pdf_path, section_pages)
        ocr_evidence = extract_project_evidence(ocr_text)
        if ocr_evidence["section_detected"] and (ocr_evidence["no_projects"] or len(ocr_evidence["projects"]) > 0):
            evidence = ocr_evidence
            source_mode = "ocr_page"

    extracted_by_num = {str(int(r["project_number"])): r for r in extracted_rows if str(r.get("project_number", "")).strip()}
    source_by_num = {str(int(p["project_number"])): p for p in evidence["projects"] if str(p.get("project_number", "")).strip()}

    project_audit_rows = []
    low_similarity_count = 0
    status_mismatch_count = 0

    project_numbers = sorted(set(extracted_by_num) | set(source_by_num), key=lambda x: int(x))
    for project_number in project_numbers:
        extracted = extracted_by_num.get(project_number, {})
        source = source_by_num.get(project_number, {})
        extracted_name = extracted.get("project_name", "")
        source_name = source.get("source_name", "")
        extracted_status = extracted.get("status", "")
        source_status = source.get("source_status", "")
        sim = similarity(extracted_name, source_name) if extracted_name and source_name else 0.0
        name_match = bool(extracted_name and source_name and sim >= 0.88)
        status_match = True
        if extracted_status and source_status:
            status_match = normalize_status(extracted_status) == normalize_status(source_status)
        if extracted_name and source_name and sim < 0.88:
            low_similarity_count += 1
        if not status_match:
            status_mismatch_count += 1

        verdict = "pass"
        if extracted_name == "" or source_name == "":
            verdict = "fail"
        elif not name_match or not status_match:
            verdict = "warning"

        project_audit_rows.append(
            {
                "report_year": row.get("report_year", ""),
                "tif_number": row.get("tif_number", ""),
                "pdf_path": str(pdf_path),
                "source_mode": source_mode,
                "project_number": project_number,
                "extracted_project_name": extracted_name,
                "source_project_name": source_name,
                "extracted_status": extracted_status,
                "source_status": source_status,
                "name_similarity": f"{sim:.4f}" if extracted_name and source_name else "",
                "name_match": 1 if name_match else 0,
                "status_match": 1 if status_match else 0,
                "source_line": source.get("source_line", ""),
                "project_audit_verdict": verdict,
            }
        )

    extracted_count = len(extracted_rows)
    source_count = len(evidence["projects"])
    count_match = int(extracted_count == source_count)
    no_projects_match = int(evidence["no_projects"] and extracted_count == 0) if evidence["no_projects"] else ""

    pdf_verdict = "pass"
    notes = []
    if evidence["no_projects"]:
        if extracted_count == 0:
            notes.append("source_explicitly_reports_no_projects")
        else:
            pdf_verdict = "fail"
            notes.append("source_reports_no_projects_but_extraction_has_rows")
    else:
        if not evidence["section_detected"]:
            pdf_verdict = "warning"
            notes.append("section5_not_detected")
        if extracted_count != source_count:
            pdf_verdict = "fail"
            notes.append("project_count_mismatch")
        elif low_similarity_count > 0 or status_mismatch_count > 0:
            if pdf_verdict != "fail":
                pdf_verdict = "warning"
            if low_similarity_count > 0:
                notes.append(f"low_similarity_projects={low_similarity_count}")
            if status_mismatch_count > 0:
                notes.append(f"status_mismatches={status_mismatch_count}")

    summary_row = {
        "report_year": row.get("report_year", ""),
        "tif_number": row.get("tif_number", ""),
        "pdf_path": str(pdf_path),
        "source_mode": source_mode,
        "extraction_method": row.get("extraction_method", ""),
        "expected_status": row.get("status", ""),
        "extracted_project_count": extracted_count,
        "source_project_count": source_count,
        "source_no_projects_flag": 1 if evidence["no_projects"] else 0,
        "count_match": count_match,
        "no_projects_match": no_projects_match,
        "low_similarity_project_count": low_similarity_count,
        "status_mismatch_count": status_mismatch_count,
        "extracted_project_names": " | ".join(r.get("project_name", "") for r in extracted_rows),
        "source_project_names": " | ".join(p.get("source_name", "") for p in evidence["projects"]),
        "source_excerpt": evidence["excerpt"],
        "pdf_audit_verdict": pdf_verdict,
        "audit_notes": " | ".join(notes),
    }

    return summary_row, project_audit_rows


def build_summary_metrics(pdf_rows, project_rows):
    verdict_counts = Counter(row.get("pdf_audit_verdict", "") for row in pdf_rows)
    source_mode_counts = Counter(row.get("source_mode", "") for row in pdf_rows)
    project_verdict_counts = Counter(row.get("project_audit_verdict", "") for row in project_rows)

    rows = [
        {"metric": "audit_pdf_rows", "value": len(pdf_rows)},
        {"metric": "audit_project_rows", "value": len(project_rows)},
        {"metric": "audit_pdf_pass", "value": verdict_counts.get("pass", 0)},
        {"metric": "audit_pdf_warning", "value": verdict_counts.get("warning", 0)},
        {"metric": "audit_pdf_fail", "value": verdict_counts.get("fail", 0)},
        {"metric": "audit_project_pass", "value": project_verdict_counts.get("pass", 0)},
        {"metric": "audit_project_warning", "value": project_verdict_counts.get("warning", 0)},
        {"metric": "audit_project_fail", "value": project_verdict_counts.get("fail", 0)},
        {"metric": "audit_source_mode_pdftotext", "value": source_mode_counts.get("pdftotext", 0)},
        {"metric": "audit_source_mode_ocr_page", "value": source_mode_counts.get("ocr_page", 0)},
        {"metric": "audit_pdf_no_project_rows", "value": sum(1 for row in pdf_rows if str(row.get("source_no_projects_flag", "")) == "1")},
        {"metric": "audit_pdf_count_mismatch_rows", "value": sum(1 for row in pdf_rows if str(row.get("count_match", "")) == "0")},
        {"metric": "audit_project_low_similarity_rows", "value": sum(1 for row in project_rows if row.get("name_match") == 0)},
        {"metric": "audit_project_status_mismatch_rows", "value": sum(1 for row in project_rows if row.get("status_match") == 0)},
    ]
    return rows


def main():
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()

    extract_summary = read_csv(output_dir / "tif_legacy_annual_report_extract_summary.csv")
    project_rows = read_csv(output_dir / "tif_legacy_annual_report_projects_2010_2016.csv")

    selected_rows = choose_sample_rows(extract_summary, args.sample_size)
    project_rows_by_key = defaultdict(list)
    for row in project_rows:
        key = (row.get("report_year", ""), row.get("tif_number", ""))
        project_rows_by_key[key].append(row)

    pdf_audit_rows = []
    project_audit_rows = []
    for row in selected_rows:
        key = (row.get("report_year", ""), row.get("tif_number", ""))
        summary_row, project_rows_for_pdf = compare_pdf(row, project_rows_by_key.get(key, []))
        pdf_audit_rows.append(summary_row)
        project_audit_rows.extend(project_rows_for_pdf)

    summary_metrics = build_summary_metrics(pdf_audit_rows, project_audit_rows)

    write_csv(
        output_dir / "tif_legacy_pdf_audit_sample.csv",
        pdf_audit_rows,
        [
            "report_year", "tif_number", "pdf_path", "source_mode", "extraction_method", "expected_status",
            "extracted_project_count", "source_project_count", "source_no_projects_flag", "count_match",
            "no_projects_match", "low_similarity_project_count", "status_mismatch_count",
            "extracted_project_names", "source_project_names", "source_excerpt", "pdf_audit_verdict", "audit_notes",
        ],
    )
    write_csv(
        output_dir / "tif_legacy_pdf_project_audit_sample.csv",
        project_audit_rows,
        [
            "report_year", "tif_number", "pdf_path", "source_mode", "project_number",
            "extracted_project_name", "source_project_name", "extracted_status", "source_status",
            "name_similarity", "name_match", "status_match", "source_line", "project_audit_verdict",
        ],
    )
    write_csv(output_dir / "tif_legacy_pdf_audit_summary.csv", summary_metrics, ["metric", "value"])

    print(f"Audited PDF rows: {len(pdf_audit_rows)}")
    print(f"Audited project rows: {len(project_audit_rows)}")
    print(f"Summary: {output_dir / 'tif_legacy_pdf_audit_summary.csv'}")


if __name__ == "__main__":
    main()
