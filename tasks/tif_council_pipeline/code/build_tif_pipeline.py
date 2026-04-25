#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import subprocess
import time
from html import unescape
from pathlib import Path
from urllib.parse import quote

csv.field_size_limit(50_000_000)

SOC_DATASETS = [
    {
        "slug": "tif_annual_report_projects",
        "id": "72uz-ikdv",
        "name": "Tax Increment Financing (TIF) Annual Report - Projects",
    },
    {
        "slug": "tif_funded_rda_iga_projects",
        "id": "mex4-ppfc",
        "name": "Tax Increment Financing (TIF) Funded RDA and IGA Projects",
    },
    {
        "slug": "tif_analysis_special_tax_allocation_fund",
        "id": "qm7s-3ctt",
        "name": "Tax Increment Financing (TIF) Annual Report - Analysis of Special Tax Allocation Fund",
    },
    {
        "slug": "tif_itemized_expenditures",
        "id": "umwj-yc4m",
        "name": "Tax Increment Financing (TIF) Annual Report - Itemized List of Expenditures from the Special Tax Allocation Fund",
    },
    {
        "slug": "tif_job_increment_creation",
        "id": "vci7-3z5g",
        "name": "Tax Increment Financing (TIF) Annual Report - Job and Increment Creation",
    },
    {
        "slug": "tif_new_redevelopment_agreements",
        "id": "8zuz-r9gs",
        "name": "Tax Increment Financing (TIF) Annual Report - New Redevelopment Agreements",
    },
    {
        "slug": "tif_funding_sources_uses_1998_2014",
        "id": "pner-h2in",
        "name": "TIF Funding Sources and Uses by TIF, Fiscal Year, and Type - 1998-2014",
    },
    {
        "slug": "tif_projections_2025_2034",
        "id": "fpsv-qjg3",
        "name": "TIF Projections - 2025-2034",
    },
    {
        "slug": "tif_boundary_districts",
        "id": "eejr-xtfb",
        "name": "Boundaries - Tax Increment Financing Districts",
    },
    {
        "slug": "tif_boundary_districts_historical",
        "id": "2xxm-9stc",
        "name": "TIF_Districts2",
    },
]

PROGRAMMING_DATASETS = [
    {"slug": "tif_programming_2017_2021", "id": "ycd5-punx"},
    {"slug": "tif_programming_2018_2022", "id": "4kht-kuvx"},
    {"slug": "tif_programming_2019_2023", "id": "jw8c-w7m3"},
    {"slug": "tif_programming_2020_2024", "id": "9up3-ycip"},
    {"slug": "tif_programming_2021_2025", "id": "scch-wiyn"},
    {"slug": "tif_programming_2022_2026", "id": "imgn-2suh"},
    {"slug": "tif_programming_2023_2027", "id": "bcqv-t97y"},
]


def run_cmd(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def run_curl(url, output_path=None, retries=3, pause=1.0):
    err = ""
    for attempt in range(retries):
        cmd = ["curl", "-sSL", "--fail", "--connect-timeout", "10", "--max-time", "45", url]
        if output_path is not None:
            cmd += ["-o", str(output_path)]
        proc = run_cmd(cmd)
        if proc.returncode == 0:
            return proc.stdout
        err = proc.stderr.strip()
        if attempt < retries - 1:
            time.sleep(pause * (attempt + 1))
    raise RuntimeError(err or f"curl failed: {url}")


def fetch_json(url):
    return json.loads(run_curl(url))


def ensure_dirs(*dirs):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def safe_int(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    m = re.search(r"-?\d+", s)
    if not m:
        return None
    return int(m.group(0))


def safe_float(x):
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if s == "":
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    return float(m.group(0))


def norm_text(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def read_csv(path):
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def csv_rows(path):
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.DictReader(f))


def download_socrata_dataset(dataset_id, out_csv):
    url = f"https://data.cityofchicago.org/resource/{dataset_id}.csv?$limit=5000000"
    run_curl(url, output_path=out_csv)
    head = out_csv.read_text(encoding="utf-8", errors="ignore")[:120]
    if head.strip().startswith("{"):
        raise RuntimeError("JSON error payload received instead of CSV")


def collect_tif_catalog_hits(out_csv):
    url = (
        "https://api.us.socrata.com/api/catalog/v1"
        "?domains=data.cityofchicago.org"
        "&search_context=data.cityofchicago.org"
        "&q=TIF&limit=500"
    )
    rows = []
    try:
        hits = fetch_json(url).get("results", [])
    except Exception:
        hits = []

    for h in hits:
        r = h.get("resource", {})
        name = r.get("name", "")
        if "tif" not in norm_text(name):
            continue
        rows.append(
            {
                "id": r.get("id", ""),
                "name": name,
                "type": r.get("type", ""),
                "updated_at": r.get("updatedAt", ""),
                "created_at": r.get("createdAt", ""),
            }
        )

    write_csv(out_csv, rows, ["id", "name", "type", "updated_at", "created_at"])


def download_socrata_bundle(soc_dir):
    inventory = []
    for ds in SOC_DATASETS + PROGRAMMING_DATASETS:
        slug = ds["slug"]
        dataset_id = ds["id"]
        name = ds.get("name", slug)
        out_csv = soc_dir / f"{slug}.csv"
        status = "ok"
        note = ""
        n = 0

        try:
            download_socrata_dataset(dataset_id, out_csv)
            n = csv_rows(out_csv)
        except Exception as exc:
            status = "failed"
            note = str(exc)

        inventory.append(
            {
                "source_type": "socrata",
                "slug": slug,
                "source_name": name,
                "source_id": dataset_id,
                "status": status,
                "local_path": str(out_csv),
                "note": f"rows={n}" if status == "ok" else note,
            }
        )

    return inventory


def fetch_tif_matters(max_matters):
    rows = []
    skip = 0
    top = 1000
    filt = "substringof('Tax Increment Financing',MatterTitle) or substringof('TIF',MatterTitle)"

    while True:
        url = (
            "https://webapi.legistar.com/v1/chicago/matters"
            f"?$filter={quote(filt)}"
            "&$orderby=MatterIntroDate"
            f"&$top={top}&$skip={skip}"
        )
        batch = fetch_json(url)
        if not isinstance(batch, list) or len(batch) == 0:
            break
        rows.extend(batch)
        if len(batch) < top or len(rows) >= max_matters:
            break
        skip += top

    return rows[:max_matters]


def collect_attachments(matters):
    attachments = []
    failures = 0
    for m in matters:
        mid = m.get("MatterId")
        if mid is None:
            continue
        url = f"https://webapi.legistar.com/v1/chicago/matters/{mid}/attachments"
        try:
            rows = fetch_json(url)
        except Exception:
            rows = []
            failures += 1

        if not isinstance(rows, list):
            rows = []

        for a in rows:
            a["MatterId"] = mid
            a["MatterFile"] = m.get("MatterFile")
            a["MatterTitle"] = m.get("MatterTitle")
            attachments.append(a)

    return attachments, failures


def try_download_pdf(url, out_path):
    proc = run_cmd(
        [
            "curl",
            "-sSL",
            "--fail",
            "--connect-timeout",
            "8",
            "--max-time",
            "25",
            "-H",
            "User-Agent: Mozilla/5.0",
            "-H",
            "Accept: application/pdf,*/*;q=0.8",
            url,
            "-o",
            str(out_path),
        ]
    )
    if proc.returncode != 0:
        return False, proc.stderr.strip()

    size = out_path.stat().st_size if out_path.exists() else 0
    if size < 500:
        if out_path.exists():
            out_path.unlink()
        return False, "downloaded file too small"

    with out_path.open("rb") as f:
        magic = f.read(5)
    if magic != b"%PDF-":
        if out_path.exists():
            out_path.unlink()
        return False, "not a PDF"

    return True, ""


def probe_http_code(url):
    proc = run_cmd(
        [
            "curl",
            "-sS",
            "-L",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--connect-timeout",
            "8",
            "--max-time",
            "20",
            "-H",
            "User-Agent: Mozilla/5.0",
            url,
        ]
    )
    if proc.returncode != 0:
        return None, proc.stderr.strip()
    code = safe_int((proc.stdout or "").strip())
    if code == 0:
        return None, "http 000"
    return code, ""


def download_pdfs(attachments, pdf_dir, max_pdf, max_pdf_attempts, legistar_token=None):
    ensure_dirs(pdf_dir)
    downloaded = 0
    attempted = 0
    status_rows = []

    for a in attachments:
        if downloaded >= max_pdf or attempted >= max_pdf_attempts:
            break

        file_name = (a.get("MatterAttachmentFileName") or "").lower()
        link = (a.get("MatterAttachmentHyperlink") or "").strip()
        if ".pdf" not in file_name and ".pdf" not in link.lower():
            continue

        mid = a.get("MatterId")
        aid = a.get("MatterAttachmentId")
        guid = a.get("MatterAttachmentGuid")
        if mid is None or aid is None:
            continue

        out = pdf_dir / f"matter_{mid}_attachment_{aid}.pdf"
        if out.exists() and out.stat().st_size > 500:
            downloaded += 1
            status_rows.append(
                {
                    "MatterId": mid,
                    "MatterAttachmentId": aid,
                    "status": "already_exists",
                    "url_used": "",
                    "error": "",
                    "bytes": out.stat().st_size,
                }
            )
            continue

        candidates = []
        api_file = f"https://webapi.legistar.com/v1/chicago/matters/{mid}/attachments/{aid}/file"

        if legistar_token:
            candidates.append(("api_token", f"{api_file}?token={quote(legistar_token)}"))
            candidates.append(("api_key", f"{api_file}?key={quote(legistar_token)}"))

        if guid:
            candidates.append(("view_guid", f"https://chicago.legistar.com/View.ashx?M=F&ID={aid}&GUID={guid}"))

        if link:
            candidates.append(("hyperlink", link))

        success = False
        last_err = "no candidate urls"
        used = ""

        attempted += 1

        for source, url in candidates:
            code, probe_err = probe_http_code(url)
            if code is not None:
                if code == 403 and "webapi.legistar.com" in url and not legistar_token:
                    last_err = "api token required"
                    continue
                if code in {404, 410}:
                    last_err = f"http {code}"
                    continue
                if code >= 400:
                    last_err = f"http {code}"
                    continue
            elif probe_err:
                last_err = f"probe failed: {probe_err}"
                continue

            ok, err = try_download_pdf(url, out)
            if ok:
                success = True
                used = url
                break
            last_err = err

        if success:
            downloaded += 1
            status_rows.append(
                {
                    "MatterId": mid,
                    "MatterAttachmentId": aid,
                    "status": "downloaded",
                    "url_used": used,
                    "error": "",
                    "bytes": out.stat().st_size,
                }
            )
        else:
            status_rows.append(
                {
                    "MatterId": mid,
                    "MatterAttachmentId": aid,
                    "status": "failed",
                    "url_used": "",
                    "error": last_err,
                    "bytes": 0,
                }
            )

    failed = sum(1 for r in status_rows if r["status"] == "failed")
    return downloaded, failed, attempted, status_rows


def matter_rows_to_csv(matters):
    rows = []
    for m in matters:
        rows.append(
            {
                "MatterId": m.get("MatterId"),
                "MatterFile": m.get("MatterFile"),
                "MatterTypeName": m.get("MatterTypeName"),
                "MatterStatusName": m.get("MatterStatusName"),
                "MatterIntroDate": m.get("MatterIntroDate"),
                "MatterPassedDate": m.get("MatterPassedDate"),
                "MatterTitle": m.get("MatterTitle"),
                "MatterName": m.get("MatterName"),
                "MatterBodyName": m.get("MatterBodyName"),
            }
        )
    return rows


def attachment_rows_to_csv(attachments):
    rows = []
    for a in attachments:
        rows.append(
            {
                "MatterId": a.get("MatterId"),
                "MatterFile": a.get("MatterFile"),
                "MatterTitle": a.get("MatterTitle"),
                "MatterAttachmentId": a.get("MatterAttachmentId"),
                "MatterAttachmentGuid": a.get("MatterAttachmentGuid"),
                "MatterAttachmentName": a.get("MatterAttachmentName"),
                "MatterAttachmentHyperlink": a.get("MatterAttachmentHyperlink"),
                "MatterAttachmentFileName": a.get("MatterAttachmentFileName"),
            }
        )
    return rows


def build_projects_by_district_year(annual_projects_csv, out_csv):
    rows = read_csv(annual_projects_csv)
    out = []
    for r in rows:
        out.append(
            {
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "report_year": safe_int(r.get("report_year")),
                "project_iga": r.get("project_iga", ""),
                "project_type": r.get("project_type", ""),
                "project_number": r.get("project_number", ""),
                "project_name": r.get("project_name", ""),
                "status": r.get("status", ""),
                "current_year_new_deals": r.get("current_year_new_deals", ""),
                "ongoing": r.get("ongoing", ""),
                "current_year_payments": safe_float(r.get("current_year_payments")),
                "estimated_next_year_payments": safe_float(r.get("estimated_next_year_payments")),
                "private_funds": safe_float(r.get("private_funds")),
                "private_funds_to_completion": safe_float(r.get("private_funds_to_completion")),
                "tif_key": norm_text(r.get("tif_district")),
                "project_key": norm_text(r.get("project_name")),
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "tif_district", "report_year", "project_iga", "project_type", "project_number",
            "project_name", "status", "current_year_new_deals", "ongoing", "current_year_payments",
            "estimated_next_year_payments", "private_funds", "private_funds_to_completion", "tif_key", "project_key",
        ],
    )


def build_projects_master(funded_projects_csv, out_csv):
    rows = read_csv(funded_projects_csv)
    out = []
    for r in rows:
        out.append(
            {
                "id": r.get("id", ""),
                "tif_district": r.get("tif_district", ""),
                "project_name": r.get("project_name", ""),
                "address": r.get("address", ""),
                "developer": r.get("developer", ""),
                "project_description": r.get("project_description", ""),
                "approved_amount": safe_float(r.get("approved_amount")),
                "total_project_cost": safe_float(r.get("total_project_cost")),
                "tif_subsidy_percentage": safe_float(r.get("tif_subsidy_percentage")),
                "ward": r.get("ward", ""),
                "community_area": r.get("community_area", ""),
                "cdc_date": r.get("cdc_date", ""),
                "coc_date": r.get("coc_date", ""),
                "x_coordinate": safe_float(r.get("x_coordinate")),
                "y_coordinate": safe_float(r.get("y_coordinate")),
                "latitude": safe_float(r.get("latitude")),
                "longitude": safe_float(r.get("longitude")),
                "tif_key": norm_text(r.get("tif_district")),
                "project_key": norm_text(r.get("project_name")),
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "id", "tif_district", "project_name", "address", "developer", "project_description",
            "approved_amount", "total_project_cost", "tif_subsidy_percentage", "ward", "community_area",
            "cdc_date", "coc_date", "x_coordinate", "y_coordinate", "latitude", "longitude",
            "tif_key", "project_key",
        ],
    )


def build_projects_with_master_match(projects_by_year_csv, projects_master_csv, out_csv):
    yr_rows = read_csv(projects_by_year_csv)
    master_rows = read_csv(projects_master_csv)

    master_map = {}
    for r in master_rows:
        key = (r.get("tif_key", ""), r.get("project_key", ""))
        if key not in master_map:
            master_map[key] = r

    out = []
    for r in yr_rows:
        key = (r.get("tif_key", ""), r.get("project_key", ""))
        m = master_map.get(key)
        out.append(
            {
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "report_year": r.get("report_year", ""),
                "project_number": r.get("project_number", ""),
                "project_name": r.get("project_name", ""),
                "project_type": r.get("project_type", ""),
                "status": r.get("status", ""),
                "current_year_payments": r.get("current_year_payments", ""),
                "estimated_next_year_payments": r.get("estimated_next_year_payments", ""),
                "private_funds": r.get("private_funds", ""),
                "master_id": m.get("id", "") if m else "",
                "master_address": m.get("address", "") if m else "",
                "master_approved_amount": m.get("approved_amount", "") if m else "",
                "master_total_project_cost": m.get("total_project_cost", "") if m else "",
                "master_ward": m.get("ward", "") if m else "",
                "master_developer": m.get("developer", "") if m else "",
                "master_cdc_date": m.get("cdc_date", "") if m else "",
                "master_coc_date": m.get("coc_date", "") if m else "",
                "master_latitude": m.get("latitude", "") if m else "",
                "master_longitude": m.get("longitude", "") if m else "",
                "master_x_coordinate": m.get("x_coordinate", "") if m else "",
                "master_y_coordinate": m.get("y_coordinate", "") if m else "",
                "matched": 1 if m else 0,
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "tif_district", "report_year", "project_number", "project_name", "project_type", "status",
            "current_year_payments", "estimated_next_year_payments", "private_funds", "master_id",
            "master_address", "master_approved_amount", "master_total_project_cost", "master_ward",
            "master_developer", "master_cdc_date", "master_coc_date", "master_latitude",
            "master_longitude", "master_x_coordinate", "master_y_coordinate", "matched",
        ],
    )


def parse_wkt_nested(body):
    stack = []
    token = ""
    root = None

    for ch in body:
        if ch == "(":
            node = []
            if stack:
                stack[-1].append(node)
            stack.append(node)
        elif ch == ")":
            if token.strip() and stack:
                stack[-1].append(token.strip())
                token = ""
            if not stack:
                return None
            node = stack.pop()
            if not stack:
                root = node
        elif ch == ",":
            if token.strip() and stack:
                stack[-1].append(token.strip())
                token = ""
        else:
            token += ch

    return root


def parse_coord_pair(value):
    nums = re.findall(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", value or "")
    if len(nums) < 2:
        return None
    return [float(nums[0]), float(nums[1])]


def parse_wkt_geometry(wkt):
    if not wkt:
        return None
    s = str(wkt).strip()
    if s == "":
        return None

    u = s.upper()
    if u.startswith("MULTIPOLYGON"):
        body = s[s.find("("):]
        nested = parse_wkt_nested(body)
        polygons_raw = nested if isinstance(nested, list) else []
    elif u.startswith("POLYGON"):
        body = s[s.find("("):]
        nested = parse_wkt_nested(body)
        polygons_raw = [nested] if nested else []
    else:
        return None

    polygons = []
    for poly in polygons_raw:
        if not isinstance(poly, list):
            continue
        rings = []
        for ring in poly:
            if not isinstance(ring, list):
                continue
            coords = []
            for pt in ring:
                if isinstance(pt, str):
                    xy = parse_coord_pair(pt)
                    if xy is not None:
                        coords.append(xy)
            if len(coords) >= 3:
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                if len(coords) >= 4:
                    rings.append(coords)
        if rings:
            polygons.append(rings)

    if not polygons:
        return None

    return {"type": "MultiPolygon", "coordinates": polygons}


def ring_area_centroid(ring):
    if len(ring) < 4:
        return None

    area2 = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        cross = (x1 * y2) - (x2 * y1)
        area2 += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross

    if abs(area2) < 1e-12:
        return None

    cx = cx / (3.0 * area2)
    cy = cy / (3.0 * area2)
    return abs(area2) / 2.0, cx, cy


def geometry_centroid(geometry):
    if not geometry or geometry.get("type") != "MultiPolygon":
        return None, None

    total_area = 0.0
    sum_x = 0.0
    sum_y = 0.0
    fallback_x = 0.0
    fallback_y = 0.0
    fallback_n = 0

    for poly in geometry.get("coordinates", []):
        if not poly:
            continue
        outer = poly[0]
        for pt in outer:
            fallback_x += pt[0]
            fallback_y += pt[1]
            fallback_n += 1

        ac = ring_area_centroid(outer)
        if ac is None:
            continue
        area, cx, cy = ac
        total_area += area
        sum_x += cx * area
        sum_y += cy * area

    if total_area > 0:
        return sum_y / total_area, sum_x / total_area

    if fallback_n > 0:
        return fallback_y / fallback_n, fallback_x / fallback_n

    return None, None


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


def build_district_boundaries(boundary_csv_paths, out_geojson, out_centroids_csv):
    existing = [p for p in boundary_csv_paths if p.exists()]
    if not existing:
        out_geojson.write_text(json.dumps({"type": "FeatureCollection", "features": []}, indent=2), encoding="utf-8")
        write_csv(
            out_centroids_csv,
            [],
            [
                "tif_number", "tif_name", "ref_raw", "approval_d", "expiration", "repealed_d",
                "shape_area", "shape_leng", "centroid_lat", "centroid_lon", "geometry_type", "source_slug",
            ],
        )
        return

    rows = []
    for p in existing:
        for r in read_csv(p):
            rr = dict(r)
            rr["_source_slug"] = p.stem
            rows.append(rr)
    features = []
    centroid_rows = []

    for r in rows:
        tif_number = normalize_tif_number(r.get("ref", ""))
        if tif_number == "":
            continue

        geometry = parse_wkt_geometry(r.get("the_geom", ""))
        if geometry is None:
            continue

        lat, lon = geometry_centroid(geometry)
        props = {
            "tif_number": tif_number,
            "tif_name": r.get("name", ""),
            "ref_raw": r.get("ref", ""),
            "approval_d": r.get("approval_d", ""),
            "expiration": r.get("expiration", ""),
            "repealed_d": r.get("repealed_d", ""),
            "shape_area": safe_float(r.get("shape_area")),
            "shape_leng": safe_float(r.get("shape_leng")),
            "centroid_lat": lat,
            "centroid_lon": lon,
            "source_slug": r.get("_source_slug", ""),
        }
        features.append({"type": "Feature", "properties": props, "geometry": geometry})

        centroid_rows.append(
            {
                "tif_number": tif_number,
                "tif_name": r.get("name", ""),
                "ref_raw": r.get("ref", ""),
                "approval_d": r.get("approval_d", ""),
                "expiration": r.get("expiration", ""),
                "repealed_d": r.get("repealed_d", ""),
                "shape_area": safe_float(r.get("shape_area")),
                "shape_leng": safe_float(r.get("shape_leng")),
                "centroid_lat": lat,
                "centroid_lon": lon,
                "geometry_type": "MultiPolygon",
                "source_slug": r.get("_source_slug", ""),
            }
        )

    with out_geojson.open("w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)

    write_csv(
        out_centroids_csv,
        centroid_rows,
        [
            "tif_number", "tif_name", "ref_raw", "approval_d", "expiration", "repealed_d",
            "shape_area", "shape_leng", "centroid_lat", "centroid_lon", "geometry_type", "source_slug",
        ],
    )


def build_projects_with_geometry(projects_with_master_csv, district_boundaries_csv, out_csv):
    projects = read_csv(projects_with_master_csv)
    boundaries = read_csv(district_boundaries_csv) if district_boundaries_csv.exists() else []

    by_tif = {}
    for r in boundaries:
        tif = normalize_tif_number(r.get("tif_number", ""))
        if tif == "":
            continue
        by_tif.setdefault(tif, []).append(r)

    out = []
    for r in projects:
        tif = normalize_tif_number(r.get("tif_number", ""))
        year = safe_int(r.get("report_year"))
        b = choose_boundary_for_year(by_tif.get(tif, []), year)

        master_lat = safe_float(r.get("master_latitude"))
        master_lon = safe_float(r.get("master_longitude"))
        district_lat = safe_float(b.get("centroid_lat")) if b else None
        district_lon = safe_float(b.get("centroid_lon")) if b else None

        if master_lat is not None and master_lon is not None:
            final_lat, final_lon = master_lat, master_lon
            source = "master_project_point"
        elif district_lat is not None and district_lon is not None:
            final_lat, final_lon = district_lat, district_lon
            source = "district_centroid_fallback"
        else:
            final_lat, final_lon = None, None
            source = "missing"

        row = dict(r)
        row["district_boundary_name"] = b.get("tif_name", "") if b else ""
        row["district_boundary_approval_d"] = b.get("approval_d", "") if b else ""
        row["district_boundary_expiration"] = b.get("expiration", "") if b else ""
        row["district_boundary_repealed_d"] = b.get("repealed_d", "") if b else ""
        row["district_centroid_lat"] = district_lat
        row["district_centroid_lon"] = district_lon
        row["latitude"] = final_lat
        row["longitude"] = final_lon
        row["geometry_source"] = source
        out.append(row)

    fieldnames = list(projects[0].keys()) + [
        "district_boundary_name", "district_boundary_approval_d", "district_boundary_expiration",
        "district_boundary_repealed_d", "district_centroid_lat", "district_centroid_lon",
        "latitude", "longitude", "geometry_source",
    ] if projects else [
        "tif_number", "report_year", "latitude", "longitude", "geometry_source",
    ]
    write_csv(out_csv, out, fieldnames)


def build_district_year_boundaries(projects_by_year_csv, district_boundaries_csv, out_csv):
    projects = read_csv(projects_by_year_csv)
    boundaries = read_csv(district_boundaries_csv) if district_boundaries_csv.exists() else []

    by_tif = {}
    for r in boundaries:
        tif = normalize_tif_number(r.get("tif_number", ""))
        if tif == "":
            continue
        by_tif.setdefault(tif, []).append(r)

    project_counts = {}
    for r in projects:
        tif = normalize_tif_number(r.get("tif_number", ""))
        year = safe_int(r.get("report_year"))
        if tif == "" or year is None:
            continue
        project_counts[(tif, year)] = project_counts.get((tif, year), 0) + 1

    out = []
    for (tif, year), n in sorted(project_counts.items()):
        b = choose_boundary_for_year(by_tif.get(tif, []), year)
        out.append(
            {
                "tif_number": tif,
                "report_year": year,
                "project_count": n,
                "district_boundary_name": b.get("tif_name", "") if b else "",
                "district_boundary_approval_d": b.get("approval_d", "") if b else "",
                "district_boundary_expiration": b.get("expiration", "") if b else "",
                "district_boundary_repealed_d": b.get("repealed_d", "") if b else "",
                "district_shape_area": safe_float(b.get("shape_area")) if b else None,
                "district_centroid_lat": safe_float(b.get("centroid_lat")) if b else None,
                "district_centroid_lon": safe_float(b.get("centroid_lon")) if b else None,
                "district_boundary_source": b.get("source_slug", "") if b else "",
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "report_year", "project_count", "district_boundary_name",
            "district_boundary_approval_d", "district_boundary_expiration", "district_boundary_repealed_d",
            "district_shape_area", "district_centroid_lat", "district_centroid_lon", "district_boundary_source",
        ],
    )


def build_coverage_summary(
    projects_by_year_csv,
    projects_master_csv,
    projects_with_match_csv,
    projects_with_geometry_csv,
    district_boundaries_csv,
    out_csv,
):
    annual = read_csv(projects_by_year_csv)
    master = read_csv(projects_master_csv)
    matched = read_csv(projects_with_match_csv)
    with_geom = read_csv(projects_with_geometry_csv)
    boundaries = read_csv(district_boundaries_csv) if district_boundaries_csv.exists() else []

    total_rows = len(annual)
    unique_tifs = {normalize_tif_number(r.get("tif_number", "")) for r in annual if normalize_tif_number(r.get("tif_number", ""))}
    unique_tif_project_number = {
        (normalize_tif_number(r.get("tif_number", "")), (r.get("project_number") or "").strip())
        for r in annual
        if normalize_tif_number(r.get("tif_number", "")) and (r.get("project_number") or "").strip()
    }
    unique_tif_project_name = {
        (normalize_tif_number(r.get("tif_number", "")), norm_text(r.get("project_name")))
        for r in annual
        if normalize_tif_number(r.get("tif_number", "")) and norm_text(r.get("project_name"))
    }

    matched_rows = sum(1 for r in matched if safe_int(r.get("matched")) == 1)
    master_rows = len(master)
    master_rows_with_points = sum(
        1 for r in master if safe_float(r.get("latitude")) is not None and safe_float(r.get("longitude")) is not None
    )

    geom_master = sum(1 for r in with_geom if r.get("geometry_source") == "master_project_point")
    geom_fallback = sum(1 for r in with_geom if r.get("geometry_source") == "district_centroid_fallback")
    geom_missing = sum(1 for r in with_geom if r.get("geometry_source") == "missing")

    districts_with_boundaries = {
        normalize_tif_number(r.get("tif_number", ""))
        for r in boundaries
        if normalize_tif_number(r.get("tif_number", ""))
    }
    district_hits = len(unique_tifs.intersection(districts_with_boundaries))

    years = [safe_int(r.get("report_year")) for r in annual if safe_int(r.get("report_year")) is not None]
    year_min = min(years) if years else None
    year_max = max(years) if years else None

    def pct(num, den):
        return (num / den) if den else None

    rows = [
        {"metric": "annual_project_year_rows", "value": total_rows},
        {"metric": "annual_unique_tif_districts", "value": len(unique_tifs)},
        {"metric": "annual_unique_projects_tif_x_project_number", "value": len(unique_tif_project_number)},
        {"metric": "annual_unique_projects_tif_x_project_name", "value": len(unique_tif_project_name)},
        {"metric": "annual_report_year_min", "value": year_min},
        {"metric": "annual_report_year_max", "value": year_max},
        {"metric": "master_rows", "value": master_rows},
        {"metric": "master_rows_with_lat_lon", "value": master_rows_with_points},
        {"metric": "master_rows_with_lat_lon_share", "value": pct(master_rows_with_points, master_rows)},
        {"metric": "annual_rows_matched_to_master", "value": matched_rows},
        {"metric": "annual_rows_matched_to_master_share", "value": pct(matched_rows, total_rows)},
        {"metric": "annual_rows_with_master_point_geometry", "value": geom_master},
        {"metric": "annual_rows_with_district_centroid_fallback", "value": geom_fallback},
        {"metric": "annual_rows_missing_any_geometry", "value": geom_missing},
        {"metric": "annual_rows_with_any_geometry_share", "value": pct(geom_master + geom_fallback, len(with_geom))},
        {"metric": "annual_districts_with_boundary_match", "value": district_hits},
        {"metric": "annual_districts_with_boundary_match_share", "value": pct(district_hits, len(unique_tifs))},
    ]

    write_csv(out_csv, rows, ["metric", "value"])


def build_district_year_panel(analysis_csv, projects_year_csv, increment_csv, out_csv):
    analysis = read_csv(analysis_csv)
    projects = read_csv(projects_year_csv)
    increment = read_csv(increment_csv)

    project_stats = {}
    for r in projects:
        key = (r.get("tif_number", ""), safe_int(r.get("report_year")))
        if key[1] is None:
            continue
        project_stats.setdefault(key, {"project_count": 0, "sum_current_year_payments": 0.0, "sum_next_year_payments": 0.0})
        project_stats[key]["project_count"] += 1
        project_stats[key]["sum_current_year_payments"] += safe_float(r.get("current_year_payments")) or 0.0
        project_stats[key]["sum_next_year_payments"] += safe_float(r.get("estimated_next_year_payments")) or 0.0

    increment_stats = {}
    for r in increment:
        key = (r.get("tif_number", ""), safe_int(r.get("report_year")))
        if key[1] is None:
            continue
        increment_stats[key] = {
            "increment_projected": safe_float(r.get("increment_projected")),
            "increment_actual": safe_float(r.get("increment_actual")),
            "jobs_projected": safe_float(r.get("jobs_projected")),
            "jobs_actual": safe_float(r.get("jobs_actual")),
        }

    out = []
    for r in analysis:
        year = safe_int(r.get("report_year"))
        key = (r.get("tif_number", ""), year)
        p = project_stats.get(key, {})
        j = increment_stats.get(key, {})
        out.append(
            {
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "report_year": year,
                "tax_allocation_fund_balance": safe_float(r.get("tax_allocation_fund_balance")),
                "property_tax_increment_current": safe_float(r.get("property_tax_increment_current")),
                "property_tax_increment_cumulative": safe_float(r.get("property_tax_increment_cumulative")),
                "transfers_from_municipal_sources_current": safe_float(r.get("municipal_current")),
                "total_expenditures_current": safe_float(r.get("total_expenditures_current")),
                "fund_balance_end_of_year": safe_float(r.get("fund_balance_end_of_year")),
                "project_count": p.get("project_count", 0),
                "sum_current_year_project_payments": p.get("sum_current_year_payments", 0.0),
                "sum_estimated_next_year_payments": p.get("sum_next_year_payments", 0.0),
                "increment_projected": j.get("increment_projected"),
                "increment_actual": j.get("increment_actual"),
                "jobs_projected": j.get("jobs_projected"),
                "jobs_actual": j.get("jobs_actual"),
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "tif_district", "report_year", "tax_allocation_fund_balance",
            "property_tax_increment_current", "property_tax_increment_cumulative",
            "transfers_from_municipal_sources_current", "total_expenditures_current", "fund_balance_end_of_year",
            "project_count", "sum_current_year_project_payments", "sum_estimated_next_year_payments",
            "increment_projected", "increment_actual", "jobs_projected", "jobs_actual",
        ],
    )


def build_programming_long(programming_csv_paths, out_csv):
    out = []
    for p in programming_csv_paths:
        if not p.exists():
            continue
        rows = read_csv(p)
        for r in rows:
            out.append(
                {
                    "source_slug": p.stem,
                    "tif_number": normalize_tif_number(r.get("tif_number", "")),
                    "tif_name": r.get("tif_name", ""),
                    "time_period": safe_int(r.get("time_period")),
                    "type": r.get("type", ""),
                    "description": r.get("description", ""),
                    "amount": safe_float(r.get("amount")),
                    "designation_date": r.get("designation_date", ""),
                    "expiration_date": r.get("expiration_date", ""),
                }
            )

    write_csv(
        out_csv,
        out,
        [
            "source_slug", "tif_number", "tif_name", "time_period", "type",
            "description", "amount", "designation_date", "expiration_date",
        ],
    )


def build_increment_panel(increment_csv, out_csv):
    rows = read_csv(increment_csv)
    out = []
    for r in rows:
        ip = safe_float(r.get("increment_projected"))
        ia = safe_float(r.get("increment_actual"))
        jp = safe_float(r.get("jobs_projected"))
        ja = safe_float(r.get("jobs_actual"))
        out.append(
            {
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "report_year": safe_int(r.get("report_year")),
                "redevelopment_agreement": r.get("redevelopment_agreement", ""),
                "increment_projected": ip,
                "increment_actual": ia,
                "increment_gap": (ia - ip) if ip is not None and ia is not None else None,
                "increment_gap_pct": ((ia - ip) / ip) if ip not in (None, 0) and ia is not None else None,
                "jobs_projected": jp,
                "jobs_actual": ja,
                "jobs_gap": (ja - jp) if jp is not None and ja is not None else None,
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "tif_district", "report_year", "redevelopment_agreement", "increment_projected",
            "increment_actual", "increment_gap", "increment_gap_pct", "jobs_projected", "jobs_actual", "jobs_gap",
        ],
    )



def build_legacy_district_year(legacy_csv, out_csv):
    rows = read_csv(legacy_csv)
    agg = {}

    for r in rows:
        key = (r.get("tif_number", ""), r.get("tifnamel", ""), safe_int(r.get("fy")))
        if key[2] is None:
            continue

        if key not in agg:
            agg[key] = {"revenue_total": 0.0, "expenditure_total": 0.0, "other_total": 0.0}

        amt = safe_float(r.get("amount")) or 0.0
        typ = (r.get("type") or "").strip().lower()

        if typ == "revenues":
            agg[key]["revenue_total"] += amt
        elif typ == "expenditures":
            agg[key]["expenditure_total"] += amt
        else:
            agg[key]["other_total"] += amt

    out = []
    for (tif_number, tif_district, fiscal_year), v in sorted(agg.items()):
        out.append(
            {
                "tif_number": normalize_tif_number(tif_number),
                "tif_district": tif_district,
                "fiscal_year": fiscal_year,
                "revenue_total": v["revenue_total"],
                "expenditure_total": v["expenditure_total"],
                "other_total": v["other_total"],
                "net_revenue_minus_expenditure": v["revenue_total"] - v["expenditure_total"],
            }
        )

    write_csv(
        out_csv,
        out,
        [
            "tif_number", "tif_district", "fiscal_year", "revenue_total", "expenditure_total",
            "other_total", "net_revenue_minus_expenditure",
        ],
    )


def build_full_district_year(legacy_panel_csv, modern_panel_csv, out_csv):
    legacy = read_csv(legacy_panel_csv)
    modern = read_csv(modern_panel_csv)

    out = []

    for r in legacy:
        out.append(
            {
                "source": "legacy_1998_2014",
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "year": safe_int(r.get("fiscal_year")),
                "revenue_total": safe_float(r.get("revenue_total")),
                "expenditure_total": safe_float(r.get("expenditure_total")),
                "fund_balance": None,
                "project_count": None,
                "project_payments_current": None,
                "increment_projected": None,
                "increment_actual": None,
            }
        )

    for r in modern:
        out.append(
            {
                "source": "modern_2017_2024",
                "tif_number": normalize_tif_number(r.get("tif_number", "")),
                "tif_district": r.get("tif_district", ""),
                "year": safe_int(r.get("report_year")),
                "revenue_total": safe_float(r.get("property_tax_increment_current")),
                "expenditure_total": safe_float(r.get("total_expenditures_current")),
                "fund_balance": safe_float(r.get("fund_balance_end_of_year")),
                "project_count": safe_int(r.get("project_count")),
                "project_payments_current": safe_float(r.get("sum_current_year_project_payments")),
                "increment_projected": safe_float(r.get("increment_projected")),
                "increment_actual": safe_float(r.get("increment_actual")),
            }
        )

    out.sort(key=lambda r: (r["tif_number"], r["year"] or 0, r["source"]))

    write_csv(
        out_csv,
        out,
        [
            "source", "tif_number", "tif_district", "year", "revenue_total", "expenditure_total",
            "fund_balance", "project_count", "project_payments_current", "increment_projected", "increment_actual",
        ],
    )



def clean_wiki_text(text):
    text = unescape(text or "")
    text = re.sub(r"<ref[^>]*>.*?</ref>", "", text, flags=re.I)
    text = re.sub(r"<ref[^/]*/>", "", text, flags=re.I)
    text = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", text)
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
    text = re.sub(r"\{\{[^{}]*\}\}", "", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("'''", "").replace("''", "")
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_term_years(term_line, default_end_year):
    line = unescape(term_line or "")
    line = line.replace("<br />", "<br>")

    start_text = ""
    if "{{dts|" in line:
        start_text = line.split("{{dts|", 1)[1].split("}}", 1)[0]
        start_text = start_text.split("|", 1)[0]
    else:
        start_text = line.lstrip("|").split("<br>", 1)[0]

    if "&ndash;" in line:
        end_text = line.split("&ndash;", 1)[1]
    elif "-" in line:
        end_text = line.split("-", 1)[1]
    else:
        end_text = ""

    start_year = safe_int(start_text)
    end_text_clean = clean_wiki_text(end_text)
    if "present" in end_text_clean.lower():
        end_year = default_end_year
    else:
        end_year = safe_int(end_text_clean)

    if start_year is None:
        start_year = safe_int(clean_wiki_text(line))
    if end_year is None:
        end_year = start_year

    if start_year is not None and end_year is not None and end_year < start_year:
        end_year = start_year

    return start_year, end_year


def parse_ward_number(value):
    if value is None:
        return None
    m = re.search(r"\b([1-9]|[1-4][0-9]|50)\b", str(value))
    return int(m.group(1)) if m else None



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





def normalize_name(name):
    name = clean_wiki_text(name)
    if not name:
        return ""
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            name = f"{parts[1]} {parts[0]}"
    name = name.lower()
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name)
    name = re.sub(r"[^a-z\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def extract_ward_from_email(email):
    if not email:
        return None
    m = re.search(r"ward\s*0*([0-9]{1,2})", email.lower())
    if m:
        return int(m.group(1))
    m = re.search(r"ward([0-9]{1,2})", email.lower())
    return int(m.group(1)) if m else None


def build_name_to_ward_map(raw):
    lines = raw.splitlines()
    ward = None
    mapping = {}

    for line in lines:
        line = line.strip()
        m = re.match(r"^===\s*(\d+)(?:st|nd|rd|th) Ward\s*===", line)
        if m:
            ward = int(m.group(1))
            continue

        if ward is None:
            continue

        if line.startswith('!scope="row"|'):
            name = clean_wiki_text(line.split("|", 1)[1])
        elif line.startswith("*"):
            name = clean_wiki_text(line.lstrip("*").strip())
        else:
            continue

        norm = normalize_name(name)
        if not norm:
            continue

        mapping.setdefault(norm, set()).add(ward)

    return mapping


def fetch_office_records():
    filt = "OfficeRecordBodyId eq 138 and OfficeRecordMemberTypeId eq 3"
    records = []
    skip = 0
    top = 1000

    while True:
        params = quote(filt)
        url = (
            "https://webapi.legistar.com/v1/chicago/officerecords"
            f"?$filter={params}&$top={top}&$skip={skip}"
        )
        batch = fetch_json(url)
        if not isinstance(batch, list) or not batch:
            break
        records.extend(batch)
        if len(batch) < top:
            break
        skip += top

    return records


def date_key(date_str):
    if not date_str:
        return 0
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if not m:
        return 0
    return int("".join(m.groups()))


def build_alderman_ward_year_tenure(out_csv, start_year, end_year):
    wiki_url = "https://en.wikipedia.org/w/index.php?title=List_of_Chicago_alderpersons_since_1923&action=raw"
    raw = run_curl(wiki_url)
    name_to_wards = build_name_to_ward_map(raw)

    office_records = fetch_office_records()

    parsed = []
    for r in office_records:
        name = r.get("OfficeRecordFullName", "")
        email = r.get("OfficeRecordEmail", "")
        ward = extract_ward_from_email(email)

        if ward is None:
            norm = normalize_name(name)
            wards = name_to_wards.get(norm, set())
            if len(wards) == 1:
                ward = next(iter(wards))

        if ward is None:
            continue

        start_year_rec = safe_int(r.get("OfficeRecordStartDate"))
        end_year_rec = safe_int(r.get("OfficeRecordEndDate"))
        if start_year_rec is None:
            continue
        if end_year_rec is None:
            end_year_rec = end_year

        parsed.append(
            {
                "ward": ward,
                "alderman_name": clean_wiki_text(name),
                "start_year": start_year_rec,
                "end_year": end_year_rec,
                "start_date_key": date_key(r.get("OfficeRecordStartDate")),
                "source": "legistar_office_records",
            }
        )

    by_ward_year = {}
    for r in parsed:
        sy = max(r["start_year"], start_year)
        ey = min(r["end_year"], end_year)
        if sy > ey:
            continue

        for y in range(sy, ey + 1):
            key = (r["ward"], y)
            prev = by_ward_year.get(key)
            if prev is None or r["start_date_key"] >= prev["start_date_key"]:
                by_ward_year[key] = r

    out = []
    for (w, y), r in sorted(by_ward_year.items()):
        out.append(
            {
                "ward": w,
                "year": y,
                "alderman_name": r["alderman_name"],
                "start_year": r["start_year"],
                "end_year": r["end_year"],
                "source": r["source"],
            }
        )

    write_csv(
        out_csv,
        out,
        ["ward", "year", "alderman_name", "start_year", "end_year", "source"],
    )
def build_project_alderman_merge(projects_with_master_csv, tenure_csv, out_projects_csv, out_summary_csv):
    tenure = read_csv(tenure_csv)
    tenure_map = {}
    for r in tenure:
        ward = safe_int(r.get("ward"))
        year = safe_int(r.get("year"))
        if ward is None or year is None:
            continue
        tenure_map[(ward, year)] = r.get("alderman_name", "")

    rows = read_csv(projects_with_master_csv)
    out_rows = []
    summary = {}

    for r in rows:
        year = safe_int(r.get("report_year"))
        ward = parse_ward_number(r.get("master_ward"))
        alderman = tenure_map.get((ward, year), "") if ward is not None and year is not None else ""

        current_pay = safe_float(r.get("current_year_payments")) or 0.0
        next_pay = safe_float(r.get("estimated_next_year_payments")) or 0.0

        out = {
            "tif_number": normalize_tif_number(r.get("tif_number", "")),
            "tif_district": r.get("tif_district", ""),
            "report_year": year,
            "project_name": r.get("project_name", ""),
            "project_type": r.get("project_type", ""),
            "status": r.get("status", ""),
            "current_year_payments": current_pay,
            "estimated_next_year_payments": next_pay,
            "master_id": r.get("master_id", ""),
            "ward": ward,
            "alderman_name": alderman,
            "matched": safe_int(r.get("matched")) or 0,
        }
        out_rows.append(out)

        if ward is None or year is None or alderman == "":
            continue

        key = (year, ward, alderman)
        if key not in summary:
            summary[key] = {
                "year": year,
                "ward": ward,
                "alderman_name": alderman,
                "project_count": 0,
                "matched_project_count": 0,
                "tif_set": set(),
                "current_year_payments": 0.0,
                "estimated_next_year_payments": 0.0,
            }

        summary[key]["project_count"] += 1
        summary[key]["matched_project_count"] += out["matched"]
        summary[key]["tif_set"].add(out["tif_number"])
        summary[key]["current_year_payments"] += current_pay
        summary[key]["estimated_next_year_payments"] += next_pay

    write_csv(
        out_projects_csv,
        out_rows,
        [
            "tif_number", "tif_district", "report_year", "project_name", "project_type", "status",
            "current_year_payments", "estimated_next_year_payments", "master_id", "ward", "alderman_name", "matched",
        ],
    )

    summary_rows = []
    for _, v in sorted(summary.items()):
        summary_rows.append(
            {
                "year": v["year"],
                "ward": v["ward"],
                "alderman_name": v["alderman_name"],
                "project_count": v["project_count"],
                "matched_project_count": v["matched_project_count"],
                "tif_count": len(v["tif_set"]),
                "current_year_payments": v["current_year_payments"],
                "estimated_next_year_payments": v["estimated_next_year_payments"],
            }
        )

    write_csv(
        out_summary_csv,
        summary_rows,
        [
            "year", "ward", "alderman_name", "project_count", "matched_project_count",
            "tif_count", "current_year_payments", "estimated_next_year_payments",
        ],
    )


def build_district_year_lead_alderman(district_year_csv, project_alderman_csv, out_csv):
    district_rows = read_csv(district_year_csv)
    project_rows = read_csv(project_alderman_csv)

    ward_stats = {}
    for r in project_rows:
        tif = normalize_tif_number(r.get("tif_number", ""))
        year = safe_int(r.get("report_year"))
        ward = safe_int(r.get("ward"))
        alderman = r.get("alderman_name", "")
        if tif == "" or year is None or ward is None or alderman == "":
            continue

        key = (tif, year, ward, alderman)
        if key not in ward_stats:
            ward_stats[key] = {"project_count": 0, "payment_sum": 0.0}
        ward_stats[key]["project_count"] += 1
        ward_stats[key]["payment_sum"] += safe_float(r.get("current_year_payments")) or 0.0

    lead = {}
    for (tif, year, ward, alderman), stat in ward_stats.items():
        key = (tif, year)
        cur = lead.get(key)
        cand = (stat["payment_sum"], stat["project_count"], ward)
        if cur is None:
            lead[key] = {"ward": ward, "alderman_name": alderman, "project_count": stat["project_count"], "payment_sum": stat["payment_sum"]}
        else:
            prev = (cur["payment_sum"], cur["project_count"], cur["ward"])
            if cand > prev:
                lead[key] = {"ward": ward, "alderman_name": alderman, "project_count": stat["project_count"], "payment_sum": stat["payment_sum"]}

    out = []
    for r in district_rows:
        tif = normalize_tif_number(r.get("tif_number", ""))
        year = safe_int(r.get("report_year"))
        l = lead.get((tif, year), None)

        row = dict(r)
        row["lead_ward"] = l["ward"] if l else None
        row["lead_alderman_name"] = l["alderman_name"] if l else ""
        row["lead_project_count"] = l["project_count"] if l else 0
        row["lead_current_year_payments"] = l["payment_sum"] if l else 0.0
        out.append(row)

    fieldnames = list(district_rows[0].keys()) + [
        "lead_ward", "lead_alderman_name", "lead_project_count", "lead_current_year_payments"
    ] if district_rows else [
        "tif_number", "tif_district", "report_year", "lead_ward", "lead_alderman_name",
        "lead_project_count", "lead_current_year_payments"
    ]

    write_csv(out_csv, out, fieldnames)

def parse_args():
    p = argparse.ArgumentParser(description="Build Chicago TIF starter data")
    p.add_argument("--input-dir", default="../input")
    p.add_argument("--output-dir", default="../output")
    p.add_argument("--max-matters", type=int, default=2000)
    p.add_argument("--max-pdf", type=int, default=250)
    p.add_argument("--max-pdf-attempts", type=int, default=400)
    p.add_argument("--legistar-token", default=os.environ.get("LEGISTAR_TOKEN", ""))
    p.add_argument("--alderman-start-year", type=int, default=1998)
    p.add_argument("--alderman-end-year", type=int, default=2024)
    return p.parse_args()


def main():
    args = parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    soc_dir = input_dir / "socrata"
    leg_dir = input_dir / "legistar"
    pdf_dir = leg_dir / "pdf"

    ensure_dirs(input_dir, output_dir, soc_dir, leg_dir, pdf_dir)

    collect_tif_catalog_hits(output_dir / "tif_catalog_hits.csv")

    inventory = []
    inventory.extend(download_socrata_bundle(soc_dir))

    matters = []
    attachments = []
    pdf_status_rows = []

    try:
        matters = fetch_tif_matters(args.max_matters)
        with (leg_dir / "tif_matters.json").open("w", encoding="utf-8") as f:
            json.dump(matters, f, indent=2)

        attachments, attachment_failures = collect_attachments(matters)
        with (leg_dir / "tif_attachments.json").open("w", encoding="utf-8") as f:
            json.dump(attachments, f, indent=2)

        pdf_downloaded, pdf_failed, pdf_attempted, pdf_status_rows = download_pdfs(
            attachments, pdf_dir, args.max_pdf, args.max_pdf_attempts, legistar_token=args.legistar_token
        )

        inventory.append(
            {
                "source_type": "legistar",
                "slug": "tif_matters_and_attachments",
                "source_name": "Legistar matters + attachments",
                "source_id": "webapi.legistar.com/v1/chicago",
                "status": "ok",
                "local_path": str(leg_dir),
                "note": (
                    f"matters={len(matters)};attachments={len(attachments)};"
                    f"attachment_failures={attachment_failures};pdf_attempted={pdf_attempted};"
                    f"pdf_downloaded={pdf_downloaded};pdf_failed={pdf_failed}"
                ),
            }
        )
    except Exception as exc:
        inventory.append(
            {
                "source_type": "legistar",
                "slug": "tif_matters_and_attachments",
                "source_name": "Legistar matters + attachments",
                "source_id": "webapi.legistar.com/v1/chicago",
                "status": "failed",
                "local_path": str(leg_dir),
                "note": str(exc),
            }
        )

    write_csv(
        output_dir / "source_inventory.csv",
        inventory,
        ["source_type", "slug", "source_name", "source_id", "status", "local_path", "note"],
    )

    write_csv(
        output_dir / "tif_matters.csv",
        matter_rows_to_csv(matters),
        [
            "MatterId", "MatterFile", "MatterTypeName", "MatterStatusName", "MatterIntroDate",
            "MatterPassedDate", "MatterTitle", "MatterName", "MatterBodyName",
        ],
    )

    write_csv(
        output_dir / "tif_attachments.csv",
        attachment_rows_to_csv(attachments),
        [
            "MatterId", "MatterFile", "MatterTitle", "MatterAttachmentId", "MatterAttachmentGuid",
            "MatterAttachmentName", "MatterAttachmentHyperlink", "MatterAttachmentFileName",
        ],
    )

    write_csv(
        output_dir / "tif_pdf_download_status.csv",
        pdf_status_rows,
        ["MatterId", "MatterAttachmentId", "status", "url_used", "error", "bytes"],
    )

    build_projects_by_district_year(
        soc_dir / "tif_annual_report_projects.csv",
        output_dir / "tif_projects_by_district_year.csv",
    )

    build_projects_master(
        soc_dir / "tif_funded_rda_iga_projects.csv",
        output_dir / "tif_projects_master.csv",
    )

    build_district_boundaries(
        [
            soc_dir / "tif_boundary_districts.csv",
            soc_dir / "tif_boundary_districts_historical.csv",
        ],
        output_dir / "tif_district_boundaries.geojson",
        output_dir / "tif_district_boundaries.csv",
    )

    build_projects_with_master_match(
        output_dir / "tif_projects_by_district_year.csv",
        output_dir / "tif_projects_master.csv",
        output_dir / "tif_projects_with_master_match.csv",
    )

    build_projects_with_geometry(
        output_dir / "tif_projects_with_master_match.csv",
        output_dir / "tif_district_boundaries.csv",
        output_dir / "tif_projects_with_geometry.csv",
    )

    build_district_year_boundaries(
        output_dir / "tif_projects_by_district_year.csv",
        output_dir / "tif_district_boundaries.csv",
        output_dir / "tif_district_year_boundaries.csv",
    )

    build_coverage_summary(
        output_dir / "tif_projects_by_district_year.csv",
        output_dir / "tif_projects_master.csv",
        output_dir / "tif_projects_with_master_match.csv",
        output_dir / "tif_projects_with_geometry.csv",
        output_dir / "tif_district_boundaries.csv",
        output_dir / "tif_coverage_summary.csv",
    )

    build_increment_panel(
        soc_dir / "tif_job_increment_creation.csv",
        output_dir / "tif_projected_actual_increment.csv",
    )

    build_district_year_panel(
        soc_dir / "tif_analysis_special_tax_allocation_fund.csv",
        output_dir / "tif_projects_by_district_year.csv",
        output_dir / "tif_projected_actual_increment.csv",
        output_dir / "tif_district_year_panel.csv",
    )

    build_programming_long(
        [soc_dir / f"{x['slug']}.csv" for x in PROGRAMMING_DATASETS],
        output_dir / "tif_district_programming_long.csv",
    )

    build_legacy_district_year(
        soc_dir / "tif_funding_sources_uses_1998_2014.csv",
        output_dir / "tif_district_year_legacy_1998_2014.csv",
    )

    build_full_district_year(
        output_dir / "tif_district_year_legacy_1998_2014.csv",
        output_dir / "tif_district_year_panel.csv",
        output_dir / "tif_district_year_panel_full_1998_2024.csv",
    )

    build_alderman_ward_year_tenure(
        output_dir / "alderman_ward_year_tenure.csv",
        args.alderman_start_year,
        args.alderman_end_year,
    )

    build_project_alderman_merge(
        output_dir / "tif_projects_with_master_match.csv",
        output_dir / "alderman_ward_year_tenure.csv",
        output_dir / "tif_projects_with_alderman.csv",
        output_dir / "tif_alderman_year_summary.csv",
    )

    build_district_year_lead_alderman(
        output_dir / "tif_district_year_panel.csv",
        output_dir / "tif_projects_with_alderman.csv",
        output_dir / "tif_district_year_with_lead_alderman.csv",
    )

    print("Pipeline finished")
    print(f"Inventory: {output_dir / 'source_inventory.csv'}")


if __name__ == "__main__":
    main()
