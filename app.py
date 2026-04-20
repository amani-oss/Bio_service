"""
app.py — BioField Dashboard  (v3 — enhanced)
author: Dr. Hakim Mitiche  |  Flask UI by Claude
 
New in v3:
  - /analytics         : Charts (time-series, species richness, success rate, altitude)
  - /api/geojson       : Download GeoJSON export
  - /api/export-csv    : Filtered CSV download
  - /api/export-excel  : Filtered Excel download  (requires openpyxl)
  - /api/backup        : ZIP of all CSVs + SQLite DB
  - /api/upload        : Drag-and-drop image upload to the correct folder
  - /api/image/<cat>/<filename> : Serve images for the lightbox viewer
"""
 
from __future__ import annotations
 
import csv
import io
import json
import os
import queue
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
 
from flask import (Flask, Response, jsonify, render_template,
                   request, send_file, stream_with_context)
from werkzeug.utils import secure_filename
 
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024   # 50 MB per upload
 
# ── Config ─────────────────────────────────────────────────────────────────────
 
BASE_DIR   = Path(__file__).parent
IMAGE_ROOT = BASE_DIR / "image"
 
CATEGORIES = {
    "insect": {
        "csv":    BASE_DIR / "insecta_metadata.csv",
        "folder": IMAGE_ROOT / "images_insects",
        "label":  "Insects",
        "color":  "#f59e0b",
        "icon":   "🪲",
    },
    "flora": {
        "csv":    BASE_DIR / "flora_metadata.csv",
        "folder": IMAGE_ROOT / "images_flora",
        "label":  "Flora",
        "color":  "#4ade80",
        "icon":   "🌿",
    },
    "fungus": {
        "csv":    BASE_DIR / "fungus_metadata.csv",
        "folder": IMAGE_ROOT / "images_fungus",
        "label":  "Fungi",
        "color":  "#c084fc",
        "icon":   "🍄",
    },
}
 
MAP_FILE     = BASE_DIR / "bio_observations_map.html"
GEOJSON_FILE = BASE_DIR / "bio_observations.geojson"
DB_FILE      = BASE_DIR / "pipeline_state.db"
ALLOWED_EXT  = {".jpg", ".jpeg", ".png", ".webp"}
 
_log_queues: dict[str, queue.Queue] = {}
 
 
# ── Data Helpers ───────────────────────────────────────────────────────────────
 
def read_csv(csv_path: Path, category: str) -> list[dict]:
    if not csv_path.exists():
        return []
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["_category"] = category
            rows.append(row)
    return rows
 
 
def get_all_observations() -> list[dict]:
    all_rows = []
    for cat, cfg in CATEGORIES.items():
        all_rows.extend(read_csv(cfg["csv"], cat))
    return all_rows
 
 
def get_stats() -> dict:
    stats = {"total": 0, "with_gps": 0, "categories": {}}
    for cat, cfg in CATEGORIES.items():
        rows = read_csv(cfg["csv"], cat)
        gps_count = sum(1 for r in rows
                        if r.get("gps_string", "").strip() not in ("", "No GPS Data"))
        stats["categories"][cat] = {
            "total": len(rows), "with_gps": gps_count,
            "label": cfg["label"], "color": cfg["color"], "icon": cfg["icon"],
        }
        stats["total"]    += len(rows)
        stats["with_gps"] += gps_count
 
    for cat, cfg in CATEGORIES.items():
        folder = cfg["folder"]
        imgs   = [f for f in folder.iterdir() if f.suffix.lower() in ALLOWED_EXT] if folder.exists() else []
        stats["categories"][cat]["images_on_disk"] = len(imgs)
 
    stats["db_processed"] = 0
    if DB_FILE.exists():
        try:
            conn = sqlite3.connect(DB_FILE)
            stats["db_processed"] = conn.execute("SELECT COUNT(*) FROM processed_images").fetchone()[0]
            conn.close()
        except Exception:
            pass
 
    stats["map_exists"]     = MAP_FILE.exists()
    stats["geojson_exists"] = GEOJSON_FILE.exists()
    return stats
 
 
def get_recent_observations(limit: int = 10) -> list[dict]:
    rows = get_all_observations()
    rows.sort(key=lambda r: r.get("date", ""), reverse=True)
    return rows[:limit]
 
 
def get_analytics_data() -> dict:
    """Aggregate observation data for charts."""
    rows = get_all_observations()
 
    # 1. Observations per month
    month_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        d = r.get("date", "")
        if d and d != "N/A" and len(d) >= 7:
            month_counts[d[:7]] += 1
    months_sorted = sorted(month_counts.items())
 
    # 2. Unique species per category
    species_by_cat: dict[str, set] = defaultdict(set)
    for r in rows:
        name = (r.get("common_name") or r.get("species_name", "")).strip()
        if name and name.lower() not in ("unknown", ""):
            species_by_cat[r["_category"]].add(name)
 
    # 3. Processing status breakdown
    status_counts: Counter = Counter()
    for r in rows:
        st = r.get("processing_status", "UNKNOWN") or "UNKNOWN"
        status_counts[st] += 1
 
    # 4. Altitude distribution buckets (0-500, 500-1000, 1000-1500, 1500+)
    alt_buckets = {"0–500 m": 0, "500–1000 m": 0, "1000–1500 m": 0, "1500+ m": 0}
    for r in rows:
        try:
            a = float(r.get("altitude_m") or r.get("altitude") or 0)
            if a < 500:
                alt_buckets["0–500 m"] += 1
            elif a < 1000:
                alt_buckets["500–1000 m"] += 1
            elif a < 1500:
                alt_buckets["1000–1500 m"] += 1
            else:
                alt_buckets["1500+ m"] += 1
        except (ValueError, TypeError):
            pass
 
    # 5. Top 10 most observed species
    all_species: Counter = Counter()
    for r in rows:
        name = (r.get("common_name") or r.get("species_name", "")).strip()
        if name and name.lower() not in ("unknown", ""):
            all_species[name] += 1
    top_species = all_species.most_common(10)
 
    # 6. GPS vs no-GPS
    with_gps    = sum(1 for r in rows if r.get("gps_string", "").strip() not in ("", "No GPS Data"))
    without_gps = len(rows) - with_gps
 
    return {
        "total": len(rows),
        "timeline": {
            "labels": [m[0] for m in months_sorted],
            "values": [m[1] for m in months_sorted],
        },
        "species_richness": {
            cat: len(sp) for cat, sp in species_by_cat.items()
        },
        "status": dict(status_counts),
        "altitude": alt_buckets,
        "top_species": top_species,
        "gps_coverage": {"With GPS": with_gps, "No GPS": without_gps},
    }
 
 
# ── SSE Streaming ──────────────────────────────────────────────────────────────
 
def _stream_subprocess(cmd: list[str], stream_id: str):
    q = _log_queues[stream_id]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1, cwd=str(BASE_DIR))
        for line in proc.stdout:
            q.put({"type": "log", "text": line.rstrip()})
        proc.wait()
        q.put({"type": "done", "rc": proc.returncode, "text": f"[Exit code {proc.returncode}]"})
    except Exception as e:
        q.put({"type": "error", "text": str(e)})
    finally:
        q.put(None)
 
 
def sse_generator(stream_id: str):
    q = _log_queues.get(stream_id)
    if not q:
        yield "data: {}\n\n"
        return
    while True:
        item = q.get()
        if item is None:
            yield "data: " + json.dumps({"type": "end"}) + "\n\n"
            _log_queues.pop(stream_id, None)
            break
        yield "data: " + json.dumps(item) + "\n\n"
 
 
# ── Global context ─────────────────────────────────────────────────────────────
 
@app.context_processor
def inject_globals():
    return {"categories": CATEGORIES}
 
 
# ── Page Routes ────────────────────────────────────────────────────────────────
 
@app.route("/")
def index():
    return render_template("index.html", stats=get_stats(), recent=get_recent_observations(8))
 
 
@app.route("/observations")
def observations():
    cat_filter = request.args.get("category", "all")
    all_obs    = get_all_observations()
    if cat_filter != "all":
        all_obs = [r for r in all_obs if r["_category"] == cat_filter]
    return render_template("observations.html", observations=all_obs,
                           active=cat_filter, total=len(all_obs))
 
 
@app.route("/map")
def map_view():
    return render_template("map_view.html",
                           map_exists=MAP_FILE.exists(),
                           geojson_exists=GEOJSON_FILE.exists())
 
 
@app.route("/map-content")
def map_content():
    if not MAP_FILE.exists():
        return "Map not generated yet.", 404
    return MAP_FILE.read_text(encoding="utf-8"), 200, {"Content-Type": "text/html"}
 
 
@app.route("/analytics")
def analytics():
    return render_template("analytics.html", data=get_analytics_data())
 
 
@app.route("/pipeline")
def pipeline():
    api_key_set = bool(os.environ.get("OPENROUTER_API_KEY", "").strip())
    return render_template("pipeline.html", stats=get_stats(), api_key_set=api_key_set)
 
 
# ── Data API ───────────────────────────────────────────────────────────────────
 
@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())
 
 
@app.route("/api/analytics")
def api_analytics():
    return jsonify(get_analytics_data())
 
 
# ── Export API ─────────────────────────────────────────────────────────────────
 
@app.route("/api/geojson")
def api_geojson():
    """Download GeoJSON file (regenerated from CSVs on the fly if needed)."""
    if GEOJSON_FILE.exists():
        return send_file(GEOJSON_FILE, as_attachment=True,
                         download_name="bio_observations.geojson",
                         mimetype="application/geo+json")
    # Build on the fly
    rows = get_all_observations()
    features = []
    for r in rows:
        try:
            lat = float(r.get("latitude_dd") or 0)
            lon = float(r.get("longitude_dd") or 0)
            if lat == 0 and lon == 0:
                continue
        except (ValueError, TypeError):
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "picture_name":    r.get("picture_name", ""),
                "category":        r["_category"],
                "species_name":    r.get("common_name") or r.get("species_name", ""),
                "scientific_name": r.get("scientific_name", ""),
                "date":            r.get("date", ""),
                "altitude":        r.get("altitude_m") or r.get("altitude", ""),
            },
        })
    geojson_str = json.dumps({"type": "FeatureCollection", "features": features},
                             ensure_ascii=False, indent=2)
    return Response(geojson_str, mimetype="application/geo+json",
                    headers={"Content-Disposition": "attachment; filename=bio_observations.geojson"})
 
 
@app.route("/api/export-csv")
def api_export_csv():
    cat_filter = request.args.get("category", "all")
    rows = get_all_observations()
    if cat_filter != "all":
        rows = [r for r in rows if r["_category"] == cat_filter]
 
    if not rows:
        return jsonify({"error": "No data"}), 404
 
    fieldnames = [k for k in rows[0].keys() if not k.startswith("_")]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
 
    fname = f"bio_{cat_filter}.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})
 
 
@app.route("/api/export-excel")
def api_export_excel():
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
    except ImportError:
        return jsonify({"error": "openpyxl not installed. Run: pip install openpyxl"}), 500
 
    cat_filter = request.args.get("category", "all")
    rows = get_all_observations()
    if cat_filter != "all":
        rows = [r for r in rows if r["_category"] == cat_filter]
    if not rows:
        return jsonify({"error": "No data"}), 404
 
    wb = openpyxl.Workbook()
 
    # One sheet per category (or just one if filtered)
    if cat_filter == "all":
        sheets_data = {}
        for r in rows:
            sheets_data.setdefault(r["_category"], []).append(r)
    else:
        sheets_data = {cat_filter: rows}
 
    CAT_COLORS = {"insect": "FFF59E0B", "flora": "FF4ADE80", "fungus": "FFC084FC"}
 
    for idx, (cat, cat_rows) in enumerate(sheets_data.items()):
        ws = wb.active if idx == 0 else wb.create_sheet()
        ws.title = cat.capitalize()
 
        fieldnames = [k for k in cat_rows[0].keys() if not k.startswith("_")]
        header_fill = PatternFill("solid", fgColor=CAT_COLORS.get(cat, "FF4ADE80"))
        header_font = Font(bold=True, color="FF000000")
 
        for col_i, field in enumerate(fieldnames, 1):
            cell = ws.cell(row=1, column=col_i, value=field.replace("_", " ").title())
            cell.fill   = header_fill
            cell.font   = header_font
            cell.alignment = Alignment(horizontal="center")
 
        for row_i, r in enumerate(cat_rows, 2):
            for col_i, field in enumerate(fieldnames, 1):
                ws.cell(row=row_i, column=col_i, value=r.get(field, ""))
 
        for col_i in range(1, len(fieldnames) + 1):
            ws.column_dimensions[get_column_letter(col_i)].auto_size = True
 
    # Remove default empty sheet if we added named sheets
    if "Sheet" in wb.sheetnames and len(wb.sheetnames) > 1:
        del wb["Sheet"]
 
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"bio_{cat_filter}.xlsx"
    return send_file(buf, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
 
 
@app.route("/api/backup")
def api_backup():
    """Download a ZIP containing all CSVs and the SQLite database."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for cat, cfg in CATEGORIES.items():
            if cfg["csv"].exists():
                zf.write(cfg["csv"], cfg["csv"].name)
        if DB_FILE.exists():
            zf.write(DB_FILE, DB_FILE.name)
        if GEOJSON_FILE.exists():
            zf.write(GEOJSON_FILE, GEOJSON_FILE.name)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="biofield_backup.zip",
                     mimetype="application/zip")
 
 
# ── Image API ──────────────────────────────────────────────────────────────────
 
@app.route("/api/image/<cat>/<path:filename>")
def api_image(cat: str, filename: str):
    """Serve an observation image for the lightbox viewer."""
    if cat not in CATEGORIES:
        return "Unknown category", 404
    folder = CATEGORIES[cat]["folder"]
    safe   = secure_filename(filename)
    path   = folder / safe
    if not path.exists() or path.suffix.lower() not in ALLOWED_EXT:
        return "Not found", 404
    mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
            "png": "image/png", "webp": "image/webp"}.get(path.suffix.lower().lstrip("."), "image/jpeg")
    return send_file(path, mimetype=mime)
 
 
@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Accept multipart image upload and save to the correct category folder."""
    cat   = request.form.get("category", "")
    files = request.files.getlist("images")
 
    if cat not in CATEGORIES:
        return jsonify({"error": f"Unknown category: {cat}"}), 400
    if not files:
        return jsonify({"error": "No files received"}), 400
 
    folder = CATEGORIES[cat]["folder"]
    folder.mkdir(parents=True, exist_ok=True)
 
    saved, skipped = [], []
    for f in files:
        if not f.filename:
            continue
        safe = secure_filename(f.filename)
        if Path(safe).suffix.lower() not in ALLOWED_EXT:
            skipped.append(safe)
            continue
        dest = folder / safe
        f.save(dest)
        saved.append(safe)
 
    return jsonify({"saved": saved, "skipped": skipped,
                    "total_saved": len(saved), "folder": str(folder)})
 
 
# ── Pipeline API ───────────────────────────────────────────────────────────────
 
@app.route("/api/run-extract", methods=["POST"])
def api_run_extract():
    import uuid
    sid = str(uuid.uuid4())
    _log_queues[sid] = queue.Queue()
    threading.Thread(target=_stream_subprocess,
                     args=([sys.executable, str(BASE_DIR / "extract.py")], sid),
                     daemon=True).start()
    return jsonify({"stream_id": sid})
 
 
@app.route("/api/run-map", methods=["POST"])
def api_run_map():
    import uuid
    sid = str(uuid.uuid4())
    _log_queues[sid] = queue.Queue()
    threading.Thread(target=_stream_subprocess,
                     args=([sys.executable, str(BASE_DIR / "map.py")], sid),
                     daemon=True).start()
    return jsonify({"stream_id": sid})
 
 
@app.route("/api/logs/<stream_id>")
def api_logs(stream_id: str):
    return Response(stream_with_context(sse_generator(stream_id)),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
 
 
# ── Main ───────────────────────────────────────────────────────────────────────
 
import os

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))