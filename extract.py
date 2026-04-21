"""
extract.py — BioField AI Extraction Pipeline  (v4 — Cloud Edition)
author: Dr. Hakim Mitiche

Changes from v3:
  - Reads PENDING observations from PostgreSQL (no local CSV/folder scan)
  - Downloads each image from its Cloudinary URL for AI processing
  - Writes all results back to PostgreSQL (no CSV output)
  - Extracts EXIF GPS from the original file bytes via requests download

Requires env vars:
  DATABASE_URL, OPENROUTER_API_KEY
  CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
DATABASE_URL       = os.environ.get("DATABASE_URL", "")

OPENROUTER_URL  = "https://openrouter.ai/api/v1/chat/completions"
AI_MODEL        = "google/gemma-3-27b-it"
MAX_RETRIES     = 3
RETRY_DELAY     = 5   # seconds between retries
CIRCUIT_BREAK_N = 5   # stop after N consecutive AI failures

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def fetch_pending(conn) -> list[dict]:
    """Return all observations with processing_status = 'PENDING'."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, category, picture_name, cloudinary_url
            FROM observations
            WHERE processing_status = 'PENDING'
            ORDER BY id
        """)
        return [dict(r) for r in cur.fetchall()]


def update_observation(conn, obs_id: int, data: dict):
    """Write AI + EXIF results back to the observations row."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE observations SET
                common_name       = %(common_name)s,
                scientific_name   = %(scientific_name)s,
                species_name      = %(species_name)s,
                date              = %(date)s,
                gps_string        = %(gps_string)s,
                latitude_dd       = %(latitude_dd)s,
                longitude_dd      = %(longitude_dd)s,
                altitude_m        = %(altitude_m)s,
                processing_status = %(processing_status)s
            WHERE id = %(id)s
        """, {**data, "id": obs_id})
    conn.commit()


# ── Image download ─────────────────────────────────────────────────────────────

def download_image_bytes(url: str) -> bytes:
    """Download image from Cloudinary URL, return raw bytes."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


# ── EXIF extraction ────────────────────────────────────────────────────────────

def extract_exif(image_bytes: bytes) -> dict:
    """
    Extract GPS + date from image EXIF using piexif or Pillow.
    Returns a dict with latitude_dd, longitude_dd, altitude_m, date, gps_string.
    """
    result = {
        "latitude_dd": None, "longitude_dd": None,
        "altitude_m": None,  "date": None,
        "gps_string": "No GPS Data",
    }

    try:
        import piexif
        exif_dict = piexif.load(image_bytes)

        # Date
        date_raw = exif_dict.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal)
        if date_raw:
            result["date"] = date_raw.decode("utf-8", errors="ignore").split(" ")[0].replace(":", "-")

        # GPS
        gps = exif_dict.get("GPS", {})
        if gps:
            def dms_to_dd(dms, ref):
                d, m, s = [(n / d) for n, d in dms]
                dd = d + m / 60 + s / 3600
                return -dd if ref in (b"S", b"W") else dd

            lat_dms = gps.get(piexif.GPSIFD.GPSLatitude)
            lat_ref = gps.get(piexif.GPSIFD.GPSLatitudeRef)
            lon_dms = gps.get(piexif.GPSIFD.GPSLongitude)
            lon_ref = gps.get(piexif.GPSIFD.GPSLongitudeRef)
            alt     = gps.get(piexif.GPSIFD.GPSAltitude)

            if lat_dms and lon_dms and lat_ref and lon_ref:
                lat = dms_to_dd(lat_dms, lat_ref)
                lon = dms_to_dd(lon_dms, lon_ref)
                result["latitude_dd"]  = round(lat, 6)
                result["longitude_dd"] = round(lon, 6)
                result["gps_string"]   = f"{lat:.6f}, {lon:.6f}"

            if alt:
                result["altitude_m"] = round(alt[0] / alt[1], 1) if isinstance(alt, tuple) else None

    except Exception as e:
        log.warning(f"EXIF extraction failed: {e}")
        result["processing_status_partial"] = "EXIF_ERROR"

    return result


# ── AI identification ──────────────────────────────────────────────────────────

def identify_species(image_bytes: bytes, category: str) -> dict:
    """
    Send image to OpenRouter (Gemma-3 multimodal) for species identification.
    Returns common_name, scientific_name, species_name.
    """
    b64 = base64.b64encode(image_bytes).decode("utf-8")

    prompt = f"""You are a field biologist expert. Identify the {category} in this image.
Return ONLY a JSON object with these exact keys:
  "common_name": string (common name in English),
  "scientific_name": string (Latin binomial),
  "confidence": "high" | "medium" | "low"

If you cannot identify it, use "Unknown" for the names.
Do not include any other text."""

    payload = {
        "model": AI_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text",      "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                ],
            }
        ],
        "max_tokens": 200,
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type":  "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(OPENROUTER_URL, headers=headers,
                                 json=payload, timeout=60)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()

            # Strip markdown fences if present
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            data = json.loads(content.strip())

            return {
                "common_name":    data.get("common_name",    "Unknown"),
                "scientific_name":data.get("scientific_name","Unknown"),
                "species_name":   data.get("common_name",    "Unknown"),
            }

        except Exception as e:
            log.warning(f"AI attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return {"common_name": "Unknown", "scientific_name": "Unknown",
            "species_name": "Unknown"}


# ── Main pipeline ──────────────────────────────────────────────────────────────

def run():
    if not OPENROUTER_API_KEY:
        log.error("OPENROUTER_API_KEY is not set. Aborting.")
        sys.exit(1)
    if not DATABASE_URL:
        log.error("DATABASE_URL is not set. Aborting.")
        sys.exit(1)

    conn = get_db()
    pending = fetch_pending(conn)

    log.info(f"Found {len(pending)} PENDING observations to process.")
    if not pending:
        log.info("Nothing to do.")
        return

    consecutive_failures = 0

    for i, obs in enumerate(pending, 1):
        obs_id   = obs["id"]
        category = obs["category"]
        url      = obs["cloudinary_url"]
        name     = obs["picture_name"]

        log.info(f"[{i}/{len(pending)}] Processing obs #{obs_id} — {name}")

        if not url:
            log.warning(f"  No Cloudinary URL for obs #{obs_id}, skipping.")
            update_observation(conn, obs_id, {
                "common_name": None, "scientific_name": None, "species_name": None,
                "date": None, "gps_string": "No GPS Data",
                "latitude_dd": None, "longitude_dd": None, "altitude_m": None,
                "processing_status": "EXIF_ERROR",
            })
            continue

        if consecutive_failures >= CIRCUIT_BREAK_N:
            log.error(f"Circuit breaker tripped after {consecutive_failures} consecutive failures. Stopping.")
            break

        # 1. Download image
        try:
            img_bytes = download_image_bytes(url)
            log.info(f"  Downloaded {len(img_bytes)//1024} KB from Cloudinary")
        except Exception as e:
            log.error(f"  Download failed: {e}")
            update_observation(conn, obs_id, {
                "common_name": None, "scientific_name": None, "species_name": None,
                "date": None, "gps_string": "No GPS Data",
                "latitude_dd": None, "longitude_dd": None, "altitude_m": None,
                "processing_status": "DOWNLOAD_ERROR",
            })
            consecutive_failures += 1
            continue

        # 2. Extract EXIF
        exif = extract_exif(img_bytes)
        log.info(f"  EXIF: date={exif['date']} gps={exif['gps_string']}")

        # 3. AI identification
        ai = identify_species(img_bytes, category)
        if ai["common_name"] == "Unknown":
            consecutive_failures += 1
            status = "AI_FAILED"
        else:
            consecutive_failures = 0
            status = "SUCCESS"

        log.info(f"  AI: {ai['common_name']} / {ai['scientific_name']} [{status}]")

        # 4. Write back to PostgreSQL
        update_observation(conn, obs_id, {
            "common_name":     ai["common_name"],
            "scientific_name": ai["scientific_name"],
            "species_name":    ai["species_name"],
            "date":            exif["date"],
            "gps_string":      exif["gps_string"],
            "latitude_dd":     exif["latitude_dd"],
            "longitude_dd":    exif["longitude_dd"],
            "altitude_m":      exif["altitude_m"],
            "processing_status": status,
        })

        log.info(f"  ✅ Saved to DB — {status}")

    conn.close()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    run()
