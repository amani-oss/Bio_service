"""
extract.py — Production-grade field observation GPS + species extraction pipeline.
 
Architecture:
  - Modular stages: scan → extract → identify → persist
  - Resumable via SQLite state tracking
  - Adaptive rate limiting with circuit breaker
  - Fully typed with dataclasses
  - Structured logging via rich
"""
 
from __future__ import annotations
 
import base64
import csv
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterator, Optional
from enum import Enum, auto
 
import requests
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
 
load_dotenv()
 
# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
log = logging.getLogger("extract")
console = Console()
 
# ── Config ────────────────────────────────────────────────────────────────────
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png"}
 
CATEGORIES = {
    "insect": {"folder": "image/images_insects", "csv": "insecta_metadata.csv"},
    "flora":  {"folder": "image/images_flora",   "csv": "flora_metadata.csv"},
    "fungus": {"folder": "image/images_fungus",  "csv": "fungus_metadata.csv"},
}
 
API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL   = "google/gemma-3-4b-it:free"
 
# Circuit breaker: pause after N consecutive failures
CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_COOLDOWN  = 300  # seconds
 
# Base delay between successful requests (respect free-tier limits)
BASE_REQUEST_DELAY = 30  # seconds; increase if hitting 429s
 
 
# ── Data Models ───────────────────────────────────────────────────────────────
 
@dataclass
class GPSCoordinates:
    latitude_dms:  str
    longitude_dms: str
    latitude_dd:   float   # decimal degrees — critical for GIS tools
    longitude_dd:  float
    altitude_m:    Optional[float] = None
 
    def to_string(self) -> str:
        return f"{self.latitude_dms}, {self.longitude_dms}"
 
 
@dataclass
class SpeciesResult:
    common_name:    str = "Unknown"
    scientific_name: str = "Unknown"
    description:    str = ""
    raw_response:   str = ""
    parse_success:  bool = False
 
 
@dataclass
class ObservationRecord:
    picture_name:    str
    date:            str
    hour:            str
    model_used:      str
    gps_string:      str
    latitude_dd:     Optional[float]
    longitude_dd:    Optional[float]
    altitude_m:      Optional[float]
    common_name:     str
    scientific_name: str
    parse_success:   bool
    processing_status: str  # SUCCESS | EXIF_ONLY | AI_FAILED | ERROR
 
    @classmethod
    def csv_headers(cls) -> list[str]:
        return [
            "picture_name", "date", "hour", "model_used",
            "gps_string", "latitude_dd", "longitude_dd", "altitude_m",
            "common_name", "scientific_name", "parse_success", "processing_status",
        ]
 
    def to_csv_row(self) -> list:
        return [getattr(self, h) for h in self.csv_headers()]
 
 
# ── EXIF Extraction ───────────────────────────────────────────────────────────
 
class ExifExtractor:
    """Extracts structured GPS and datetime metadata from JPEG/PNG EXIF data."""
 
    @staticmethod
    def _to_float(value) -> float:
        """Convert IFDRational or tuple fraction to float."""
        try:
            return float(value)
        except TypeError:
            return float(value[0]) / float(value[1])
 
    @classmethod
    def _dms_to_dd(cls, dms, ref: str) -> float:
        """Convert DMS tuple to signed decimal degrees."""
        d, m, s = [cls._to_float(x) for x in dms]
        dd = d + m / 60.0 + s / 3600.0
        return -dd if ref in ("S", "W") else dd
 
    @classmethod
    def _dms_string(cls, dms, ref: str) -> str:
        d, m, s = [cls._to_float(x) for x in dms]
        return f"{int(d)}°{int(m)}'{round(s, 2)}\" {ref}"
 
    @classmethod
    def extract(cls, image_path: Path) -> tuple[str, str, Optional[GPSCoordinates]]:
        """
        Returns: (date_str, hour_str, GPSCoordinates | None)
        Raises: OSError, AttributeError on unreadable files.
        """
        date, hour = "N/A", "N/A"
        gps: Optional[GPSCoordinates] = None
 
        with Image.open(image_path) as img:
            raw_exif = img._getexif()
            if not raw_exif:
                return date, hour, gps
 
        exif = {TAGS.get(tag, tag): val for tag, val in raw_exif.items()}
 
        # DateTime
        dt_str = exif.get("DateTime", "")
        if dt_str and " " in dt_str:
            date_part, time_part = dt_str.split(" ", 1)
            date = date_part.replace(":", "-")
            hour = time_part
 
        # GPS
        raw_gps = exif.get("GPSInfo")
        if raw_gps:
            gps_info = {GPSTAGS.get(k, k): raw_gps[k] for k in raw_gps}
            lat      = gps_info.get("GPSLatitude")
            lat_ref  = gps_info.get("GPSLatitudeRef", "N")
            lon      = gps_info.get("GPSLongitude")
            lon_ref  = gps_info.get("GPSLongitudeRef", "E")
            alt      = gps_info.get("GPSAltitude")
 
            if lat and lon:
                gps = GPSCoordinates(
                    latitude_dms  = cls._dms_string(lat, lat_ref),
                    longitude_dms = cls._dms_string(lon, lon_ref),
                    latitude_dd   = cls._dms_to_dd(lat, lat_ref),
                    longitude_dd  = cls._dms_to_dd(lon, lon_ref),
                    altitude_m    = cls._to_float(alt) if alt else None,
                )
 
        return date, hour, gps
 
 
# ── AI Species Identifier ─────────────────────────────────────────────────────
 
class CircuitBreakerOpen(Exception):
    """Raised when the circuit breaker trips."""
 
 
class SpeciesIdentifier:
    """
    Calls OpenRouter LLM to identify species from an image.
    Implements:
      - Exponential backoff on 429
      - Circuit breaker after N consecutive failures
      - Structured JSON parsing of response
    """
 
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._consecutive_failures = 0
 
    def _parse_response(self, raw: str) -> SpeciesResult:
        """Parse LLM response into structured SpeciesResult. Never raises."""
        result = SpeciesResult(raw_response=raw)
        try:
            match = re.search(r'\{.*?\}', raw, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                result.common_name     = data.get("common_name", "Unknown")
                result.scientific_name = data.get("scientific_name", "Unknown")
                result.description     = data.get("description", "")
                result.parse_success   = True
        except json.JSONDecodeError:
            # Fallback: markdown bold pattern
            m = re.search(r'\*\*Common Name:\*\*\s*(.*)', raw)
            if m:
                result.common_name   = m.group(1).strip()
                result.parse_success = True
        return result
 
    def identify(self, image_path: Path) -> Optional[SpeciesResult]:
        """
        Returns SpeciesResult or None on hard failure.
        Raises CircuitBreakerOpen if threshold exceeded.
        """
        if self._consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            raise CircuitBreakerOpen(
                f"Circuit breaker open after {self._consecutive_failures} failures"
            )
 
        if not image_path.exists():
            log.warning(f"Image not found: {image_path}")
            return None
 
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
 
        payload = {
            "model": MODEL,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": (
                        "Identify this organism. Respond ONLY with a JSON object "
                        "with keys: common_name, scientific_name, description. "
                        "No markdown, no extra text."
                    )},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
                ]
            }]
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
 
        wait = 10
        for attempt in range(1, 4):
            try:
                resp = requests.post(API_URL, headers=headers, json=payload, timeout=30)
 
                if resp.status_code == 200:
                    self._consecutive_failures = 0
                    raw = resp.json()["choices"][0]["message"]["content"]
                    return self._parse_response(raw)
 
                if resp.status_code == 429:
                    log.warning(f"Rate limited (attempt {attempt}/3). Waiting {wait}s...")
                    time.sleep(wait)
                    wait *= 2
                    continue
 
                log.error(f"Unexpected HTTP {resp.status_code}: {resp.text[:200]}")
                break
 
            except requests.RequestException as e:
                log.error(f"Request error on attempt {attempt}: {e}")
                time.sleep(wait)
                wait *= 2
 
        self._consecutive_failures += 1
        log.error(f"AI identification failed. Consecutive failures: {self._consecutive_failures}")
        return None
 
 
# ── State Manager (SQLite) ────────────────────────────────────────────────────
 
class StateManager:
    """
    Tracks processed images in SQLite.
    More reliable than scanning CSV rows on every run.
    """
 
    def __init__(self, db_path: str = "pipeline_state.db"):
        self.conn = sqlite3.connect(db_path)
        self._init_schema()
 
    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_images (
                picture_name TEXT PRIMARY KEY,
                category     TEXT NOT NULL,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                status       TEXT
            )
        """)
        self.conn.commit()
 
    def is_processed(self, picture_name: str, category: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM processed_images WHERE picture_name=? AND category=?",
            (picture_name, category)
        )
        return cur.fetchone() is not None
 
    def mark_processed(self, picture_name: str, category: str, status: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO processed_images (picture_name, category, status) VALUES (?,?,?)",
            (picture_name, category, status)
        )
        self.conn.commit()
 
    def close(self):
        self.conn.close()
 
 
# ── CSV Writer ────────────────────────────────────────────────────────────────
 
class CSVWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
 
    def append(self, records: list[ObservationRecord]):
        write_header = not self.output_path.exists()
        with open(self.output_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(ObservationRecord.csv_headers())
            for rec in records:
                writer.writerow(rec.to_csv_row())
        log.info(f"Wrote {len(records)} records to {self.output_path}")
 
 
# ── Pipeline Orchestrator ─────────────────────────────────────────────────────
 
class ObservationPipeline:
    def __init__(self, category: str, folder: str, csv_path: str):
        self.category   = category
        self.folder     = Path(folder)
        self.csv_path   = Path(csv_path)
        self.extractor  = ExifExtractor()
        self.identifier = SpeciesIdentifier(api_key=os.getenv("OPENROUTER_API_KEY", "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"))
        self.state      = StateManager()
        self.writer     = CSVWriter(self.csv_path)
 
    def _scan_images(self) -> Iterator[Path]:
        if not self.folder.exists():
            log.warning(f"Folder not found: {self.folder}")
            return
        for f in sorted(self.folder.iterdir()):
            if f.suffix.lower() in SUPPORTED_EXTENSIONS:
                yield f
 
    def run(self):
        log.info(f"[{self.category.upper()}] Starting pipeline on {self.folder}")
        records = []
 
        for image_path in self._scan_images():
            name = image_path.name
 
            if self.state.is_processed(name, self.category):
                log.debug(f"Skipping already-processed: {name}")
                continue
 
            log.info(f"Processing: {name}")
            status = "SUCCESS"
            date, hour = "N/A", "N/A"
            gps: Optional[GPSCoordinates] = None
            species = SpeciesResult()
 
            # Stage 1: EXIF
            try:
                date, hour, gps = ExifExtractor.extract(image_path)
            except Exception as e:
                log.error(f"EXIF extraction failed for {name}: {e}")
                status = "EXIF_ERROR"
 
            # Stage 2: AI Identification
            try:
                result = self.identifier.identify(image_path)
                if result:
                    species = result
                else:
                    status = "AI_FAILED"
            except CircuitBreakerOpen as e:
                log.critical(f"Circuit breaker open: {e}. Flushing and stopping.")
                status = "CIRCUIT_BREAK"
                # Flush what we have before stopping
                if records:
                    self.writer.append(records)
                    for r in records:
                        self.state.mark_processed(r.picture_name, self.category, r.processing_status)
                return
 
            # Stage 3: Build record
            record = ObservationRecord(
                picture_name     = name,
                date             = date,
                hour             = hour,
                model_used       = MODEL,
                gps_string       = gps.to_string() if gps else "No GPS Data",
                latitude_dd      = gps.latitude_dd if gps else None,
                longitude_dd     = gps.longitude_dd if gps else None,
                altitude_m       = gps.altitude_m if gps else None,
                common_name      = species.common_name,
                scientific_name  = species.scientific_name,
                parse_success    = species.parse_success,
                processing_status = status,
            )
 
            records.append(record)
            self.state.mark_processed(name, self.category, status)
            log.info(f"✅ {name} → {species.common_name} ({species.scientific_name})")
 
            time.sleep(BASE_REQUEST_DELAY)
 
        if records:
            self.writer.append(records)
 
        log.info(f"[{self.category.upper()}] Done. {len(records)} new records.")
        self.state.close()
 
 
# ── Entry Point ───────────────────────────────────────────────────────────────
 
def main():
    console.rule("[bold green]GPS + Species Extraction Pipeline")
    for cat_name, config in CATEGORIES.items():
        pipeline = ObservationPipeline(
            category  = cat_name,
            folder    = config["folder"],
            csv_path  = config["csv"],
        )
        pipeline.run()
    console.rule("[bold green]Pipeline Complete")
 
 
if __name__ == "__main__":
    main()