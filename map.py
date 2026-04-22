"""
map.py — BioField Field Map Generator  (v4 — Cloud Edition)
author: Dr. Hakim Mitiche

Reads all observations with GPS from PostgreSQL.
Images displayed via Cloudinary URLs (no local files needed).
Outputs bio_observations_map.html in the project directory.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import folium
import folium.plugins
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
BASE_DIR     = Path(__file__).parent
OUTPUT_FILE  = BASE_DIR / "bio_observations_map.html"

CATEGORY_COLORS = {
    "insect": "#ef4444",
    "flora":  "#22c55e",
    "fungus": "#f97316",
}


def get_gps_observations() -> list[dict]:
    """Return all observations that have valid GPS coordinates."""
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, category, picture_name, cloudinary_url,
                   common_name, scientific_name,
                   date, gps_string, latitude_dd, longitude_dd, altitude_m,
                   processing_status
            FROM observations
            WHERE latitude_dd IS NOT NULL
              AND longitude_dd IS NOT NULL
              AND latitude_dd != 0
              AND longitude_dd != 0
            ORDER BY created_at DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def make_popup(obs: dict) -> str:
    """Build HTML popup for a map marker."""
    name      = obs.get("common_name") or obs.get("picture_name") or "Unknown"
    sci       = obs.get("scientific_name") or ""
    date      = obs.get("date") or "—"
    alt       = obs.get("altitude_m")
    cat       = obs.get("category", "")
    img_url   = obs.get("cloudinary_url") or ""
    color     = CATEGORY_COLORS.get(cat, "#888")

    img_html = ""
    if img_url:
        # Request a thumbnail from Cloudinary (w_300 transformation)
        thumb_url = img_url.replace("/upload/", "/upload/w_300,c_fill/")
        img_html = f'<img src="{thumb_url}" style="width:100%;border-radius:6px;margin-bottom:8px">'

    alt_html = f"<div style='font-size:0.75rem;color:#888'>⛰ {alt} m</div>" if alt else ""

    return f"""
    <div style="font-family:sans-serif;min-width:220px;max-width:280px">
      {img_html}
      <div style="font-weight:600;font-size:0.95rem;color:#1a1a1a">{name}</div>
      <div style="font-style:italic;font-size:0.78rem;color:#555;margin-bottom:4px">{sci}</div>
      <div style="display:inline-block;background:{color}22;color:{color};
                  border:1px solid {color}55;border-radius:20px;
                  padding:1px 8px;font-size:0.68rem;margin-bottom:6px">
        {cat.capitalize()}
      </div>
      <div style="font-size:0.75rem;color:#888">📅 {date}</div>
      {alt_html}
    </div>
    """


def generate_map(rows: list[dict]):
    # Centre map on average of all points
    avg_lat = sum(r["latitude_dd"] for r in rows) / len(rows)
    avg_lon = sum(r["longitude_dd"] for r in rows) / len(rows)

    m = folium.Map(
        location=[avg_lat, avg_lon],
        zoom_start=10,
        tiles="OpenStreetMap",
    )

    # Satellite layer toggle
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri", name="Satellite", overlay=False, control=True,
    ).add_to(m)

    # One FeatureGroup per category
    groups = {cat: folium.FeatureGroup(name=cat.capitalize()) for cat in CATEGORY_COLORS}
    for grp in groups.values():
        grp.add_to(m)

    # Heatmap data
    heat_data = []

    for obs in rows:
        lat = obs["latitude_dd"]
        lon = obs["longitude_dd"]
        cat = obs.get("category", "insect")
        color = CATEGORY_COLORS.get(cat, "#888")

        heat_data.append([lat, lon])

        marker = folium.CircleMarker(
            location=[lat, lon],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(make_popup(obs), max_width=300),
            tooltip=obs.get("common_name") or obs.get("picture_name") or "Unknown",
        )
        groups.get(cat, groups.get("insect")).add_child(marker)

    # Heatmap layer
    folium.plugins.HeatMap(heat_data, name="Heatmap", radius=20).add_to(m)

    # Cluster layer (all categories combined)
    cluster = folium.plugins.MarkerCluster(name="Clusters")
    for obs in rows:
        folium.Marker(
            location=[obs["latitude_dd"], obs["longitude_dd"]],
            popup=folium.Popup(make_popup(obs), max_width=300),
            tooltip=obs.get("common_name") or obs.get("picture_name") or "Unknown",
        ).add_to(cluster)
    cluster.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    folium.plugins.MiniMap().add_to(m)
    folium.plugins.Fullscreen().add_to(m)

    m.save(str(OUTPUT_FILE))
    log.info(f"Map saved → {OUTPUT_FILE}")


def run():
    if not DATABASE_URL:
        log.error("❌ DATABASE_URL not set in .env")
        return

    rows = get_gps_observations()
    log.info(f"Found {len(rows)} observations with GPS")

    if not rows:
        log.warning("No GPS observations found — map not generated.")
        log.warning("Make sure you ran the pipeline first (extract.py).")
        return

    generate_map(rows)
    log.info(f"✅ Map generated with {len(rows)} points → {OUTPUT_FILE.name}")


if __name__ == "__main__":
    run()
