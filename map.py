"""
map.py  —  Enhanced
author: Dr. Hakim Mitiche  |  update: Feb. 2026

Generates an interactive Folium map from field observation CSVs.
New in this version:
  - MarkerCluster  (groups nearby markers, cleans up dense areas)
  - HeatMap layer  (density visualisation, togglable)
  - MiniMap        (overview in bottom-left corner)
  - GeoJSON export (bio_observations.geojson sidecar file)
"""

import base64
import csv
import io
import json
import os
import re
import sys
from pathlib import Path

# Force UTF-8 output on Windows (fixes cp1252 encoding crash)
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import folium
from folium import FeatureGroup, LayerControl
from folium.plugins import HeatMap, MarkerCluster, MiniMap

CATEGORIES = [
    {"name": "Insect", "csv": "insecta_metadata.csv", "folder": "image/images_insects", "color": "red",    "icon": "bug"},
    {"name": "Flora",  "csv": "flora_metadata.csv",   "folder": "image/images_flora",   "color": "green",  "icon": "leaf"},
    {"name": "Fungus", "csv": "fungus_metadata.csv",  "folder": "image/images_fungus",  "color": "orange", "icon": "circle"},
]

OUTPUT_MAP     = "bio_observations_map.html"
OUTPUT_GEOJSON = "bio_observations.geojson"


def dms_to_decimal(s: str):
    m = re.search(r"(\d+)°\s*(\d+)'\s*([\d.]+)\"?\s*([NSEW])", s.strip())
    if not m:
        return None
    d, mi, sec, ref = m.groups()
    v = float(d) + float(mi) / 60 + float(sec) / 3600
    return -v if ref in ("S", "W") else v


def image_to_base64_tag(image_path: str, width: int = 200) -> str:
    path = Path(image_path)
    if not path.exists():
        return "<i style='color:#999'>Image not found</i>"
    mime = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".webp": "image/webp"}.get(path.suffix.lower(), "image/jpeg")
    try:
        b64 = base64.b64encode(path.read_bytes()).decode()
        return f'<img src="data:{mime};base64,{b64}" width="{width}" style="border-radius:6px;margin-top:6px;">'
    except OSError as e:
        return f"<i style='color:#c00'>Error: {e}</i>"


def load_locations(csv_file, image_folder, category):
    locs = []
    if not os.path.isfile(csv_file):
        print(f"[warning] Missing CSV: {csv_file}")
        return locs
    with open(csv_file, newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            gps_raw = (row.get("gps_string") or row.get("GPS coordinates (DMS)") or "").replace('""', '"').strip()
            if not gps_raw or gps_raw == "No GPS Data":
                continue
            parts = gps_raw.split(",", 1)
            if len(parts) != 2:
                continue
            lat, lon = dms_to_decimal(parts[0]), dms_to_decimal(parts[1])
            if lat is None or lon is None:
                continue
            locs.append({
                "lat": lat, "lon": lon,
                "picture_name":    row.get("picture_name", ""),
                "date":            row.get("date", ""),
                "hour":            row.get("hour", ""),
                "altitude":        row.get("altitude_m") or row.get("altitude", ""),
                "species_name":    row.get("common_name") or row.get("species_name", "Unknown"),
                "scientific_name": row.get("scientific_name", ""),
                "confidence":      row.get("confidence_rate", ""),
                "note":            row.get("note", ""),
                "image_path":      os.path.join(image_folder, row.get("picture_name", "")),
                "category":        category,
            })
    print(f"[info] Loaded {len(locs)} {category} points")
    return locs


def build_popup(loc):
    sci  = f"<i>({loc['scientific_name']})</i>" if loc["scientific_name"] else ""
    conf = f"<b>Confidence:</b> {loc['confidence']}<br>" if loc["confidence"] else ""
    alt  = f"<b>Altitude:</b> {loc['altitude']} m<br>" if loc["altitude"] else ""
    img  = image_to_base64_tag(loc["image_path"])
    html = f"""
    <div style="font-family:sans-serif;font-size:13px;max-width:260px;">
      <b style="font-size:14px;">{loc['picture_name']}</b><br>
      <span style="color:#555">{loc['category']}</span><br><br>
      <b>Species:</b> <span style="color:#2a7a2a">{loc['species_name']}</span> {sci}<br>
      {conf}<b>Date:</b> {loc['date']} {loc['hour']}<br>
      <b>Lat/Lon:</b> {loc['lat']:.6f}, {loc['lon']:.6f}<br>{alt}{img}
    </div>"""
    return folium.Popup(folium.Html(html, script=True), max_width=300)


def export_geojson(all_locs, path):
    features = [{
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [l["lon"], l["lat"]]},
        "properties": {k: l[k] for k in ("picture_name","category","species_name",
                                          "scientific_name","date","hour","altitude","confidence")},
    } for l in all_locs]
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, indent=2)
    print(f"[info] GeoJSON saved -> {path}")


def main():
    print("[info] Loading datasets...")
    all_locs = []
    for cat in CATEGORIES:
        pts = load_locations(cat["csv"], cat["folder"], cat["name"])
        for p in pts:
            p["color"] = cat["color"]
            p["icon"]  = cat["icon"]
        all_locs.extend(pts)

    if not all_locs:
        print("[ERROR] No valid GPS points found.")
        sys.exit(1)

    clat = sum(p["lat"] for p in all_locs) / len(all_locs)
    clon = sum(p["lon"] for p in all_locs) / len(all_locs)
    print(f"[info] Center: {clat:.5f}, {clon:.5f}  |  Total: {len(all_locs)}")

    # Base map
    m = folium.Map(location=[clat, clon], zoom_start=15, tiles="OpenStreetMap")
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri", name="Satellite",
    ).add_to(m)

    # MiniMap
    MiniMap(toggle_display=True, position="bottomleft").add_to(m)

    # HeatMap (togglable layer, off by default)
    heat_group = FeatureGroup(name="Density Heatmap", show=False)
    HeatMap([[p["lat"], p["lon"]] for p in all_locs],
            min_opacity=0.3, radius=20, blur=15,
            gradient={0.2: "blue", 0.5: "lime", 0.8: "orange", 1.0: "red"}
    ).add_to(heat_group)
    heat_group.add_to(m)

    # Per-category clustered layers
    layers = {}
    for cat in CATEGORIES:
        layers[cat["name"]] = MarkerCluster(name=cat["name"])

    for loc in all_locs:
        folium.Marker(
            location=[loc["lat"], loc["lon"]],
            popup=build_popup(loc),
            tooltip=f"{loc['category']} — {loc['species_name']}",
            icon=folium.Icon(color=loc["color"], icon=loc["icon"], prefix="fa"),
        ).add_to(layers[loc["category"]])

    for layer in layers.values():
        layer.add_to(m)

    LayerControl(collapsed=False).add_to(m)

    # Write with explicit UTF-8 to avoid Windows cp1252 crash
    with open(OUTPUT_MAP, "w", encoding="utf-8") as _f:
        _f.write(m.get_root().render())
    print(f"[info] Map saved -> {OUTPUT_MAP}")
    export_geojson(all_locs, OUTPUT_GEOJSON)
    print("[info] All done.")


if __name__ == "__main__":
    main()
