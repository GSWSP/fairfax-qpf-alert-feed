
#!/usr/bin/env python3
"""
Fairfax, VA — WPC QPF-based 48h rain alert RSS (no API keys)
Alerts when any 48-hour window through Day 7 meets/exceeds 0.30"

Data source:
- WPC QPF MapServer (NOAA/NWS) with 48h layers (Day 1–2, 4–5, 6–7) and 6h intervals through Day 3.
  https://mapservices.weather.noaa.gov/vector/rest/services/precip/wpc_qpf/MapServer
  (Layer directory shows the names/IDs and that renderer is on the 'qpf' field.)
"""

import json
import time
from pathlib import Path
from email.utils import formatdate
import urllib.request, urllib.parse

# ===== CONFIG =====
LAT, LON = 38.8460, -77.3060       # Fairfax, VA point (City of Fairfax)
THRESHOLD_IN = 0.30
SERVICE = "https://mapservices.weather.noaa.gov/vector/rest/services/precip/wpc_qpf/MapServer"
FEED_FILE = Path("feed.xml")
ITEMS_FILE = Path("items.json")
STATE_FILE = Path("state.json")
MAX_ITEMS = 25

# Layer display names (we will discover their IDs at runtime)
NAME_48_DAY12 = "QPF 48 Hour Day 1-2"
NAME_48_DAY45 = "QPF 48 Hour Day 4-5"
NAME_48_DAY67 = "QPF 48 Hour Day 6-7"
NAME_6H_PARENT = "QPF_6_Hour_Intervals"  # parent group of 6-hour interval sublayers

def http_json(url, timeout=25):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def find_layer_id_by_name(service_root_json, name):
    # Search top-level layers
    for lyr in service_root_json.get("layers", []):
        if lyr.get("name") == name:
            return lyr.get("id")
    # Search subLayers references (if present)
    for lyr in service_root_json.get("layers", []):
        for sub in (lyr.get("subLayers") or []):
            if sub.get("name") == name:
                return sub.get("id")
    return None

def discover_6h_sublayers(service_root_json):
    """Return [(id, name)] for the 6-hour interval sublayers in order by start hour."""
    parent_id = find_layer_id_by_name(service_root_json, NAME_6H_PARENT)
    out = []
    for lyr in service_root_json.get("layers", []):
        if lyr.get("parentLayerId") == parent_id:
            out.append((lyr["id"], lyr["name"]))
    def start_hr(nm):
        try:
            return int(nm.split("_")[1].split("-")[0])
        except Exception:
            return 9999
    out.sort(key=lambda x: start_hr(x[1]))
    return out

def point_query(layer_id, lat, lon):
    """Query one layer at a point; return QPF inches (prefer attribute 'qpf')."""
    geom = {"x": lon, "y": lat, "spatialReference": {"wkid": 4326}}
    params = {
        "where": "1=1",
        "geometry": json.dumps(geom),
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "outSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{SERVICE}/{layer_id}/query?{urllib.parse.urlencode(params)}"
    data = http_json(url)
    feats = data.get("features", [])
    if not feats:
        return 0.0
    attrs = feats[0].get("attributes", {})
    # The WPC layers use the 'qpf' field for renderer and values; try it first.
    if "qpf" in attrs and isinstance(attrs["qpf"], (int, float)):
        return float(attrs["qpf"])
    # Fallback: largest numeric attribute
    numeric = [float(v) for v in attrs.values() if isinstance(v, (int, float))]
    return max(numeric) if numeric else 0.0

def sliding_48h_from_6h_layers(sublayers_ids, lat, lon):
    """Sum any 8 consecutive 6-hour intervals => 48h total; return best total and label range."""
    values, names = [], []
    for lid, nm in sublayers_ids:
        q = point_query(lid, lat, lon)
        values.append(q)
        names.append(nm)
    best_total = 0.0
    best_range = None
    for i in range(0, max(0, len(values) - 8 + 1)):
        total = sum(values[i:i+8])
        if total > best_total:
            best_total = total
            best_range = (names[i], names[i+7], total)
    return best_total, best_range

def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default

def save_json(path, obj):
    path.write_text(json.dumps(obj, indent=2))

def escape_xml(s):
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def add_item(items, title, description, link):
    now = time.time()
    guid = f"fairfax-wpc-{int(now)}"
    pubDate = formatdate(now, usegmt=True)
    items.insert(0, {"title": title, "description": description, "link": link, "guid": guid, "pubDate": pubDate})
    return items

def write_rss(items):
    rss_items = []
    for it in items[:MAX_ITEMS]:
        rss_items.append(f"""
      <item>
        <title>{escape_xml(it['title'])}</title>
        <link>{escape_xml(it['link'])}</link>
        <guid isPermaLink="false">{escape_xml(it['guid'])}</guid>
        <pubDate>{it['pubDate']}</pubDate>
        <description><![CDATA[{it['description']}]]></description>
      </item>""")
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Fairfax VA — WPC 48h QPF Alerts (≥ {THRESHOLD_IN:.2f}")</title>
    <link>https://www.wpc.ncep.noaa.gov/qpf/day1-2.shtml</link>
    <description>Alerts whenever WPC indicates ≥ {THRESHOLD_IN:.2f}" in any 48h window through Day 7.</description>
    <lastBuildDate>{formatdate(time.time(), usegmt=True)}</lastBuildDate>
    {''.join(rss_items)}
  </channel>
</rss>"""
    FEED_FILE.write_text(xml, encoding="utf-8")

def main():
    items = load_json(ITEMS_FILE, [])
    state = load_json(STATE_FILE, {"alert_active": False})

    root = http_json(f"{SERVICE}?f=json")
    # Discover layer IDs by name (avoids brittle hard-coding)
    lid_12 = find_layer_id_by_name(root, NAME_48_DAY12)
    lid_45 = find_layer_id_by_name(root, NAME_48_DAY45)
    lid_67 = find_layer_id_by_name(root, NAME_48_DAY67)
    sixes = discover_6h_sublayers(root)

    best_total = 0.0
    best_desc  = ""

    # Sliding 48h windows for Days 1–3 (sum of 8 consecutive 6-hour intervals)
    if sixes:
        s_total, s_range = sliding_48h_from_6h_layers(sixes, LAT, LON)
        if s_total > best_total:
            best_total = s_total
            if s_range:
                best_desc = f"Sliding 48h (6h intervals): {s_range[0]} → {s_range[1]} total={s_total:.2f}\" (Days 1–3)"

    # Fixed 48h windows: Day 1–2
    if lid_12 is not None:
        t12 = point_query(lid_12, LAT, LON)
        if t12 > best_total:
            best_total = t12
            best_desc = f"Fixed 48h: Day 1–2 total={t12:.2f}\""

    # Fixed 48h windows: Day 4–5
    if lid_45 is not None:
        t45 = point_query(lid_45, LAT, LON)
        if t45 > best_total:
            best_total = t45
            best_desc = f"Fixed 48h: Day 4–5 total={t45:.2f}\""

    # Fixed 48h windows: Day 6–7
    if lid_67 is not None:
        t67 = point_query(lid_67, LAT, LON)
        if t67 > best_total:
            best_total = t67
            best_desc = f"Fixed 48h: Day 6–7 total={t67:.2f}\""

    # Decide alert publication
    should_alert = (best_total >= THRESHOLD_IN) and (not state.get("alert_active", False))
    if should_alert:
        title = f"Fairfax: 48‑hr rain ≥ {THRESHOLD_IN:.2f}\" (Forecast window total {best_total:.2f}\")"
        desc  = f"{best_desc}\nSource: WPC QPF (NOAA/NWS)."
        link  = "https://www.wpc.ncep.noaa.gov/qpf/day1-2.shtml"
        items = add_item(items, title, desc, link)
        state["alert_active"] = True

    # Reset flag when conditions dip below threshold to allow future alerts
    if state.get("alert_active", False) and best_total < THRESHOLD_IN:
        state["alert_active"] = False

    # Write outputs
    ITEMS_FILE.write_text(json.dumps(items[:MAX_ITEMS], indent=2))
    write_rss(items)
    STATE_FILE.write_text(json.dumps(state, indent=2))
    print(f"Best 48h total={best_total:.2f}\"; alert_active={state['alert_active']}")

if __name__ == "__main__":
    main()
