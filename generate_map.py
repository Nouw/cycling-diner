#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
from urllib.parse import quote, urlencode
from urllib.request import urlopen, Request


UTRECHT_CENTER = (5.1214201, 52.0907374)  # lon, lat
DEFAULT_ZOOM = 12
DEFAULT_SIZE = (1280, 1280)
MARKER_COLOR = "ff2d20"  # red-ish
RATE_LIMIT_DELAY = 0.15  # seconds between geocoding requests


def read_addresses(csv_path: str):
    addresses = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stad = (row.get("Stad") or "").strip()
            adres = (row.get("Adres") or "").strip()
            postcode = (row.get("Postcode") or "").strip()
            if not adres:
                continue
            # Prefer Utrecht, but include all if no Stad column match
            if stad and stad.lower() != "utrecht":
                continue
            full = ", ".join([p for p in [adres, postcode, "Utrecht, Nederland"] if p])
            addresses.append(full)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for a in addresses:
        if a not in seen:
            seen.add(a)
            unique.append(a)
    return unique


def geocode_address(address: str, token: str):
    base = "https://api.mapbox.com/geocoding/v5/mapbox.places/" + quote(address) + ".json"
    qs = {
        "access_token": token,
        "limit": 1,
        "proximity": f"{UTRECHT_CENTER[0]},{UTRECHT_CENTER[1]}",
        "language": "nl",
        "country": "nl",
    }
    url = base + "?" + urlencode(qs)
    req = Request(url, headers={"User-Agent": "cycling-diner/1.0"})
    with urlopen(req) as resp:
        data = json.load(resp)
    feats = data.get("features") or []
    if not feats:
        return None
    # center: [lon, lat]
    return feats[0].get("center")


def geocode_all(addresses, token):
    coords = []  # list of (lon, lat)
    for idx, addr in enumerate(addresses, start=1):
        try:
            c = geocode_address(addr, token)
        except Exception as e:
            print(f"Geocoding error for '{addr}': {e}", file=sys.stderr)
            c = None
        if c:
            lon, lat = float(c[0]), float(c[1])
            coords.append((lon, lat))
        else:
            print(f"Warning: no result for '{addr}'", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)
    # Deduplicate coordinates
    seen = set()
    unique_coords = []
    for c in coords:
        if c not in seen:
            seen.add(c)
            unique_coords.append(c)
    return unique_coords


def build_marker_overlays(coords):
    # Mapbox static marker overlay: pin-s+COLOR(lon,lat)
    parts = []
    for lon, lat in coords:
        parts.append(f"pin-s+{MARKER_COLOR}({lon:.6f},{lat:.6f})")
    return ",".join(parts)


def build_static_url(overlays: str, token: str, center=UTRECHT_CENTER, zoom=DEFAULT_ZOOM, size=DEFAULT_SIZE):
    lon, lat = center
    width, height = size
    # style can be changed if desired (e.g., streets-v12, light-v11)
    base = "https://api.mapbox.com/styles/v1/mapbox/streets-v12/static/"
    path = f"{overlays}/{lon:.6f},{lat:.6f},{zoom},0,0/{width}x{height}@2x"
    return f"{base}{path}?{urlencode({'access_token': token})}"


def download(url: str, out_path: str):
    req = Request(url, headers={"User-Agent": "cycling-diner/1.0"})
    with urlopen(req) as resp, open(out_path, "wb") as f:
        f.write(resp.read())


def chunk(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


def main():
    token = os.getenv("MAPBOX_TOKEN")
    if not token:
        print("Error: set MAPBOX_TOKEN in your environment.", file=sys.stderr)
        sys.exit(1)

    csv_path = os.getenv("DUOS_CSV", "data/duos.csv")
    if not os.path.exists(csv_path):
        print(f"Error: CSV not found at {csv_path}", file=sys.stderr)
        sys.exit(1)

    addresses = read_addresses(csv_path)
    if not addresses:
        print("No addresses found.")
        sys.exit(0)

    print(f"Found {len(addresses)} Utrecht addresses. Geocoding…")
    coords = geocode_all(addresses, token)
    print(f"Resolved {len(coords)} coordinate pairs.")

    if not coords:
        print("No coordinates resolved; nothing to map.")
        sys.exit(0)

    # Build overlays, respecting URL length. Aim for conservative chunk sizes.
    # Each marker ~ 30–40 chars; 150 markers ~ ~6k chars. Keep chunks to 120 markers.
    overlays_all = build_marker_overlays(coords)
    test_url = build_static_url(overlays_all, token)
    if len(test_url) < 7800:
        out = "utrecht_markers.png"
        print(f"Downloading static map to {out}…")
        download(test_url, out)
        print(f"Saved {out}")
        return

    # URL too long; split into multiple images
    print("URL too long for a single image; chunking markers…")
    # Roughly 100 markers per chunk; adjust as needed
    per = 100
    for i, sub in enumerate(chunk(coords, per), start=1):
        overlays = build_marker_overlays(sub)
        url = build_static_url(overlays, token)
        out = f"utrecht_markers_{i}.png"
        print(f"Downloading {out}…")
        download(url, out)
    print("Done.")


if __name__ == "__main__":
    main()

