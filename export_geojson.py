#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
from typing import Dict, Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


UTRECHT_CENTER = (5.1214201, 52.0907374)  # lon, lat
RATE_LIMIT_DELAY = 0.15


def read_rows(csv_path: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            row["__row"] = i
            yield row


def build_full_address(row: Dict[str, Any]) -> str:
    adres = (row.get("Adres") or "").strip()
    postcode = (row.get("Postcode") or "").strip()
    return ", ".join([p for p in [adres, postcode, "Utrecht, Nederland"] if p])


def geocode(address: str, token: str):
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
    c = feats[0].get("center")
    return (float(c[0]), float(c[1])) if c else None


def sanitize_props(row: Dict[str, Any]) -> Dict[str, Any]:
    # Keep only non-sensitive fields for Studio
    keep = {
        "Naam persoon A": "naamA",
        "Naam persoon B": "naamB",
        "Adres": "adres",
        "Postcode": "postcode",
        "Stad": "stad",
        "Team persoon A": "teamA",
        "Team persoon B": "teamB",
        "Dieetwensen": "dieet",
        "Allergieën": "allergie",
        "Overige opmerkingen": "opmerking",
    }
    out = {v: (row.get(k) or "").strip() for k, v in keep.items()}
    out["row"] = row.get("__row")
    return out


def main():
    token = os.getenv("MAPBOX_TOKEN")
    if not token:
        print("Error: set MAPBOX_TOKEN in your environment.", file=sys.stderr)
        sys.exit(1)

    csv_path = os.getenv("DUOS_CSV", "data/duos.csv")
    out_path = os.getenv("GEOJSON_OUT", "data/duos_points.geojson")

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    features = []
    for row in read_rows(csv_path):
        stad = (row.get("Stad") or "").strip().lower()
        if stad and stad != "utrecht":
            continue
        address = build_full_address(row)
        if not address:
            continue
        try:
            coord = geocode(address, token)
        except Exception as e:
            print(f"Geocoding failed for '{address}': {e}", file=sys.stderr)
            coord = None
        if not coord:
            print(f"No result for '{address}'", file=sys.stderr)
            continue
        lon, lat = coord
        props = sanitize_props(row)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
        time.sleep(RATE_LIMIT_DELAY)

    fc = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    print(f"Wrote {len(features)} features to {out_path}")


if __name__ == "__main__":
    main()

