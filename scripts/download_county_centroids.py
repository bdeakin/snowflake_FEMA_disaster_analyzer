import csv
import io
import zipfile
from pathlib import Path

import requests

GAZ_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2023_Gazetteer/2023_Gaz_counties_national.zip"
)
OUTPUT_PATH = Path("data/county_centroids.csv")


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(GAZ_URL, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = next((n for n in zf.namelist() if n.endswith("_counties_national.txt")), None)
        if not name:
            raise RuntimeError("Could not find counties file in Gazetteer zip")
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.reader(text, delimiter="\t")
            header = [h.strip() for h in next(reader)]
            index = {col: i for i, col in enumerate(header)}
            rows = []
            for row in reader:
                geoid = row[index["GEOID"]] if "GEOID" in index else ""
                name = row[index["NAME"]] if "NAME" in index else ""
                state_fips = row[index["STATE"]] if "STATE" in index else ""
                state_abbr = row[index["USPS"]] if "USPS" in index else ""
                lat = row[index["INTPTLAT"]] if "INTPTLAT" in index else ""
                lon = row[index["INTPTLONG"]] if "INTPTLONG" in index else ""
                if not geoid or not lat or not lon:
                    continue
                rows.append(
                    {
                        "county_fips": geoid.zfill(5),
                        "county_name": name,
                        "state_fips": state_fips,
                        "state_abbr": state_abbr,
                        "centroid_lat": lat,
                        "centroid_lon": lon,
                    }
                )

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "county_fips",
                "county_name",
                "state_fips",
                "state_abbr",
                "centroid_lat",
                "centroid_lon",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
