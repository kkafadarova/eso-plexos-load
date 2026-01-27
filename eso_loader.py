import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

ESO_URL = "https://www.eso.bg/load_plus_forecast.json.php"
OUTPUT_FILE = Path("data/plexos_load_master.xlsx")


def fetch_data():
    r = requests.get(ESO_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def build_dataframe(data):
    rows = []
    for entry in data:
        d = datetime.strptime(entry["name"], "%d.%m.%Y")
        vals = entry["data"]

        if len(vals) != 24:
            continue

        row = {"Year": d.year, "Month": d.month, "Day": d.day}
        for i, v in enumerate(vals):
            row[str(i + 1)] = v

        rows.append(row)

    df = pd.DataFrame(rows)
    return df.sort_values(["Year", "Month", "Day"])


def main():
    print("Fetching ESO data...")
    data = fetch_data()

    print("Building dataframe...")
    df = build_dataframe(data)

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    print(f"✅ Updated {OUTPUT_FILE} ({len(df)} rows)")


if __name__ == "__main__":
    main()
