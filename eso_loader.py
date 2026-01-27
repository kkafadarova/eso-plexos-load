import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/plexos_load_master.xlsx")

import time
import requests
import re

ESO_FORECAST_TABLE_URL = "https://www.eso.bg/doc?37="

def fetch_data():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,*/*",
    }
    r = requests.get(ESO_FORECAST_TABLE_URL, headers=headers, timeout=30)
    r.raise_for_status()
    html = r.text

    # Търсим редове: dd.mm.yyyy + 24 числа
    pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4})\s+((?:\d+\s+){23}\d+)")
    matches = pattern.findall(html)
    if not matches:
        raise RuntimeError("Не намерих редове с прогноза в https://www.eso.bg/doc?37=")

    # Връщаме в същия формат като преди: list от {name, data}
    out = []
    for dstr, values in matches:
        vals = list(map(int, values.split()))
        if len(vals) != 24:
            continue
        out.append({"name": dstr, "data": vals})

    if not out:
        raise RuntimeError("Намерих редове, но никой няма 24 стойности.")
    return out


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
