import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/plexos_load_master.xlsx")

import time
import requests
import re
import pandas as pd
import io


ESO_FORECAST_TABLE_URL = "https://www.eso.bg/doc?37="

def fetch_data():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,*/*",
    }
    r = requests.get(ESO_FORECAST_TABLE_URL, headers=headers, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(io.StringIO(r.text))
    if not tables:
        raise RuntimeError("Не намерих HTML таблици в https://www.eso.bg/doc?37=")

    # взимаме таблицата с най-много колони (трябва да е 25: дата + 24 часа)
    best = max(tables, key=lambda t: t.shape[1])

    # нормализираме
    best = best.dropna(how="all")
    best = best.dropna(axis=1, how="all")

    if best.shape[1] < 25:
        raise RuntimeError(f"Таблицата няма 25 колони (дата + 24 часа). shape={best.shape}")

    date_col = best.columns[0]
    hour_cols = list(best.columns[1:25])

    out = []
    for _, row in best.iterrows():
        dstr = str(row[date_col]).strip()
        vals = []
        ok = True
        for c in hour_cols:
            v = pd.to_numeric(row[c], errors="coerce")
            if pd.isna(v):
                ok = False
                break
            vals.append(int(round(float(v))))
        if ok and len(vals) == 24 and dstr:
            out.append({"name": dstr, "data": vals})

    if not out:
        raise RuntimeError("Прочетох таблицата, но не успях да извлека редове с 24 стойности.")

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
