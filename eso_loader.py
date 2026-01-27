import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/plexos_load_master.xlsx")


import time
import requests

ESO_URL = "https://eso.bg/load_plus_forecast.json.php"

def fetch_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (GitHubActions; eso-plexos-load/1.0)",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://eso.bg/doc/?460",
    }

    last_err = None
    for attempt in range(1, 4):  # 3 опита
        try:
            r = requests.get(ESO_URL, headers=headers, timeout=30, allow_redirects=True)

            # ако е rate limit / временен проблем
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Temporary HTTP {r.status_code}")

            r.raise_for_status()

            # ако върнат HTML вместо JSON
            ctype = (r.headers.get("Content-Type") or "").lower()
            text = r.text.strip()

            if not text:
                raise RuntimeError("Empty response body (no JSON).")

            if "application/json" not in ctype and not text.startswith("[") and not text.startswith("{"):
                snippet = text[:300].replace("\n", " ")
                raise RuntimeError(f"Non-JSON response (Content-Type={ctype}): {snippet}")

            return r.json()

        except Exception as e:
            last_err = e
            # backoff
            time.sleep(2 * attempt)

    raise RuntimeError(f"Failed to fetch ESO JSON after retries: {last_err}")



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
