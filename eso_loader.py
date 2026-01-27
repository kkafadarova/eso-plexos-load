import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/plexos_load_master.xlsx")

import time
import requests
import re

ESO_FORECAST_TABLE_URL = "https://www.eso.bg/doc?37="

import pandas as pd
import requests

ESO_FORECAST_TABLE_URL = "https://www.eso.bg/doc?37="

def fetch_data():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,*/*",
    }
    r = requests.get(ESO_FORECAST_TABLE_URL, headers=headers, timeout=30)
    r.raise_for_status()

    # pandas ще извади всички таблици от HTML-а
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("Не намерих HTML таблици в https://www.eso.bg/doc?37=")

    # Обикновено правилната таблица е тази, която има колона с дата + 24 часа
    best = None
    best_score = -1
    for t in tables:
        # махаме напълно празни колони
        t = t.dropna(axis=1, how="all")
        # score: колко числови колони има (целта е ~24)
        numeric_cols = 0
        for col in t.columns:
            if col == t.columns[0]:
                continue
            s = pd.to_numeric(t[col], errors="coerce")
            if s.notna().sum() > 0:
                numeric_cols += 1
        if numeric_cols > best_score:
            best_score = numeric_cols
            best = t

    if best is None or best.empty:
        raise RuntimeError("Намерих таблици, но не открих подходяща таблица за прогноза.")

    # Първата колона приемаме, че е дата (пример: 26.01.2026)
    best = best.dropna(how="all")
    date_col = best.columns[0]

    # Нормализираме колони: взимаме първите 24 колони след датата
    cols = list(best.columns)
    if len(cols) < 25:
        raise RuntimeError(f"Таблицата няма достатъчно колони за 24 часа. Колони: {len(cols)}")

    hour_cols = cols[1:25]

    out = []
    for _, row in best.iterrows():
        dstr = str(row[date_col]).strip()
        vals = []
        for c in hour_cols:
            v = pd.to_numeric(row[c], errors="coerce")
            if pd.isna(v):
                vals = []
                break
            vals.append(int(round(float(v))))
        if len(vals) == 24 and dstr:
            out.append({"name": dstr, "data": vals})

    if not out:
        # Помага за дебъг в логовете
        sample_cols = ", ".join(map(str, best.columns[:8]))
        raise RuntimeError(f"Прочетох таблица, но не успях да извлека 24-часови редове. Примерни колони: {sample_cols}")

    return out

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
