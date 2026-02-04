import os
from io import StringIO
from datetime import datetime
import pandas as pd
import requests

ESO_URL = os.getenv("ESO_URL", "https://www.eso.bg/doc?37=")  # това работи при теб по логовете
OUT_PATH = os.getenv("OUT_PATH", "data/plexos_load_master.xlsx")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")

HOUR_COLS = [str(i) for i in range(1, 25)]
FINAL_COLS = ["Year", "Month", "Day"] + HOUR_COLS


def fetch_forecast_table() -> pd.DataFrame:
    """
    Чете HTML страницата и вади таблицата "Прогноза на товара на ЕЕС".
    Връща dataframe с колони: Date + 1..24 (или подобни), които после нормализираме.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    r = requests.get(ESO_URL, headers=headers, timeout=30)
    r.raise_for_status()

    # IMPORTANT: pandas.read_html приема string, но е по-сигурно през StringIO
    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError(f"Не намерих HTML таблици в {ESO_URL}")

    # Обикновено правилната таблица е първата (с 'Дата/Час' + 1..24)
    # Ако някой ден излезе друга първа, можем да търсим по колони.
    best = None
    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        if any("Дата" in c for c in cols) and any(c == "1" for c in cols) and any(c == "24" for c in cols):
            best = t
            break
    if best is None:
        best = tables[0]

    return best


def normalize_new_data(raw: pd.DataFrame) -> pd.DataFrame:
    # Нормализира името на първата колона към "Дата/Час"
    raw = raw.copy()
    raw.columns = [str(c).strip() for c in raw.columns]

    date_col = raw.columns[0]  # първата колона е дата
    df = raw.rename(columns={date_col: "Date"})

    # Вадим само Date + 1..24
    missing = [c for c in HOUR_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Липсват часови колони {missing}. Колони: {list(df.columns)}")

    df = df[["Date"] + HOUR_COLS].copy()

    # Date: "28.01.2026" -> datetime
    df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="raise")
    df["Year"] = df["Date"].dt.year.astype(int)
    df["Month"] = df["Date"].dt.month.astype(int)
    df["Day"] = df["Date"].dt.day.astype(int)
    df = df.drop(columns=["Date"])

    # часовете към int (ако има NaN/стрингове)
    for c in HOUR_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # ред/колони
    df = df[FINAL_COLS].copy()

    return df


def read_existing(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=FINAL_COLS)

    existing = pd.read_excel(path, sheet_name=SHEET_NAME, dtype="Int64")
    # Ако е празно/различна структура, пак го нормализираме
    existing.columns = [str(c).strip() for c in existing.columns]

    # гарантираме, че имаме нужните колони
    for col in FINAL_COLS:
        if col not in existing.columns:
            existing[col] = pd.NA

    existing = existing[FINAL_COLS].copy()
    # Year/Month/Day като int
    for c in ["Year", "Month", "Day"]:
        existing[c] = pd.to_numeric(existing[c], errors="coerce").astype("Int64")

    return existing


def merge_append(existing: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    # Ключ = дата (Year/Month/Day)
    key_cols = ["Year", "Month", "Day"]

    # махаме дубликати в new_df
    new_df = new_df.drop_duplicates(subset=key_cols, keep="last")

    # merge: взимаме всички стари + само нови дати
    merged = pd.concat([existing, new_df], ignore_index=True)

    # махаме дубликати по дата (ако има, предпочитаме "last" = новите стойности)
    merged = merged.drop_duplicates(subset=key_cols, keep="last")

    # сортиране по дата
    merged["_dt"] = pd.to_datetime(
        merged["Year"].astype(str) + "-" + merged["Month"].astype(str) + "-" + merged["Day"].astype(str),
        errors="coerce",
    )
    merged = merged.sort_values("_dt").drop(columns=["_dt"]).reset_index(drop=True)

    return merged


def write_xlsx(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=SHEET_NAME)


def main():
    print("Fetching ESO data…")
    raw = fetch_forecast_table()
    new_df = normalize_new_data(raw)

    existing = read_existing(OUT_PATH)
    merged = merge_append(existing, new_df)

    before = len(existing)
    after = len(merged)
    print(f"Rows: {before} -> {after} (added/updated: {after - before if after >= before else 0})")

    write_xlsx(merged, OUT_PATH)
    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
