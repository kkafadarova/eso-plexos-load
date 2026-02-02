import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup

URL = "https://www.eso.bg/doc?37="

MASTER_FILE = Path("data/plexos_load_master.xlsx")
ARCHIVE_DIR = Path("data/archive")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "14"))  # можеш да го смениш от workflow env


def fetch_data() -> pd.DataFrame:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "bg-BG,bg;q=0.9,en;q=0.8",
    }
    r = requests.get(URL, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("Не е намерена таблица с прогноза (HTML <table> липсва).")

    rows = table.find_all("tr")
    if len(rows) < 2:
        raise RuntimeError("Таблицата е празна или няма редове.")

    data = []
    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if not cols:
            continue

        d = datetime.strptime(cols[0], "%d.%m.%Y").date()
        hours = [int(x) for x in cols[1:25]]

        rec = {"Year": d.year, "Month": d.month, "Day": d.day}
        # Плексос формат: 1..24
        for i in range(24):
            rec[str(i + 1)] = hours[i]
        data.append(rec)

    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("Не успях да извлека данни (df е празен).")

    return df.sort_values(["Year", "Month", "Day"]).reset_index(drop=True)


from datetime import date

def merge_with_existing(new_df: pd.DataFrame) -> pd.DataFrame:
    today = date.today()

    if not MASTER_FILE.exists():
        return new_df

    old_df = pd.read_excel(MASTER_FILE)

    # гарантираме типове
    for c in ["Year", "Month", "Day"]:
        old_df[c] = old_df[c].astype(int)
        new_df[c] = new_df[c].astype(int)

    old_df["__date__"] = pd.to_datetime(
        old_df[["Year", "Month", "Day"]]
    ).dt.date

    new_df["__date__"] = pd.to_datetime(
        new_df[["Year", "Month", "Day"]]
    ).dt.date

    # 1️⃣ миналото: пазим само от old_df
    past = old_df[old_df["__date__"] < today]

    # 2️⃣ бъдещето: взимаме от new_df (обновена прогноза)
    future = new_df[new_df["__date__"] >= today]

    merged = pd.concat([past, future], ignore_index=True)

    merged = (
        merged.sort_values(["Year", "Month", "Day"])
        .drop(columns="__date__")
        .reset_index(drop=True)
    )

    return merged

def archive_snapshot(df: pd.DataFrame) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime("%Y-%m-%d")
    archive_path = ARCHIVE_DIR / f"plexos_load_{stamp}.xlsx"
    df.to_excel(archive_path, index=False)
    return archive_path


def cleanup_old_archives():
    if not ARCHIVE_DIR.exists():
        return

    cutoff = date.today() - timedelta(days=RETENTION_DAYS)
    for p in ARCHIVE_DIR.glob("plexos_load_*.xlsx"):
        # очакваме plexos_load_YYYY-MM-DD.xlsx
        name = p.stem  # plexos_load_YYYY-MM-DD
        try:
            stamp = name.replace("plexos_load_", "")
            d = datetime.strptime(stamp, "%Y-%m-%d").date()
        except Exception:
            continue

        if d < cutoff:
            p.unlink()


def main():
    print("Fetching ESO data...")
    new_df = fetch_data()

    print("Merging with existing master...")
    final_df = merge_with_existing(new_df)

    MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_excel(MASTER_FILE, index=False)
    print(f"✅ Updated master: {MASTER_FILE} (rows={len(final_df)})")

    archive_path = archive_snapshot(final_df)
    print(f"📌 Archived snapshot: {archive_path}")

    cleanup_old_archives()
    print(f"🧹 Cleanup done (retention={RETENTION_DAYS} days)")


if __name__ == "__main__":
    main()
