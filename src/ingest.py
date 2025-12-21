import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg


ROOT = Path(__file__).resolve().parents[1]
SQL_DDL = ROOT / "sql" / "ddl_raw.sql"


def get_conn():
    load_dotenv(ROOT / ".env")
    db = os.getenv("POSTGRES_DB", "daejeon_air")
    user = os.getenv("POSTGRES_USER", "daejeon")
    pw = os.getenv("POSTGRES_PASSWORD", "daejeon_pw")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")

    # docker-compose로 띄운 postgres에 접속
    return psycopg.connect(
        f"host={host} port={port} dbname={db} user={user} password={pw}"
    )


def ensure_schema(conn):
    ddl = SQL_DDL.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def copy_csv(conn, csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    with csv_path.open("r", encoding="u8", newline="") as f:
        reader = csv.DictReader(f)
        rows = [(r["measured_at"], r["station"], r["pm10"], r["pm25"]) for r in reader]

    with conn.cursor() as cur:
        cur.executemany(
            """
        INSERT INTO raw_pm (measured_at, station, pm10, pm25)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (measured_at, station) DO UPDATE SET
          pm10 = EXCLUDED.pm10,
          pm25 = EXCLUDED.pm25
            """,
            rows,
        )

    conn.commit()


def main():
    csv_path = (
        Path(sys.argv[1])
        if len(sys.argv) >= 2
        else (ROOT / "data" / "sample_air_quality.csv")
    )

    with get_conn() as conn:
        ensure_schema(conn)
        copy_csv(conn, csv_path)
        # debug
        with conn.cursor() as cur:
            cur.execute("select count(*) from raw_pm;")
            print("[COUNT]", cur.fetchone()[0])

    print(f"[OK] loaded: {csv_path}")


if __name__ == "__main__":
    main()
