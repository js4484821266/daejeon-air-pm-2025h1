import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg

ROOT = Path(__file__).resolve().parents[1]
DDL_QUALITY = ROOT / "sql" / "ddl_quality.sql"

CHECKS = [
    (
        "raw_has_rows",
        "SELECT (COUNT(*) > 0) AS passed, ('count=' || COUNT(*)) AS details FROM raw_pm;",
    ),
    (
        "raw_measured_at_not_null",
        "SELECT (COUNT(*) = 0) AS passed, ('null_count=' || COUNT(*)) AS details FROM raw_pm WHERE measured_at IS NULL;",
    ),
    (
        "raw_station_not_empty",
        "SELECT (COUNT(*) = 0) AS passed, ('bad_count=' || COUNT(*)) AS details FROM raw_pm WHERE station IS NULL OR BTRIM(station) = '';",
    ),
    (
        "raw_pm_non_negative",
        "SELECT (COUNT(*) = 0) AS passed, ('bad_count=' || COUNT(*)) AS details FROM raw_pm WHERE (pm10 IS NOT NULL AND pm10 < 0) OR (pm25 IS NOT NULL AND pm25 < 0);",
    ),
    (
        "mart_has_rows",
        "SELECT (COUNT(*) > 0) AS passed, ('count=' || COUNT(*)) AS details FROM mart_daily_summary;",
    ),
]


def get_conn():
    load_dotenv(ROOT / ".env")
    db = os.getenv("POSTGRES_DB", "daejeon_air")
    user = os.getenv("POSTGRES_USER", "daejeon")
    pw = os.getenv("POSTGRES_PASSWORD", "daejeon_pw")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return psycopg.connect(
        f"host={host} port={port} dbname={db} user={user} password={pw}"
    )


def ensure_quality_table(conn):
    ddl = DDL_QUALITY.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def run_checks(conn):
    results = []
    with conn.cursor() as cur:
        for name, sql in CHECKS:
            cur.execute(sql)
            passed, details = cur.fetchone()
            results.append((name, bool(passed), str(details)))
    return results


def insert_logs(conn, results):
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO quality_log (check_name, passed, details) VALUES (%s, %s, %s)",
            results,
        )
    conn.commit()


def main():
    with get_conn() as conn:
        ensure_quality_table(conn)
        results = run_checks(conn)
        insert_logs(conn, results)

    # 콘솔 출력: CI/로컬에서 바로 보이게
    all_passed = True
    for name, passed, details in results:
        print(f"[{'OK' if passed else 'FAIL'}] {name} - {details}")
        all_passed = all_passed and passed

    if not all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
