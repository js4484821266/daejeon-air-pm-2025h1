import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg

ROOT= Path(__file__).resolve().parents[1]
SQL_MART= ROOT / "sql" / "mart_daily_summary.sql"

def get_conn():
    load_dotenv(ROOT / ".env")
    db=os.getenv("POSTGRES_DB","daejeon_air")
    user=os.getenv("POSTGRES_USER","daejeon")
    pw=os.getenv("POSTGRES_PASSWORD","daejeon_pw")
    host=os.getenv("POSTGRES_HOST","localhost")
    port=os.getenv("POSTGRES_PORT","5432")
    return psycopg.connect(
        f"host={host} port={port} dbname={db} user={user} password={pw}"
    )

def main():
    sql=SQL_MART.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute("select count(*) from mart_daily_summary;")
            print("[COUNT]",cur.fetchone()[0])
        conn.commit()

if __name__=="__main__":
    main()