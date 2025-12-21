import os
from dotenv import load_dotenv
import psycopg
import streamlit as st
import matplotlib.pyplot as plt

def get_conn():
    load_dotenv(".env")
    db=os.getenv("POSTGRES_DB","daejeon_air")
    user=os.getenv("POSTGRES_USER","daejeon")
    pw=os.getenv("POSTGRES_PASSWORD","daejeon_pw")
    host=os.getenv("POSTGRES_HOST","localhost")
    port=os.getenv("POSTGRES_PORT","5432")
    return psycopg.connect(
        f'dbname={db} user={user} password={pw} host={host} port={port}'
    )

st.set_page_config(page_title="Daejeon Air Quality Dashboard", layout="centered")
st.title("Daejeon Air Quality Dashboard")
st.caption("Data: mart_daily_summary (day, station, pm10_avg, pm25_avg, row_count)")

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("select distinct station from mart_daily_summary order by station;")
        stations=[row[0] for row in cur.fetchall()]

if not stations:
    st.warning("No station data available.")
    st.stop()

station=st.selectbox("Station", stations)

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("""
select day, pm10_avg, pm25_avg, row_count
from mart_daily_summary
where station=%s
order by day;
                    """,
                    (station,)
                    )
        rows=cur.fetchall()

days=[row[0] for row in rows]
pm10=[row[1] for row in rows]
pm25=[row[2] for row in rows]
counts=[row[3] for row in rows]

st.subheader("Daily summary table")
st.dataframe(
    [{"day":d,"pm10_avg":p10,"pm25_avg":p25,"row_count":rc} for d,p10,p25,rc in rows],
    use_container_width=True
)

st.subheader("Trend")
fig=plt.figure()
plt.plot(days, pm10, marker="o", label="PM10 avg")
plt.plot(days, pm25, marker="o", label="PM2.5 avg")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
st.pyplot(fig)