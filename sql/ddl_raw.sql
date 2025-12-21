CREATE TABLE IF NOT EXISTS raw_pm (
  measured_at TIMESTAMP NOT NULL,
  station TEXT NOT NULL,
  pm10 REAL,
  pm25 REAL
);
