up:
	docker compose up -d

down:
	docker compose down

ingest:
	python -m src.ingest data/sample_air_quality.csv
transform:
	python -m src.transform
