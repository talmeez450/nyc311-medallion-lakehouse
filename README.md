# NYC 311 Medallion Lakehouse

Data Analysis and Visualization project. A Bronze / Silver / Gold pipeline on
Apache Spark and Delta Lake (Databricks Free Edition) using NYC 311
Service Requests (NYC Open Data, dataset erm2-nwe9).

Team: Talmeez ur Rehman (24L-2576), Mahad Jawad (24L-2504)

## Contents
- docs/       Phase 1 proposal
- sample_data/ full_load_sample.csv (CSV, 5,000 rows), incremental_sample.json (JSON, 5,000 rows)
- scripts/    get_samples.py (download samples), pii_scan.py and final_checks.py (PII checks),
              check_nulls.py, check_updated_at.py, check_future.py (source data checks)
- notebooks/  Bronze, Silver and Gold notebooks (Phase 2)