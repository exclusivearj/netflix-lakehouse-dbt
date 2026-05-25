# Seeds

The four CSVs in this directory are **placeholders** that exist only so `dbt parse` can resolve `ref()` calls.

Run `make setup` (or `python setup.py`) to download MovieLens 25M and overwrite these files with real data:
- `ratings.csv` — ~25M rows
- `movies.csv`  — ~62K rows
- `tags.csv`    — ~1M rows
- `users.csv`   — 162,541 synthetic rows (deterministic, `random.seed=42`)
