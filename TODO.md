# Fix Postgres "raw_telemetry does not exist" Error ✓
MakFleet Ingestion Pipeline

## Current Status
- [x] Diagnosed: Missing Postgres table `raw_telemetry`
- [x] Confirmed: CSV generation works, DB schema missing
- [x] Updated: Created `app/setup_db_fixed.py` with correct UUID schema + seeds
- [ ] Run DB init: `python app/setup_db_fixed.py`
- [ ] Test pipeline

## Steps to Complete
1. **Test DB connection**:
   ```
   python -c "from app.db.postgres import test_pg_connection; print(test_pg_connection())"
   ```
2. **Create tables**:
   ```
   python app/setup_db_fixed.py
   ```
3. **Verify tables**:
   ```
   psql -h localhost -U postgres -d makfleet -c "\\dt"
   ```
4. **Retry full pipeline**:
   ```
   python -m app.ingestion.simulator
   python -m app.ingestion.loader
   python -m app.ingestion.enricher
   ```

## Docker Alternative (No Code Changes)
```
docker-compose up postgres  # Auto-runs init/*.sql
```

Progress: Run setup → mark step 2 ✅
