# Clinical Copilot Engine (Backend)

Production-oriented backend refactor for Solr-powered clinical autocomplete.

## Structure

- `app/` runtime application package
  - `app/app.py` FastAPI entrypoint (filename unchanged)
  - `app/api/` API routes
  - `app/core/` shared config
  - `app/services/` note completion + context parsing services
  - `app/models/` Pydantic models
- `scripts/` ETL and maintenance scripts
- `tests/` test suite
- `infra/` infra configuration
- `data/` runtime data artifacts (`*.json`, `*.log`)

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

Compatibility import for existing code/tests is preserved:

```python
from backend import app
```

## Test

```bash
pytest -q
```

## Notes

- Existing filenames were preserved per refactor constraints.
- Logic was not changed except import/path updates required by file movement.
