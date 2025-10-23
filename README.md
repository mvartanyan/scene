# Scene Development Quickstart

## Prerequisites
- Python 3.12 (a virtual environment is already provided at `venv/`).

## Install Dependencies
```bash
source venv/bin/activate
pip install -e .
```
> The project uses `pyproject.toml`; alternatively run `pip install -r requirements.txt` if you export one later.

## Run the Development Server
```bash
source venv/bin/activate
uvicorn app.main:app --reload
```
- App served on `http://127.0.0.1:8000/`.
- Hot reload is enabled via `--reload`.

## Key Screens
- **Projects**: Select a project then manage Pages, Tasks, and Batches via Bootstrap tabs. Inline edit/delete is available via collapsible forms.
- **Runs**: Filter and review runs with a live detail panel; list updates via HTMX.
- **Config**: Use the gear icon in the navbar to toggle browser availability and manage viewport presets. Browsers in use stay locked; add new entries with the inline form.

## Run Tests
```bash
source venv/bin/activate
pytest
```
