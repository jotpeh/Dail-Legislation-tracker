# Oireachtas Legislative Tracker

A small command-line tool that pulls current Irish legislation, identifies recent debates, summarises them with a local LLM, and generates weekly reports for the Dáil, Seanad, and Committees.

## What it does
- Fetches active bills from the Oireachtas API
- Backfills the earliest known initiation date for each bill
- Finds relevant debates and summarises them locally
- Produces separate weekly reports per chamber

## Usage
```bash
python3 main.py
```

Debug mode and a limiter for quick testing:
```bash
python3 main.py --debug --limit-bills 3
```

## Outputs
Reports are written to:
- `report_dáil_YYYY_WW.md`
- `report_seanad_YYYY_WW.md`
- `report_committee_YYYY_WW.md`

An initiation-date dataset is also exported as `active_bills_initiation.csv`.
