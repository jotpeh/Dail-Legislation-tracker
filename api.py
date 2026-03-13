from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query

from persistence import LegislativeDB
from messenger import LegislativeMessenger


app = FastAPI(title="Oireachtas Legislative Tracker API")


def get_db() -> LegislativeDB:
    # Simple singleton-ish pattern for this process
    # FastAPI will keep module-level state alive while the process runs.
    global _DB_INSTANCE
    try:
        return _DB_INSTANCE
    except NameError:
        _DB_INSTANCE = LegislativeDB()
        return _DB_INSTANCE


def get_messenger() -> LegislativeMessenger:
    global _MESSENGER_INSTANCE
    try:
        return _MESSENGER_INSTANCE
    except NameError:
        _MESSENGER_INSTANCE = LegislativeMessenger(get_db())
        return _MESSENGER_INSTANCE


@app.get("/reports/weekly")
def get_weekly_report(
    chamber: Optional[str] = Query(
        None, description="Chamber name: Dáil, Seanad, or Committee"
    ),
    days: int = Query(7, ge=1, le=90, description="Number of days in the reporting window"),
    bill_ids: Optional[List[str]] = Query(
        None, description="Optional list of bill_ids to filter the report to"
    ),
) -> Dict[str, Any]:
    """
    Return the weekly report content for a given chamber and time window.

    Content is returned as Markdown plus a simple structured representation
    of the items for easier consumption by a separate frontend.
    """
    messenger = get_messenger()
    db = get_db()

    # Normalize chamber casing to match existing code
    normalized_chamber = None
    if chamber:
        if chamber.lower() in {"dail", "dáil"}:
            normalized_chamber = "Dáil"
        elif chamber.lower() == "seanad":
            normalized_chamber = "Seanad"
        elif "committee" in chamber.lower():
            normalized_chamber = "Committee"
        else:
            raise HTTPException(status_code=400, detail="Invalid chamber name")

    updates = db.get_weekly_changes(chamber=normalized_chamber, days=days)
    if bill_ids:
        wanted = set(bill_ids)
        updates = [u for u in updates if u.get("bill_id") in wanted]

    content = messenger.generate_weekly_report(
        bill_ids=[u.get("bill_id") for u in updates] if updates else None,
        chamber=normalized_chamber,
        days=days,
    )

    return {
        "chamber": normalized_chamber,
        "days": days,
        "bill_ids": bill_ids,
        "markdown": content,
        "items": updates,
    }


@app.get("/metrics/legislation/status")
def get_legislation_status_metrics() -> Dict[str, Any]:
    """
    Aggregate metrics about days in progress broken down by status.
    """
    db = get_db()
    metrics = db.get_legislative_metrics_by_status()
    return {"by_status": metrics}


@app.get("/metrics/sponsors")
def get_sponsor_metrics() -> Dict[str, Any]:
    """
    Metrics for legislative sponsors (number of bills, lifecycle distribution, recency).
    """
    db = get_db()
    metrics = db.get_sponsor_metrics()
    return {"sponsors": metrics}


@app.get("/metrics/legislation/timeline")
def get_legislation_timelines(
    bill_ids: Optional[List[str]] = Query(
        None, description="Optional list of bill_ids; if omitted, all active bills are returned"
    )
) -> Dict[str, Any]:
    """
    Per-bill timelines including basic duration metrics.
    """
    db = get_db()
    if bill_ids:
        bills = [b for b in db.get_all_active_bills() if b["bill_id"] in set(bill_ids)]
    else:
        bills = db.get_all_active_bills()

    timelines: List[Dict[str, Any]] = []
    for bill in bills:
        tl = db.get_bill_timeline(bill["bill_id"])
        timelines.append(tl)

    return {"timelines": timelines}


@app.get("/bills/{bill_id}/timeline")
def get_bill_timeline(bill_id: str) -> Dict[str, Any]:
    """
    Change-tracking view per bill: timeline of events plus simple text diffs
    between successive summaries.
    """
    db = get_db()
    timeline = db.get_bill_timeline(bill_id)
    if not timeline:
        raise HTTPException(status_code=404, detail="Bill not found or no timeline available")
    return timeline


@app.get("/qc/summaries")
def list_summaries_for_qc(
    status: str = Query(
        "unreviewed",
        description="QC status filter: unreviewed, accepted, or flagged",
    )
) -> Dict[str, Any]:
    """
    List summaries needing review, including heuristic flags and metadata.
    """
    db = get_db()
    items = db.get_summaries_for_qc(status=status)
    return {"qc_status": status, "items": items}


@app.post("/qc/summaries/{debate_id}")
def update_summary_qc(
    debate_id: str,
    status: str = Query(..., description="New QC status: accepted or flagged"),
    notes: Optional[str] = Query(None, description="Optional reviewer notes"),
) -> Dict[str, Any]:
    """
    Update QC status for a given debate summary.
    """
    if status not in {"accepted", "flagged"}:
        raise HTTPException(status_code=400, detail="status must be 'accepted' or 'flagged'")

    db = get_db()
    ok = db.set_summary_qc_status(debate_id=debate_id, status=status, notes=notes)
    if not ok:
        raise HTTPException(status_code=404, detail="Debate not found")
    return {"debate_id": debate_id, "status": status, "notes": notes}


@app.get("/bills/{bill_id}/policies")
def get_bill_policies(bill_id: str) -> Dict[str, Any]:
    """
    Stub endpoint: returns any stored party-policy matches for a given bill.
    Actual matching and policy ingestion are deferred to a later phase.
    """
    db = get_db()
    matches = db.get_bill_policy_matches(bill_id)
    return {"bill_id": bill_id, "matches": matches}


@app.get("/policies/{party}")
def get_policies_for_party(party: str) -> Dict[str, Any]:
    """
    Stub endpoint: list stored policy documents and any linked bills for a party.
    """
    db = get_db()
    policies = db.get_policies_for_party(party)
    return {"party": party, "policies": policies}

