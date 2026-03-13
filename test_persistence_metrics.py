from datetime import datetime, timedelta

from persistence import LegislativeDB


def _seed_sample_data(db: LegislativeDB):
    cursor = db.conn.cursor()
    today = datetime.now().date()
    # Two bills, one enacted, one active
    cursor.execute(
        """
        INSERT INTO bills (bill_id, uri, title, status, date_initiated, date_enacted, last_updated, lifecycle_phase, stage, sponsor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "1",
            "/bill/2026/1",
            "Sample Bill One",
            "Enacted",
            (today - timedelta(days=10)).strftime("%Y-%m-%d"),
            today.strftime("%Y-%m-%d"),
            today.strftime("%Y-%m-%d"),
            "Completed",
            "Fifth Stage",
            "Sponsor A",
        ),
    )
    cursor.execute(
        """
        INSERT INTO bills (bill_id, uri, title, status, date_initiated, date_enacted, last_updated, lifecycle_phase, stage, sponsor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "2",
            "/bill/2026/2",
            "Sample Bill Two",
            "Current",
            (today - timedelta(days=5)).strftime("%Y-%m-%d"),
            None,
            today.strftime("%Y-%m-%d"),
            "Active",
            "Second Stage",
            "Sponsor B",
        ),
    )
    db.conn.commit()


def test_legislative_metrics_by_status():
    db = LegislativeDB(db_path=":memory:")
    _seed_sample_data(db)
    metrics = db.get_legislative_metrics_by_status()
    assert "Enacted" in metrics
    assert metrics["Enacted"]["count"] == 1
    assert metrics["Enacted"]["min_days"] <= metrics["Enacted"]["max_days"]


def test_sponsor_metrics_basic():
    db = LegislativeDB(db_path=":memory:")
    _seed_sample_data(db)
    sponsors = db.get_sponsor_metrics()
    sponsor_names = {s["sponsor"] for s in sponsors}
    assert "Sponsor A" in sponsor_names
    assert "Sponsor B" in sponsor_names

