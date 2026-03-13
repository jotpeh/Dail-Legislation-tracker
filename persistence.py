import sqlite3
from datetime import datetime

class LegislativeDB:
    def __init__(self, db_path="oireachtas_tracker.db"):
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        # Table for Bills
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bills (
                bill_id TEXT PRIMARY KEY,
                uri TEXT,
                title TEXT,
                status TEXT,
                date_initiated TEXT,
                date_enacted TEXT,
                last_updated TEXT,
                lifecycle_phase TEXT,
                stage TEXT,
                sponsor TEXT
            )
        ''')
        # Backwards-compatible column add (if DB already exists)
        cursor.execute("PRAGMA table_info(bills)")
        existing_bill_cols = {row[1] for row in cursor.fetchall()}
        if "stage" not in existing_bill_cols:
            cursor.execute("ALTER TABLE bills ADD COLUMN stage TEXT")
        if "sponsor" not in existing_bill_cols:
            cursor.execute("ALTER TABLE bills ADD COLUMN sponsor TEXT")

        # Table for Debates (to avoid duplicate summaries)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS debates (
                debate_id TEXT PRIMARY KEY,
                bill_id TEXT,
                debate_date TEXT,
                transcript_content TEXT,
                is_summarized INTEGER DEFAULT 0,
            chamber TEXT,
            qc_status TEXT,
            qc_flags TEXT,
            qc_notes TEXT
            )
        ''')
        # Backwards-compatible column add (if DB already exists)
        cursor.execute("PRAGMA table_info(debates)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        if "chamber" not in existing_cols:
            cursor.execute("ALTER TABLE debates ADD COLUMN chamber TEXT")
        if "qc_status" not in existing_cols:
            cursor.execute("ALTER TABLE debates ADD COLUMN qc_status TEXT")
        if "qc_flags" not in existing_cols:
            cursor.execute("ALTER TABLE debates ADD COLUMN qc_flags TEXT")
        if "qc_notes" not in existing_cols:
            cursor.execute("ALTER TABLE debates ADD COLUMN qc_notes TEXT")

        # Table for party policies (architecture for future matching)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS party_policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                party TEXT,
                url TEXT,
                title TEXT,
                body_text TEXT,
                last_fetched TEXT
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bill_policy_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id TEXT,
                party_policy_id INTEGER,
                party TEXT,
                score REAL,
                summary TEXT,
                FOREIGN KEY (party_policy_id) REFERENCES party_policies(id)
            )
            """
        )

        # Indexes for faster lookups as the DB grows
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_debates_bill_chamber ON debates(bill_id, chamber)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_debates_date ON debates(debate_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bills_phase ON bills(lifecycle_phase)")
        self.conn.commit()

    # --- Metrics & timelines -------------------------------------------------

    def get_legislative_metrics_by_status(self):
        """Return counts and duration stats grouped by bill status."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT bill_id, status, date_initiated, date_enacted
            FROM bills
            """
        )
        rows = cursor.fetchall()
        by_status = {}
        for bill_id, status, date_initiated, date_enacted in rows:
            if not date_initiated:
                continue
            try:
                start = datetime.strptime(date_initiated[:10], "%Y-%m-%d")
                end = (
                    datetime.strptime(date_enacted[:10], "%Y-%m-%d")
                    if date_enacted
                    else datetime.now()
                )
                days = (end - start).days
            except Exception:
                continue
            bucket = by_status.setdefault(
                status or "Unknown", {"count": 0, "total_days": 0, "min_days": None, "max_days": None}
            )
            bucket["count"] += 1
            bucket["total_days"] += days
            bucket["min_days"] = days if bucket["min_days"] is None else min(bucket["min_days"], days)
            bucket["max_days"] = days if bucket["max_days"] is None else max(bucket["max_days"], days)

        # Compute averages
        for status, data in by_status.items():
            if data["count"]:
                data["avg_days"] = data["total_days"] / data["count"]
        return by_status

    def get_sponsor_metrics(self):
        """Aggregate basic metrics by sponsor."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT bill_id, sponsor, status, date_initiated, date_enacted
            FROM bills
            """
        )
        rows = cursor.fetchall()
        sponsors = {}
        for bill_id, sponsor, status, date_initiated, date_enacted in rows:
            key = sponsor or "Unknown"
            bucket = sponsors.setdefault(
                key,
                {
                    "sponsor": key,
                    "bill_count": 0,
                    "active_count": 0,
                    "enacted_count": 0,
                    "min_initiated": None,
                    "max_initiated": None,
                },
            )
            bucket["bill_count"] += 1
            if status == "Enacted":
                bucket["enacted_count"] += 1
            else:
                bucket["active_count"] += 1
            if date_initiated:
                if not bucket["min_initiated"] or date_initiated < bucket["min_initiated"]:
                    bucket["min_initiated"] = date_initiated
                if not bucket["max_initiated"] or date_initiated > bucket["max_initiated"]:
                    bucket["max_initiated"] = date_initiated
        return list(sponsors.values())

    def get_bill_timeline(self, bill_id):
        """Return a per-bill timeline including debates and basic duration."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT bill_id, uri, title, status, date_initiated, date_enacted, stage, sponsor
            FROM bills
            WHERE bill_id = ?
            """,
            (bill_id,),
        )
        bill_row = cursor.fetchone()
        if not bill_row:
            return {}
        bill = {
            "bill_id": bill_row[0],
            "uri": bill_row[1],
            "title": bill_row[2],
            "status": bill_row[3],
            "date_initiated": bill_row[4],
            "date_enacted": bill_row[5],
            "stage": bill_row[6],
            "sponsor": bill_row[7],
        }

        # Duration
        duration_days = self.get_duration(bill_id)

        cursor.execute(
            """
            SELECT debate_id, debate_date, chamber, transcript_content, qc_status, qc_flags
            FROM debates
            WHERE bill_id = ?
              AND is_summarized = 1
            ORDER BY date(debate_date) ASC
            """,
            (bill_id,),
        )
        debate_rows = cursor.fetchall()
        debates = []
        for row in debate_rows:
            debates.append(
                {
                    "debate_id": row[0],
                    "debate_date": row[1],
                    "chamber": row[2],
                    "summary": row[3],
                    "qc_status": row[4],
                    "qc_flags": row[5],
                    # diff_from_previous will be filled in below
                }
            )

        # Simple text-level change tracking between successive summaries
        import difflib

        prev_summary = None
        for event in debates:
            curr_summary = event.get("summary") or ""
            if prev_summary is None:
                event["diff_from_previous"] = None
            else:
                diff_lines = list(
                    difflib.unified_diff(
                        prev_summary.splitlines(),
                        curr_summary.splitlines(),
                        lineterm="",
                    )
                )
                event["diff_from_previous"] = "\n".join(diff_lines) if diff_lines else ""
            prev_summary = curr_summary

        return {
            "bill": bill,
            "duration_days": duration_days,
            "events": debates,
        }

    def save_bill(self, bill_data):
        cursor = self.conn.cursor()
        incoming_init = bill_data.get('date_initiated')

        # Fetch existing initiation date (preserve only if incoming is missing)
        cursor.execute("SELECT date_initiated FROM bills WHERE bill_id=?", (bill_data['bill_id'],))
        row = cursor.fetchone()
        existing_init = row[0] if row and row[0] else None

        chosen_init = incoming_init or existing_init

        cursor.execute('''
            INSERT INTO bills (bill_id, uri, title, status, date_initiated, date_enacted, last_updated, lifecycle_phase, stage, sponsor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bill_id) DO UPDATE SET
                status=excluded.status,
                last_updated=excluded.last_updated,
                date_enacted=excluded.date_enacted,
                lifecycle_phase=excluded.lifecycle_phase,
                date_initiated=COALESCE(excluded.date_initiated, bills.date_initiated),
                stage=COALESCE(excluded.stage, bills.stage),
                sponsor=COALESCE(excluded.sponsor, bills.sponsor)
        ''', (
            bill_data['bill_id'], bill_data['uri'], bill_data['title'], 
            bill_data['status'], chosen_init, 
            bill_data.get('date_enacted'), bill_data['last_updated'],
            bill_data['lifecycle_phase'],
            bill_data.get('stage'),
            bill_data.get('sponsor')
        ))
        self.conn.commit()

    def get_duration(self, bill_id):
        """Calculates duration: Current Age if active, Total Time if enacted."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT date_initiated, date_enacted FROM bills WHERE bill_id=?", (bill_id,))
        row = cursor.fetchone()
        if not row: return None
        
        start = datetime.strptime(row[0], '%Y-%m-%d')
        end = datetime.strptime(row[1], '%Y-%m-%d') if row[1] else datetime.now()
        return (end - start).days

    def get_initiation_dates(self):
        """Returns a map of bill_id -> date_initiated for cached bills."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT bill_id, date_initiated FROM bills")
        rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows if row[0]}

    def get_all_active_bills(self):
        """Retrieves all bills that are currently in an 'Active' phase."""
        cursor = self.conn.cursor()
        # We want to check debates for any bill that isn't finalized (Enacted/Lapsed)
        cursor.execute('''
            SELECT bill_id, uri, title, status 
            FROM bills 
            WHERE lifecycle_phase = 'Active'
        ''')
    
        # Converting the list of tuples into a list of dictionaries for easier use
        rows = cursor.fetchall()
        return [
            {
                'bill_id': row[0],
                'uri': row[1],
                'title': row[2],
                'status': row[3]
            } for row in rows
        ]

    def is_debate_summarized(self, debate_id):
        """True if the debate_id has already been summarized."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM debates WHERE debate_id=? AND is_summarized=1", (debate_id,))
        return cursor.fetchone() is not None

    def get_summarized_debate_ids(self):
        """Returns a set of summarized debate_ids for fast filtering."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT debate_id FROM debates WHERE is_summarized=1")
        return {row[0] for row in cursor.fetchall() if row and row[0]}

    def update_debate_summary(self, debate_id, bill_id, summary, debate_date=None, chamber=None):
        """Upserts a summarized debate record."""
        cursor = self.conn.cursor()

        # Basic heuristic QC flags based on summary text only (no transcript stored)
        import json as _json
        text = summary or ""
        paragraphs = [p for p in text.split("\n\n") if p.strip()]
        has_first_person = any(
            token in text.lower()
            for token in [" i ", " we ", " our ", "my view", "i think"]
        )
        flags = {
            "length_chars": len(text),
            "paragraphs": len(paragraphs),
            "too_short": len(text) < 400,
            "too_long": len(text) > 4000,
            "has_first_person": has_first_person,
        }
        qc_status = "unreviewed"
        qc_flags_json = _json.dumps(flags)

        cursor.execute(
            """
            INSERT INTO debates (debate_id, bill_id, debate_date, transcript_content, is_summarized, chamber, qc_status, qc_flags)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(debate_id) DO UPDATE SET
                transcript_content = excluded.transcript_content,
                is_summarized = 1,
                bill_id = COALESCE(excluded.bill_id, debates.bill_id),
                debate_date = COALESCE(excluded.debate_date, debates.debate_date),
                chamber = COALESCE(excluded.chamber, debates.chamber),
                qc_status = COALESCE(excluded.qc_status, debates.qc_status),
                qc_flags = COALESCE(excluded.qc_flags, debates.qc_flags)
            """,
            (debate_id, bill_id, debate_date, summary, chamber, qc_status, qc_flags_json),
        )
        self.conn.commit()

        # region agent log
        try:
            import json as _json
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "D2",
                    "location": "persistence.py:update_debate_summary",
                    "message": "debate_summary_upserted",
                    "data": {
                        "debate_id": debate_id,
                        "bill_id": bill_id,
                        "debate_date": debate_date,
                        "chamber": chamber
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion

    def get_weekly_changes(self, chamber=None, days=7):
        """Fetches bills that were updated or had new debates in the last 7 days."""
        cursor = self.conn.cursor()
        # One summary per bill + chamber: pick the most recently inserted summarized debate
        base_query = '''
            SELECT b.bill_id, b.title, b.status, d.transcript_content as narrative_summary, d.chamber,
                   b.date_initiated, b.date_enacted, d.debate_date, b.stage, b.sponsor
            FROM bills b
            JOIN debates d ON b.bill_id = d.bill_id
            WHERE d.is_summarized = 1
              AND date(d.debate_date) >= date('now', ?)
              AND d.rowid IN (
                SELECT MAX(d2.rowid)
                FROM debates d2
                WHERE d2.is_summarized = 1
                GROUP BY d2.bill_id, d2.chamber
              )
        '''
        days_clause = f"-{int(days)} days"
        if chamber:
            cursor.execute(base_query + " AND d.chamber = ?", (days_clause, chamber))
        else:
            cursor.execute(base_query, (days_clause,))
        rows = cursor.fetchall()

        # region agent log
        try:
            import json as _json
            from datetime import datetime as _dt
            debate_dates = [r[7] for r in rows if r[7]]
            def _parse(d):
                try:
                    return _dt.strptime(str(d)[:10], "%Y-%m-%d")
                except Exception:
                    return None
            parsed = [p for p in (_parse(d) for d in debate_dates) if p]
            min_date = min(parsed).strftime("%Y-%m-%d") if parsed else None
            max_date = max(parsed).strftime("%Y-%m-%d") if parsed else None
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "D1",
                    "location": "persistence.py:get_weekly_changes",
                    "message": "weekly_changes_result",
                    "data": {
                        "row_count": len(rows),
                        "chamber": chamber,
                        "days": days,
                        "min_debate_date": min_date,
                        "max_debate_date": max_date
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion

        return [
            {
                'bill_id': row[0],
                'title': row[1],
                'status': row[2],
                'narrative_summary': row[3],
                'chamber': row[4],
                'date_initiated': row[5],
                'date_enacted': row[6],
                'debate_date': row[7],
                'stage': row[8],
                'sponsor': row[9]
            } for row in rows
        ]

    def repair_chambers_from_uri(self):
        """One-pass repair of debate chambers based on debate_id URI."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT debate_id, chamber FROM debates")
        rows = cursor.fetchall()
        updated = 0
        for debate_id, chamber in rows:
            uri = (debate_id or "").lower()
            new_chamber = None
            if "/dail/" in uri:
                new_chamber = "Dáil"
            elif "/seanad/" in uri:
                new_chamber = "Seanad"
            elif "committee" in uri:
                new_chamber = "Committee"
            if new_chamber and new_chamber != chamber:
                cursor.execute(
                    "UPDATE debates SET chamber=? WHERE debate_id=?",
                    (new_chamber, debate_id),
                )
                updated += 1
        self.conn.commit()

        # region agent log
        try:
            import json as _json
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({
                    "sessionId": "9c338e",
                    "runId": "post-fix",
                    "hypothesisId": "F1",
                    "location": "persistence.py:repair_chambers_from_uri",
                    "message": "debate_chambers_repaired",
                    "data": {
                        "rows_scanned": len(rows),
                        "rows_updated": updated,
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion

    # --- QC helpers ---------------------------------------------------------

    def get_summaries_for_qc(self, status="unreviewed"):
        """Return summaries filtered by qc_status with basic metadata."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT debate_id, bill_id, debate_date, chamber, transcript_content, qc_status, qc_flags, qc_notes
            FROM debates
            WHERE is_summarized = 1
              AND (qc_status = ? OR (? = 'all'))
            ORDER BY date(debate_date) DESC
            """,
            (status, status),
        )
        rows = cursor.fetchall()
        return [
            {
                "debate_id": r[0],
                "bill_id": r[1],
                "debate_date": r[2],
                "chamber": r[3],
                "summary": r[4],
                "qc_status": r[5],
                "qc_flags": r[6],
                "qc_notes": r[7],
            }
            for r in rows
        ]

    def set_summary_qc_status(self, debate_id, status, notes=None):
        """Update qc_status and optional notes for a debate summary."""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE debates SET qc_status = ?, qc_notes = ? WHERE debate_id = ?",
            (status, notes, debate_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    # --- Policy linking stubs -----------------------------------------------

    def get_bill_policy_matches(self, bill_id):
        """Stub for future party-policy matching; returns empty list for now."""
        # Placeholder so the API can be wired without implementing matching yet.
        return []

    def get_policies_for_party(self, party):
        """Stub for future party-policy retrieval; returns empty list for now."""
        return []
