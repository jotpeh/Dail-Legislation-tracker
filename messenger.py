from datetime import datetime

class LegislativeMessenger:
    def __init__(self, db_manager):
        self.db = db_manager

    def generate_weekly_report(self, bill_ids=None, chamber=None, days=7):
        """Builds a Markdown report for weekly activity, optionally filtered by chamber."""
        report_date = datetime.now().strftime('%Y-%m-%d')
        title = "Weekly Legislative Update"
        if chamber:
            title = f"Weekly {chamber} Legislative Update"
        report = f"# {title}: {report_date}\n\n"

        # Fetch recent changes; if none are found within the requested window,
        # fall back to a slightly longer window so that infrequent sittings
        # still produce a meaningful report.
        effective_days = days
        updates = self.db.get_weekly_changes(chamber=chamber, days=days)
        if not updates and days < 30:
            fallback_days = 30
            updates = self.db.get_weekly_changes(chamber=chamber, days=fallback_days)
            effective_days = fallback_days

        if bill_ids:
            wanted = set(bill_ids)
            updates = [u for u in updates if u.get("bill_id") in wanted]
        
        if not updates:
            return report + "No major legislative activity detected this week."

        report += f"_Reporting window: last {effective_days} days._\n\n"

        for item in updates:
            duration = self._compute_duration(item.get("date_initiated"), item.get("date_enacted"))
            report += f"## {item['title']}\n"
            report += f"**Status:** {item['status']} | **Timeline:** {duration} days in progress\n\n"
            stage = item.get("stage") or "Unknown"
            sponsor = item.get("sponsor") or "Unknown"
            report += f"**Stage:** {stage} | **Sponsor:** {sponsor}\n\n"
            
            # The Narrative Summary from Module 3
            report += "### Summary of Debates\n"
            report += f"> {item['narrative_summary']}\n\n"
            
            report += "---\n"
            
        return report

    def _compute_duration(self, start_date, end_date):
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
        except Exception:
            return "Unknown"
        if end_date:
            try:
                end = datetime.strptime(end_date, '%Y-%m-%d')
            except Exception:
                end = datetime.now()
        else:
            end = datetime.now()
        return (end - start).days

    def save_report(self, content, chamber=None):
        suffix = chamber.lower() if chamber else "all"
        filename = f"report_{suffix}_{datetime.now().strftime('%Y_%W')}.md"
        with open(filename, "w") as f:
            f.write(content)
        print(f"✅ Weekly report generated: {filename}")
