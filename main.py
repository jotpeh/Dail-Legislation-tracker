import sys
import argparse
import logging
import time
import os
from contextlib import contextmanager
from harvester import OireachtasHarvester
from persistence import LegislativeDB
from brain import LegislativeBrain
from editor_local import LocalLegislativeEditor
from messenger import LegislativeMessenger


def setup_logging(debug_mode):
    """Configures how much information we see in the terminal."""
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


@contextmanager
def timed(logger, label, enabled):
    start = time.perf_counter()
    try:
        yield
    finally:
        if enabled:
            elapsed = time.perf_counter() - start
            logger.debug(f"⏱️ {label} took {elapsed:.2f}s")


def main():
    # 1. Parse Command Line Arguments
    parser = argparse.ArgumentParser(description="Oireachtas Legislative Tracker")
    parser.add_argument(
        "--debug", action="store_true", help="Enable detailed debug logging"
    )
    parser.add_argument(
        "--limit-bills",
        type=int,
        default=None,
        help="Limit number of unique bills to summarize (debug)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to include in weekly reports",
    )
    args = parser.parse_args()

    logger = setup_logging(args.debug)
    logger.info("🚀 Starting Weekly Legislative Sync...")

    if args.debug:
        logger.debug("🔧 Debug Mode is ON. Detailed JSON paths will be logged.")

    try:
        # 2. Initialize Components
        db = LegislativeDB()
        harvester = OireachtasHarvester()
        brain = LegislativeBrain(db)
        # Let editor choose the best available local model (config via OIREACHTAS_MODEL env if needed)
        editor = LocalLegislativeEditor()
        messenger = LegislativeMessenger(db)

        # 3. Sync Bills
        logger.info("📥 Fetching latest bill statuses...")
        with timed(logger, "Fetch active bills", args.debug):
            bills = harvester.get_active_bills()
        with timed(logger, "Backfill initiation dates", args.debug):
            known_dates = db.get_initiation_dates()
            bills, init_updates = harvester.backfill_initiation_dates(
                bills, known_dates=known_dates
            )
        with timed(logger, "Persist bills", args.debug):
            for bill in bills:
                if args.debug:
                    logger.debug(
                        f"Processing Bill: {bill['title']} (ID: {bill['bill_id']})"
                    )
                db.save_bill(bill)

        # Export a reusable dataset of active bills and their initiation dates
        with timed(logger, "Export initiation dataset", args.debug):
            dataset_path = "active_bills_initiation.csv"
            if init_updates > 0 or not os.path.exists(dataset_path):
                harvester.export_initiation_dataset(bills, path=dataset_path)

        # 4. Process Debates
        logger.info("🧠 Identifying primary agenda debates...")
        with timed(logger, "Find relevant debates", args.debug):
            new_content = brain.find_relevant_content(harvester)

        if not new_content:
            logger.info("☕ No new relevant activity found.")
        else:
            # Skip already-summarized debates
            summarized_ids = db.get_summarized_debate_ids()
            new_content = [
                e for e in new_content if e.get("debate_id") not in summarized_ids
            ]
            if not new_content:
                logger.info("☕ No new relevant activity found.")
            else:
                # Optional debug limiter: summarize only the first N unique bills
                if args.limit_bills and args.limit_bills > 0:
                    limited = []
                    seen = set()
                    for entry in new_content:
                        bid = entry.get("bill_id")
                        if not bid or bid in seen:
                            continue
                        seen.add(bid)
                        limited.append(entry)
                        if len(limited) >= args.limit_bills:
                            break
                    new_content = limited
                    logger.info(
                        f"🔎 Limiting summaries to {len(new_content)} bills for debug"
                    )

                logger.info(f"✍️  Summarizing {len(new_content)} sessions...")
                with timed(logger, "Summarize debates", args.debug):
                    for entry in new_content:
                        if args.debug:
                            logger.debug(f"Summarizing debate for: {entry['title']}")
                        summary = editor.summarize_debate(
                            entry["title"], entry["transcript"]
                        )
                        db.update_debate_summary(
                            debate_id=entry["debate_id"],
                            bill_id=entry["bill_id"],
                            summary=summary,
                            debate_date=entry.get("date"),
                            chamber=entry.get("chamber"),
                        )

        # 5. Repair historical chamber labels (for legacy runs)
        db.repair_chambers_from_uri()

        # 6. Output
        logger.info("📄 Assembling weekly reports...")
        limit_ids = (
            [e.get("bill_id") for e in new_content]
            if (args.limit_bills and new_content)
            else None
        )
        with timed(logger, "Generate reports", args.debug):
            for chamber in ("Dáil", "Seanad", "Committee"):
                report_content = messenger.generate_weekly_report(
                    bill_ids=limit_ids, chamber=chamber, days=args.days
                )
                messenger.save_report(report_content, chamber=chamber)
        logger.info("✅ Process complete.")

    except Exception as e:
        logger.error(
            f"❌ A critical error occurred: {str(e)}", exc_info=args.debug
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
