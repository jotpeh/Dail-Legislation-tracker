import logging
import json
import csv
import re
import requests
from datetime import datetime, timedelta
from tqdm import tqdm

logger = logging.getLogger(__name__)

class OireachtasHarvester:
    BASE_URL = "https://api.oireachtas.ie/v1"
    
    def __init__(self):
        self.timeout = 15
        self.detail_timeout = 10
        self.session = requests.Session()

    def get_active_bills(self):
        """Fetches all current bills with loop protection and a progress bar."""
        all_active_bills = []
        seen_ids = set()  # The 'Circuit Breaker' memory
        offset = 0
        limit = 50  # Smaller chunks are more stable for pagination logic
        
        # We start a manual progress bar
        pbar = tqdm(desc="📦 Syncing Bills", unit=" bills")
        
        while True:
            # 1. SAFETY CEILING: Realistically, active bills won't exceed 1,000.
            if offset > 1000:
                logger.warning("🚨 Safety ceiling reached. Ending sync to prevent infinite loop.")
                break

            endpoint = f"{self.BASE_URL}/legislation"
            params = {
                'limit': limit,
                'offset': offset,
                'bill_status': 'Current'
            }
            
            try:
                response = self.session.get(endpoint, params=params, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                results = data.get('results', [])
                
                # SIGNAL 1: The API returns an empty list (Natural End)
                if not results:
                    logger.debug("Reached end of API data (empty results).")
                    break

                new_on_this_page = 0
                for item in results:
                    bill_meta = item.get('bill', {})
                    bill_no = str(bill_meta.get('billNo', ''))
                    bill_year = str(bill_meta.get('billYear', ''))
                    bill_key = bill_meta.get('billUri') or bill_meta.get('uri') or f"{bill_year}-{bill_no}"
                    
                    # SIGNAL 2: DUPLICATE DETECTION (The Loop Breaker)
                    # If we've seen this bill number before, we've looped.
                    if bill_key and bill_key in seen_ids:
                        logger.info("🔄 Duplicate bill detected. Ending sync—all data captured.")
                        pbar.close()
                        return all_active_bills
                    
                    if bill_key:
                        seen_ids.add(bill_key)
                    
                    # Filter for bills active in either house
                    status = bill_meta.get('status', 'Unknown')
                    if any(s in status for s in ["Before Dáil", "Before Seanad", "Current"]):
                        all_active_bills.append(self._format_bill_data(bill_meta))
                    
                    new_on_this_page += 1
                
                pbar.update(new_on_this_page)
                
                # SIGNAL 3: Partial Page (End of Data)
                if len(results) < limit:
                    logger.debug("Final partial page reached.")
                    break
                    
                offset += limit
                
            except Exception as e:
                logger.error(f"❌ Pagination error: {e}")
                break
        
        pbar.close()
        # region agent log
        try:
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "B1",
                    "location": "harvester.py:get_active_bills",
                    "message": "get_active_bills_result",
                    "data": {
                        "bill_count": len(all_active_bills),
                        "seen_ids": len(seen_ids)
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion
        logger.info(f"✅ Harvested {len(all_active_bills)} unique active bills.")
        return all_active_bills

    def _format_bill_data(self, bill):
        """Standardizes metadata for the local database."""
        uri = bill.get('billUri') or bill.get('uri') or "unknown_uri"
        # Keep original initiation date if provided; avoid defaulting to today (which skews age calculations)
        init_date = bill.get('dateInitiated')
        stage = self._extract_stage(bill)
        sponsor = self._extract_sponsor(bill)
        
        return {
            'bill_id': str(bill.get('billNo', '000')),
            'bill_year': bill.get('billYear'),
            'uri': uri,
            'title': bill.get('shortTitleEn', 'Untitled Bill'),
            'status': bill.get('status', 'Active'),
            'lifecycle_phase': "Active",
            'date_initiated': init_date,
            'date_enacted': bill.get('enactmentDate'),
            'last_updated': bill.get('lastStageUpdated', init_date),
            'stage': stage,
            'sponsor': sponsor
        }

    def get_weekly_debates(self, bill_uri):
        """Fetches all debate segments for a specific bill (30-day window)."""
        # Using a 30-day window is wise for catching late-published transcripts
        lookback = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        endpoint = f"{self.BASE_URL}/debates"

        attempts = []
        bill_no = bill_uri.rsplit('/', 1)[-1] if '/' in bill_uri else bill_uri

        # Try multiple known parameter keys; API docs are spotty and sometimes ignore bill_uri
        attempts.append({'bill_uri': bill_uri, 'date_start': lookback, 'limit': 100})
        attempts.append({'bill': bill_uri, 'date_start': lookback, 'limit': 100})
        attempts.append({'bill': bill_no, 'date_start': lookback, 'limit': 100})

        for idx, params in enumerate(attempts, start=1):
            try:
                response = self.session.get(endpoint, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                raw_results = data.get('results', [])
                logger.debug(f"Debate fetch attempt {idx} returned {len(raw_results)} raw results using params {params}")

                # Filter to debateRecords that actually reference a bill
                records = []
                for i in raw_results:
                    dr = i.get('debateRecord')
                    if not dr:
                        continue
                    if self._has_bill_reference(dr):
                        records.append(dr)

                # If we got meaningful results, stop retrying
                if records:
                    logger.debug(f"Debate fetch attempt {idx} succeeded with {len(records)} bill-linked records using params {params}")
                    return records
                else:
                    # For diagnostics: if the API returned data but none were bill-linked, dump a few for inspection
                    if raw_results:
                        self._dump_filtered_debates(raw_results, params)
                    else:
                        logger.debug(f"Debate fetch attempt {idx} had zero results (params {params})")

            except Exception as e:
                logger.warning(f"⚠️ Debate API error (attempt {idx}) for {bill_uri}: {e}")

        logger.debug(f"No bill-linked debates found for {bill_uri} since {lookback} across {len(attempts)} param variants")
        return []

    def get_recent_debates(self, lookback_days=30, limit=200):
        """Fetch debates once for the lookback window (batch mode)."""
        lookback = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        endpoint = f"{self.BASE_URL}/debates"
        all_records = []
        offset = 0

        while True:
            if offset > 5000:
                logger.warning("🚨 Debate pagination ceiling reached; stopping.")
                break
            params = {"date_start": lookback, "limit": limit, "offset": offset}
            try:
                response = self.session.get(endpoint, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                if not results:
                    break
                for item in results:
                    dr = item.get("debateRecord")
                    if dr:
                        all_records.append(dr)
                if len(results) < limit:
                    break
                offset += limit
            except Exception as e:
                logger.warning(f"⚠️ Debate batch fetch error: {e}")
                break

        logger.debug(f"Batch debate fetch returned {len(all_records)} records since {lookback}")
        # region agent log
        try:
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "B2",
                    "location": "harvester.py:get_recent_debates",
                    "message": "get_recent_debates_result",
                    "data": {
                        "debate_record_count": len(all_records),
                        "lookback_days": lookback_days,
                        "limit": limit
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion
        return all_records

    def _extract_stage(self, bill):
        """Extract a readable stage string from mostRecentStage if available."""
        stage_obj = bill.get("mostRecentStage") or {}
        event = stage_obj.get("event") or {}
        stage = event.get("showAs")
        house = (event.get("house") or {}).get("showAs") or (event.get("chamber") or {}).get("showAs")
        if stage and house:
            return f"{stage} ({house})"
        return stage or None

    def _extract_sponsor(self, bill):
        """Extract primary sponsor name if available, otherwise the first sponsor."""
        sponsors = bill.get("sponsors") or []
        primary = None
        names = []
        for s in sponsors:
            sp = s.get("sponsor") if isinstance(s, dict) else None
            if not sp:
                continue
            by = sp.get("by") or {}
            name = by.get("showAs") or by.get("name")
            if name:
                names.append(name)
                if sp.get("isPrimary"):
                    primary = name
        if primary:
            return primary
        return names[0] if names else None

    # --- Initiation date helpers -------------------------------------------------
    def get_bill_initiation_date(self, bill):
        """Derive the earliest initiation date for a bill.

        Sources consulted (earliest wins):
        1) Exact legislation record for (bill_year, bill_no) including events/debates.
        2) Earliest debate date linked to the bill (fallback proxy).
        """
        earliest_leg = self._get_earliest_legislation_date(bill)
        if isinstance(earliest_leg, datetime):
            return earliest_leg.strftime("%Y-%m-%d")
        if isinstance(earliest_leg, str):
            return earliest_leg[:10]
        # Optional fallback: debates (only if we truly cannot derive from legislation)
        earliest_debate = self._get_earliest_debate_date(bill)
        if isinstance(earliest_debate, datetime):
            return earliest_debate.strftime("%Y-%m-%d")
        if isinstance(earliest_debate, str):
            return earliest_debate[:10]
        return None

    def backfill_initiation_dates(self, bills, known_dates=None):
        """Populate initiation dates; replace when an authoritative date is available.

        Strategy:
        1) Use full legislation detail (stages + published dates).
        2) Cross-check debates API (oldest debate linked to bill) as a fallback proxy for first proposal.
        """
        updated = 0
        for bill in bills:
            current = bill.get("date_initiated")
            bill_id = bill.get("bill_id")

            # If already known locally, avoid a per-bill API lookup
            if not current and known_dates and bill_id in known_dates:
                bill["date_initiated"] = known_dates[bill_id]
                continue
            if current:
                continue

            init = self.get_bill_initiation_date(bill)
            if not init:
                continue

            # Use authoritative initiation date whenever we can derive it
            if init and init != current:
                bill["date_initiated"] = init
                updated += 1

        if updated:
            logger.info(f"🗂️  Updated initiation dates for {updated} bills")
        else:
            logger.info("ℹ️  No initiation date changes after backfill")
        return bills, updated

    def _fetch_legislation_detail(self, bill_year, bill_no):
        """Fetch the exact legislation record for a bill year/number."""
        endpoint = f"{self.BASE_URL}/legislation"
        params = {
            "bill_no": bill_no,
            "bill_year": bill_year,
            "limit": 1,
            "include": "debates,events"
        }
        try:
            resp = self.session.get(endpoint, params=params, timeout=self.detail_timeout)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                return []
            return [results[0].get("bill") or {}]
        except Exception as e:
            logger.debug(f"Legislation detail fetch failed for {bill_year}/{bill_no}: {e}")
            return []

    def _get_earliest_legislation_date(self, bill):
        """Return earliest date from the exact legislation record for this bill."""
        bill_year, bill_no = self._extract_bill_year_no(bill)
        if not bill_year or not bill_no:
            return None
        records = self._fetch_legislation_detail(bill_year, bill_no)
        if not records:
            return None
        dates = []
        for meta in records:
            d = self._extract_earliest_date(meta)
            if d:
                try:
                    dates.append(datetime.strptime(d, "%Y-%m-%d"))
                except Exception:
                    continue
        if not dates:
            return None
        return min(dates)

    def _extract_bill_year_no(self, bill):
        uri = bill.get("uri") or ""
        m = re.search(r"/bill/(\d{4})/(\d+)", uri)
        if m:
            return int(m.group(1)), int(m.group(2))
        # Fallback if uri missing
        by = bill.get("bill_year") or bill.get("billYear")
        bn = bill.get("bill_id") or bill.get("billNo")
        try:
            return int(by), int(bn)
        except Exception:
            return None, None

    def _get_earliest_debate_date(self, bill):
        """Fallback: scan debates for the earliest linked date (proxy for first proposal)."""
        bill_uri = bill.get("uri")
        endpoint = f"{self.BASE_URL}/debates"
        params_list = [
            {"bill_uri": bill_uri, "date_start": "1900-01-01", "limit": 200},
            {"bill": bill_uri, "date_start": "1900-01-01", "limit": 200},
        ]

        dates = []
        for params in params_list:
            offset = 0
            try:
                while True:
                    paged = dict(params, offset=offset)
                    resp = self.session.get(endpoint, params=paged, timeout=self.timeout)
                    resp.raise_for_status()
                    data = resp.json()
                    results = data.get("results", [])
                    if not results:
                        break

                    for r in results:
                        dr = r.get("debateRecord") or {}
                        d = dr.get("context", {}).get("date") or dr.get("date")
                        if d:
                            try:
                                dates.append(datetime.strptime(d[:10], "%Y-%m-%d"))
                            except Exception:
                                continue

                    if len(results) < paged.get("limit", 200):
                        break
                    offset += paged.get("limit", 200)
                    if offset > 2000:
                        break
                if dates:
                    break
            except Exception as e:
                logger.debug(f"Debate date scan failed params {params}: {e}")
                continue

        if not dates:
            return None
        return min(dates).strftime("%Y-%m-%d")

    def _extract_earliest_date(self, bill_meta):
        """Find the earliest plausible initiation date from a bill payload.

        Priority order:
        1) Events: earliest available event date (often "Published"/"Admissibility for Introduction")
        2) Direct initiation/presentation fields
        3) Explicit first-stage entries in stages array
        """

        def parse_date(val):
            try:
                return datetime.strptime(str(val)[:10], "%Y-%m-%d")
            except Exception:
                return None

        candidates = []

        # 1) Events (often the best signal for first proposal)
        events = bill_meta.get("events") or []
        for ewrap in events:
            ev = ewrap.get("event") if isinstance(ewrap, dict) else None
            if not ev:
                continue
            for d in ev.get("dates", []):
                dt = parse_date(d.get("date") if isinstance(d, dict) else d)
                if dt:
                    candidates.append(dt)

        # 2) Direct fields (ordered by desirability)
        ordered_keys = [
            "dateInitiated", "datePresented", "dateIntroduced", "introDate",
            "dateFirstStage", "firstStageDate"
        ]
        for k in ordered_keys:
            dt = parse_date(bill_meta.get(k))
            if dt:
                candidates.append(dt)

        # 3) Debates array (often holds First Stage with date)
        debates = bill_meta.get("debates") or []
        for d in debates:
            name = (d.get("showAs") or "").lower()
            if "first stage" in name:
                dt = parse_date(d.get("date"))
                if dt:
                    candidates.append(dt)

        # 4) Stages array: explicit first stage markers
        for stages_key in ("billStages", "stages", "showStages"):
            stages = bill_meta.get(stages_key) or []
            for s in stages:
                stage_obj = s.get("billStage") if isinstance(s, dict) else None
                data = stage_obj if stage_obj else s if isinstance(s, dict) else None
                if not data:
                    continue
                name = (data.get("shortNameEn") or data.get("stage") or data.get("stageName") or "").lower()
                number = data.get("stageNumber") or data.get("number")
                if ("first stage" in name) or (number == 1):
                    dt = parse_date(data.get("stageDate") or data.get("date"))
                    if dt:
                        candidates.append(dt)

        if not candidates:
            return None
        return min(candidates).strftime("%Y-%m-%d")

    def export_initiation_dataset(self, bills, path="active_bills_initiation.csv"):
        """Export a simple CSV of active bills and their initiation dates."""
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["bill_id", "title", "uri", "status", "date_initiated"])
                for b in bills:
                    writer.writerow([
                        b.get("bill_id"),
                        b.get("title"),
                        b.get("uri"),
                        b.get("status"),
                        b.get("date_initiated")
                    ])
            logger.info(f"📤 Exported initiation dataset to {path}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to write initiation dataset: {e}")

    def _has_bill_reference(self, dr):
        """True if the debate record explicitly links to a bill."""
        # direct bills list
        bills = dr.get('bills') or []
        if any(isinstance(b, dict) and (b.get('billRef') or b.get('billUri') or b.get('uri')) for b in bills):
            return True

        # counts metadata
        if dr.get('counts', {}).get('billCount', 0) > 0:
            return True

        # sections may carry a 'bill' object
        sections = dr.get('debateSections', []) or dr.get('showDebateSections', [])
        for sec in sections:
            block = sec.get('debateSection') if isinstance(sec, dict) else None
            block = block or sec
            if isinstance(block, dict):
                bill_obj = block.get('bill')
                if isinstance(bill_obj, dict) and (bill_obj.get('billRef') or bill_obj.get('billUri') or bill_obj.get('uri')):
                    return True

        return False

    def _dump_filtered_debates(self, raw_results, params):
        """When debates were returned but none linked to bills, persist a small sample for debugging."""
        try:
            sample = []
            for i in raw_results[:3]:
                dr = i.get('debateRecord') or {}
                sample.append({
                    "params": params,
                    "counts": dr.get("counts", {}),
                    "bills": dr.get("bills", []),
                    "sections_len": len(dr.get("debateSections", []) or dr.get("showDebateSections", []) or []),
                    "uri": dr.get("uri"),
                    "title": dr.get("title") or dr.get("sectionName") or dr.get("context", {}).get("heading")
                })
            path = "filtered_debates.jsonl"
            logger.info(f"ℹ️ Writing {len(sample)} non-bill debates to {path} (params {params})")
            with open(path, "a", encoding="utf-8") as f:
                for entry in sample:
                    json.dump(entry, f)
                    f.write("\n")
        except Exception as e:
            logger.debug(f"DEBUG: Failed to dump filtered debates: {e}")
