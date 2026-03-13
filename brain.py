import logging
import re
import json

logger = logging.getLogger(__name__)

class LegislativeBrain:
    def __init__(self, db_manager):
        self.db = db_manager
        self.dumped_unknown = 0

    def find_relevant_content(self, harvester):
        """Identifies primary agenda debates using an aggressive deep-search."""
        active_bills = self.db.get_all_active_bills()
        logger.debug(f"Brain cross-referencing {len(active_bills)} bills.")
        # Precompute normalized titles once per run
        for b in active_bills:
            b["_norm_title"] = self._normalize_title(b.get("title"))

        # Batch fetch debates once (30-day window)
        debates = harvester.get_recent_debates()
        # region agent log
        try:
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "C1",
                    "location": "brain.py:find_relevant_content",
                    "message": "brain_inputs",
                    "data": {
                        "active_bill_count": len(active_bills),
                        "debate_record_count": len(debates)
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion
        pending_summaries = []
        bill_by_uri = {self._normalize_uri(b['uri']): b for b in active_bills if b.get('uri')}
        seen = set()

        for item in debates:
            section_name, bill_refs = self._extract_metadata_robustly(item)
            transcript = self._extract_full_text(item)
            debate_uri = item.get("uri")
            chamber = self._infer_chamber(section_name, transcript, debate_uri)

            logger.debug(f"DEBUG: Found {len(transcript)} chars of text for {section_name}")

            matched = False
            # Stable debate key for de-duplication
            debate_uri = item.get("uri")
            debate_date = item.get("date") or (item.get("context") or {}).get("date")
            section_key = self._normalize_text(section_name)
            base_key = debate_uri or f"{debate_date}-{section_key}"

            # Prefer explicit bill references
            if bill_refs:
                # De-duplicate refs
                for ref in set(self._normalize_uri(r) for r in bill_refs if r):
                    bill = bill_by_uri.get(ref)
                    if not bill:
                        continue
                    if self._is_primary_agenda(bill, section_name, bill_refs, transcript):
                        debate_id = f"{base_key}-{bill['bill_id']}"
                        key = (debate_id, bill['bill_id'])
                        if key not in seen:
                            seen.add(key)
                            logger.info(f"🎯 Match Found: '{bill['title']}' in '{section_name}'")
                            pending_summaries.append({
                                'bill_id': bill['bill_id'],
                                'title': bill['title'],
                                'debate_id': debate_id,
                                'date': debate_date or item.get('date'),
                                'chamber': chamber,
                                'transcript': transcript
                            })
                        matched = True

            # Fallback: title-based match if no explicit refs
            if not matched:
                for bill in active_bills:
                    if self._is_primary_agenda(bill, section_name, bill_refs, transcript):
                        debate_id = f"{base_key}-{bill['bill_id']}"
                        key = (debate_id, bill['bill_id'])
                        if key not in seen:
                            seen.add(key)
                            logger.info(f"🎯 Match Found: '{bill['title']}' in '{section_name}'")
                            pending_summaries.append({
                                'bill_id': bill['bill_id'],
                                'title': bill['title'],
                                'debate_id': debate_id,
                                'date': debate_date or item.get('date'),
                                'chamber': chamber,
                                'transcript': transcript
                            })

        # region agent log
        try:
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "9c338e",
                    "runId": "initial",
                    "hypothesisId": "C2",
                    "location": "brain.py:find_relevant_content",
                    "message": "brain_output",
                    "data": {
                        "pending_summary_count": len(pending_summaries)
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion
        return pending_summaries

    def _extract_metadata_robustly(self, item):
        """Aggressive search for the debate title + bill links in nested 2026 JSON."""
        # Priority list of keys for the title
        section_name = (
            item.get('debateSection', {}).get('sectionName') or
            item.get('context', {}).get('heading') or
            item.get('sectionName') or
            item.get('title')
        )

        # Prep bill refs accumulator
        bill_refs = []

        # If the record is a list of segments, check them
        sections = item.get('debateSections', []) or item.get('showDebateSections', [])
        for sec in sections:
            block = sec.get('debateSection') if isinstance(sec, dict) else None
            block = block or sec
            if not section_name:
                section_name = (
                    block.get('sectionName') or
                    block.get('showAs') or
                    block.get('heading') or
                    block.get('title')
                )
            # Bill info can live inside each section
            bill_obj = block.get('bill') if isinstance(block, dict) else None
            if bill_obj and isinstance(bill_obj, dict):
                ref = bill_obj.get('billRef') or bill_obj.get('billUri') or bill_obj.get('uri')
                if ref:
                    bill_refs.append(ref)

        # Extract linked bills if present (API commonly provides a 'bills' list)
        for b in item.get('bills', []) or []:
            if isinstance(b, dict):
                ref = b.get('billRef') or b.get('billUri') or b.get('uri')
                if ref:
                    bill_refs.append(ref)

        # Deep fallback: crawl nested JSON for section name and bill refs
        section_candidates = []
        if not section_name or not bill_refs:
            self._find_section_candidates(item, section_candidates)
            if not section_name and section_candidates:
                section_name = self._pick_best_section_name(section_candidates)

            if not bill_refs:
                self._find_bill_refs(item, bill_refs)

        if not section_name:
            section_name = "Unknown Topic"

        if section_name == "Unknown Topic" or not bill_refs:
            self._maybe_dump_unknown(item, section_name, section_candidates, bill_refs)

        return section_name, bill_refs

    def _is_primary_agenda(self, bill, section_name, bill_refs, transcript):
        """Standardizes matching using a 3-layer safety net."""
        # Layer 1: Direct API Link
        if bill['uri'] and bill_refs:
            target = self._normalize_uri(bill['uri'])
            for ref in bill_refs:
                if target == self._normalize_uri(ref):
                    return True
        
        # Layer 2: Fuzzy Title in Section Heading
        clean_title = bill.get("_norm_title") or self._normalize_title(bill['title'])
        if clean_title and clean_title in self._normalize_text(section_name):
            return True
            
        # Layer 3: Contextual Lead-Text Match (Crucial for 2026 'Unknown Topic' issues)
        # If the bill name appears in the first 2000 characters, it's the main topic.
        lead = self._normalize_text(transcript[:2000])
        if clean_title and clean_title in lead:
            return True

        return False

    def _normalize_text(self, text):
        if not text:
            return ""
        # Lowercase and remove punctuation for stable substring matching
        cleaned = re.sub(r"[^a-z0-9\s]", " ", text.lower())
        return re.sub(r"\s+", " ", cleaned).strip()

    def _normalize_title(self, title):
        if not title:
            return ""
        cleaned = self._normalize_text(title)
        # Soften common boilerplate without fully deleting signal words
        cleaned = cleaned.replace(" the ", " ")
        cleaned = cleaned.replace(" bill ", " ")
        cleaned = cleaned.replace(" 2026 ", " ")
        return re.sub(r"\s+", " ", cleaned).strip()

    def _infer_chamber(self, section_name, transcript, uri=None):
        """Infer chamber using URI first, then section metadata and transcript text."""
        text = f"{section_name or ''} {transcript or ''}".lower()
        normalized_uri = (uri or "").lower()

        # URI-based override (2026 API paths are reliable for chamber)
        chamber_from_uri = None
        if "/dail/" in normalized_uri:
            chamber_from_uri = "Dáil"
        elif "/seanad/" in normalized_uri:
            chamber_from_uri = "Seanad"
        elif "committee" in normalized_uri:
            chamber_from_uri = "Committee"

        # Text-based heuristic (fallback / cross-check)
        if "seanad" in text or "senator" in text or "cathaoirleach" in text:
            chamber_from_text = "Seanad"
        elif "committee" in text or "select committee" in text or "joint committee" in text or "committee on" in text:
            chamber_from_text = "Committee"
        elif "dáil" in text or "dail" in text or "teachta" in text or "td" in text or "ceann comhairle" in text:
            chamber_from_text = "Dáil"
        else:
            chamber_from_text = "Dáil"

        final_chamber = chamber_from_uri or chamber_from_text

        # region agent log
        try:
            with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "9c338e",
                    "runId": "post-fix",
                    "hypothesisId": "E1",
                    "location": "brain.py:_infer_chamber",
                    "message": "infer_chamber_decision",
                    "data": {
                        "uri": uri,
                        "chamber_from_uri": chamber_from_uri,
                        "chamber_from_text": chamber_from_text,
                        "final_chamber": final_chamber
                    },
                    "timestamp": int(__import__("time").time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # endregion

        return final_chamber

    def _normalize_uri(self, uri):
        if not uri:
            return ""
        # Strip scheme/host and normalize slashes
        uri = re.sub(r"^https?://[^/]+", "", uri)
        uri = uri.strip()
        if uri.startswith("/"):
            uri = uri[1:]
        return uri

    def _find_section_candidates(self, obj, out):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, str):
                    if k in ("sectionName", "heading", "title", "topic", "showAs") and len(v.strip()) > 3:
                        out.append(v.strip())
                else:
                    self._find_section_candidates(v, out)
        elif isinstance(obj, list):
            for v in obj:
                self._find_section_candidates(v, out)

    def _find_bill_refs(self, obj, out):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, str):
                    if k in ("billRef", "billUri", "billURI", "uri") or "bill" in k.lower():
                        if "/bill/" in v:
                            out.append(v)
                else:
                    self._find_bill_refs(v, out)
        elif isinstance(obj, list):
            for v in obj:
                self._find_bill_refs(v, out)

    def _pick_best_section_name(self, candidates):
        # Prefer longer, descriptive headings
        return sorted(candidates, key=lambda s: (len(s.split()), len(s)), reverse=True)[0]

    def _maybe_dump_unknown(self, item, section_name, section_candidates, bill_refs):
        """Persist a few unknown-topic records for offline inspection."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        if self.dumped_unknown >= 5:
            return
        payload = {
            "section_name": section_name,
            "section_candidates": section_candidates[:5],
            "bill_refs": bill_refs[:5],
            "record": item,
        }
        try:
            with open("unknown_debates.jsonl", "a", encoding="utf-8") as f:
                json.dump(payload, f)
                f.write("\n")
            self.dumped_unknown += 1
        except Exception as e:
            logger.debug(f"DEBUG: Failed to dump unknown debate: {e}")

    def _extract_full_text(self, item):
        """Extracts actual speech while filtering out metadata URLs and tags."""
        text_blocks = []
    
        def find_text_recursively(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str):
                        # FILTER: Ignore URLs and very short procedural strings
                        if value.startswith('http') or len(value) < 30:
                            continue
                    
                        # CLEAN: Remove HTML/XML tags
                        clean_text = re.sub('<[^<]+?>', '', value).strip()
                        if clean_text:
                            text_blocks.append(clean_text)
                    else:
                        find_text_recursively(value)
            elif isinstance(obj, list):
                for element in obj:
                    find_text_recursively(element)

        find_text_recursively(item)
        return " ".join(text_blocks)
