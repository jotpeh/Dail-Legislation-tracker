import os
import logging
import json
import ollama

logger = logging.getLogger(__name__)

class LocalLegislativeEditor:
    def __init__(self, model_name=None):
        # Allow override via env; default to the requested local model tag
        self.model = model_name or os.environ.get("OIREACHTAS_MODEL", "llama3.1:latest")

    def summarize_debate(self, bill_title, transcript_text):
        """Generates a narrative using a local model."""
        chamber = self._infer_chamber(transcript_text)
        system_instructions = (
            "Act as a senior political correspondent for a national Irish newspaper. "
            "Your task is to write a concise, objective summary of the following parliamentary debate "
            f"from the {chamber}.\n\n"
            "Constraints:\n\n"
            "Structure: Use the \"Inverted Pyramid\" format. Start with the most significant development "
            "or the main decision, followed by the supporting arguments and context.\n\n"
            "Format: Use 3–4 cohesive paragraphs. Do not use bullet points, lists, or bolded headers.\n\n"
            "Voice & Tone: Write in the third person. Maintain a neutral, professional, and journalistic tone. "
            "Avoid \"first-person\" language (do not say \"I think\" or \"This summary shows\").\n\n"
            "Content: Identify the bill being discussed, the lead Minister/proposer, and the primary concerns "
            "raised by the Opposition. Use phrases like \"The Minister argued,\" \"Concerns were raised regarding,\" "
            "or \"The House heard that.\""
        )

        prompt = f"Debate Transcript:\n\n{transcript_text}"

        try:
            # region agent log
            try:
                with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "9c338e",
                        "runId": "initial",
                        "hypothesisId": "A1",
                        "location": "editor_local.py:summarize_debate",
                        "message": "summarize_debate_start",
                        "data": {
                            "model": self.model,
                            "bill_title_len": len(bill_title or ""),
                            "transcript_len": len(transcript_text or "")
                        },
                        "timestamp": int(__import__("time").time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # endregion

            result = self._generate_with_model(self.model, system_instructions, prompt)

            # region agent log
            try:
                with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "9c338e",
                        "runId": "initial",
                        "hypothesisId": "A2",
                        "location": "editor_local.py:summarize_debate",
                        "message": "summarize_debate_success",
                        "data": {
                            "summary_len": len(result or "")
                        },
                        "timestamp": int(__import__("time").time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # endregion

            return result
        except ollama._types.ResponseError as e:
            msg = str(e).lower()
            if "not found" in msg or "unknown model" in msg:
                logger.warning(f"Model '{self.model}' not found locally. Pulling it now...")
                self._pull_model(self.model)
                return self._generate_with_model(self.model, system_instructions, prompt)
            # Other errors bubble up
            # region agent log
            try:
                with open("/Users/joeran/Downloads/Dail-Legislation-tracker-main/.cursor/debug-9c338e.log", "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "9c338e",
                        "runId": "initial",
                        "hypothesisId": "A3",
                        "location": "editor_local.py:summarize_debate",
                        "message": "summarize_debate_ollama_error",
                        "data": {
                            "error": str(e)
                        },
                        "timestamp": int(__import__("time").time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # endregion
            raise

    def _generate_with_model(self, model, system, prompt):
        response = ollama.generate(
            model=model,
            system=system,
            prompt=prompt,
            options={
                "num_ctx": 8192,
                "temperature": 0.3
            }
        )
        return response["response"]

    def _pull_model(self, model):
        try:
            ollama.pull(model)
            logger.info(f"Pulled model '{model}' successfully.")
        except Exception as e:
            logger.error(f"Failed to pull model '{model}': {e}")
            raise

    def _infer_chamber(self, transcript_text):
        """Infer chamber from transcript text: Dáil, Seanad, or Committee."""
        if not transcript_text:
            return "Dáil"
        t = transcript_text.lower()
        if "seanad" in t or "senator" in t or "cathaoirleach" in t:
            return "Seanad"
        if "committee" in t or "select committee" in t or "joint committee" in t or "committee on" in t:
            return "Committee"
        if "dáil" in t or "dail" in t or "teachta" in t or "td" in t or "ceann comhairle" in t:
            return "Dáil"
        return "Dáil"
