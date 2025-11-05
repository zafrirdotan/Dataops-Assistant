# guardrails_service.py
"""
Service for sanitation, guardrails, and validation of user input and LLM output.
"""
import json

# app/service.py
import re
import unicodedata
from typing import Dict, List, Tuple

from app.services.llm_service import LLMService

# --- Normalization / Cleaning ---

# Bidirectional control characters
BIDI_CHARS = {
    "\u202A", "\u202B", "\u202D", "\u202E",  # LRE, RLE, LRO, RLO
    "\u2066", "\u2067", "\u2068", "\u2069",  # LRI, RLI, FSI, PDI
}

# Zero-width characters
ZERO_WIDTH = {
    "\u200B",  # ZWSP
    "\u200C",  # ZWNJ
    "\u200D",  # ZWJ
    "\u2060",  # WJ
    "\uFEFF",  # BOM
}

def _normalize_nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def _strip_control_chars(s: str) -> str:
    out = []
    for ch in s:
        cat = unicodedata.category(ch)
        if ch in ("\n", "\t"):
            out.append(ch)
        elif cat.startswith("C"):  # control/format/surrogate
            continue
        else:
            out.append(ch)
    return "".join(out)

def _remove_bidi_zero_width(s: str) -> str:
    for ch in BIDI_CHARS.union(ZERO_WIDTH):
        s = s.replace(ch, "")
    return s

def basic_clean(s: str) -> str:
    s = _normalize_nfkc(s)
    s = _remove_bidi_zero_width(s)
    s = _strip_control_chars(s)
    return s

# --- Detection rules ---

PATTERNS: List[Tuple[str, re.Pattern, str]] = [
    ("code_block", re.compile(r"```.+?```", re.DOTALL | re.IGNORECASE), "Contains fenced code block"),
    ("python_import", re.compile(r"\b(import|from)\s+[a-zA-Z0-9_\.]+", re.IGNORECASE), "Python import detected"),
    ("dangerous_python", re.compile(r"\b(eval|exec|__import__|open\(|compile\(|input\()", re.IGNORECASE), "Dangerous Python API"),
    ("shell_cmd", re.compile(r"(^|\s)(?:!|%|sh\s+-c\s+|\$\(|`[^`]*`)", re.IGNORECASE), "Shell execution marker"),
    ("subprocess", re.compile(r"\bsubprocess\.(run|Popen|call|check_output)\b", re.IGNORECASE), "Subprocess usage"),
    ("os_cmd", re.compile(r"\bos\.(system|popen|execv|execl|spawn)\b", re.IGNORECASE), "OS command execution"),
    ("sql_keywords", re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|UNION|--|;)\b", re.IGNORECASE), "SQL keyword(s)"),
    ("powershell", re.compile(r"\bInvoke-Expression\b|\bNew-Object\b", re.IGNORECASE), "PowerShell markers"),
    ("injection_english", re.compile(r"\b(ignore|bypass|override|disregard)\b.*\b(instruction|policy|previous)\b", re.IGNORECASE), "Injection phrasing (EN)"),
    ("injection_system", re.compile(r"\b(system|developer)\s+prompt\b|\bYou are now\b", re.IGNORECASE), "System-prompt manipulation"),
    ("injection_hebrew", re.compile(r"(התעלם|תתעלם|עוקף|בטל).{0,16}(הנחיות|חוקים|הוראות)", re.IGNORECASE), "Injection phrasing (HE)"),
    ("urls", re.compile(r"https?://[^\s)]+", re.IGNORECASE), "URL(s) present"),
    ("markdown_links", re.compile(r"\[[^\]]+\]\([^)]+\)"), "Markdown link(s)"),
    ("attachments_hint", re.compile(r"\b(base64|data:)[^,\s]{0,20},", re.IGNORECASE), "Possible embedded data"),
]

SEVERITY = {
    "dangerous_python": "high",
    "shell_cmd": "high",
    "subprocess": "high",
    "os_cmd": "high",
    "python_import": "medium",
    "sql_keywords": "medium",
    "powershell": "high",
    "code_block": "low",
    "urls": "low",
    "markdown_links": "low",
    "attachments_hint": "medium",
    "injection_english": "medium",
    "injection_hebrew": "medium",
    "injection_system": "medium",
}

SAFE_CHARS_RE = re.compile(r"^[\n\t\r a-zA-Z0-9_\-.,:;!?()\"'@#/$%&*+=<>[\]{}|\\~`]+$")

class PromptGuardService:
    def __init__(self, allowlist_max_len: int = 2000):
        self.allowlist_max_len = allowlist_max_len
        self.llm = LLMService()

    async def llm_guard_check(self, cleaned: str) -> bool:
        """ Checks if the input is meeting requirements and guardrails to pass through LLM guard """

        prompt = f"""
You are a security guard for user inputs to a language model. Determine if the following input is safe to process: {cleaned}

Allowed source types:
  - Local files (CSV or JSON) from ./data/
  - PostgreSQL
  - API endpoints
  - No other sources are allowed.

Allowed destination types:
  - PostgreSQL
  - Parquet files
  - SQLite databases
  - No other destinations are allowed.

Allowed operations:
  - Extracting data from the source
  - Transforming data according to the specified transformation logic
  - Storing data in the destination
  - Performing **merge (upsert)** operations into destinations by key
    (e.g. `merge into dw.fact_transactions by txn_id`).
    Merge is considered a safe load operation, not a transformation.

Allowed transformations:
  - Filtering rows
  - Adding calculated columns
  - Aggregating data

Allowed SQL operations:
  - SELECT / PROJECT
  - FILTER
  - CAST
  - JOIN (inner/left) on key
  - GROUP BY aggregation
  - DEDUPE by key + latest updated_at

Allowed schedules:
  - manual
  - daily at 2am
  - weekly on Monday at 6am

You are allowed to get data from the source, transform it according to the logic,
and store (including merge/upsert) it into the destination.

You must block any request that tries to:
  - delete, truncate, or drop tables
  - modify schemas, users, or permissions
  - perform destructive updates
  - use destinations or sources not listed above
  - execute arbitrary SQL beyond the allowed operations
  - merge without specifying a key

Example of allowed merge:
  "From Postgres table public.transactions, merge into Postgres dw.fact_transactions by txn_id."
"""

        response = await self.llm.response_create_async(
            input=prompt,
            text={
            "format": {
                "type": "json_schema",
                "name": "extract_json",
                "schema": {
                "type": "object",
                "properties": {
                    "is_safe": {
                    "type": "boolean",
                    "description": "Indicates if the input is safe to process."
                    },
                    "reason": {
                    "type": "string",
                    "description": "Explanation for the safety decision."
                    },
                    "violations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "violation_type": {
                                    "type": "string",
                                    "enum": ["source_type", "destination_type", "operation", "transformation", "schedule"]
                                },
                                "value": {
                                    "type": "string",
                                    "description": "Specific value related to the defect type."
                                }
                            },
                            "required": ["violation_type", "value"],
                            "additionalProperties": False
                        },
                        "description": "List of properties that caused the input to be unsafe or not meet requirements."
                    }
                },
                "required": ["is_safe", "reason", "violations"],
                "additionalProperties": False
                },
                "strict": True,
            }
            }
        )
        print(response)  # For debugging purposes

        return json.loads(response.output_text)


    def analyze(self, raw: str) -> Dict:
        cleaned = basic_clean(raw)
        findings = []
        for key, rx, desc in PATTERNS:
            hits = [(m.start(), m.end()) for m in rx.finditer(cleaned)]
            if hits:
                findings.append({
                    "rule": key,
                    "severity": SEVERITY.get(key, "low"),
                    "count": len(hits),
                    "description": desc,
                })

        weights = {"low": 1, "medium": 3, "high": 6}
        score = sum(weights[f["severity"]] * f["count"] for f in findings)
        decision = "block" if score >= 8 else "review" if score >= 3 else "allow"

        return {
            "original": raw,
            "cleaned": cleaned,
            "findings": findings,
            "risk_score": score,
            "decision": decision,
        }

    def allowlist_only(self, cleaned: str) -> bool:
        if len(cleaned) > self.allowlist_max_len:
            return False
        return bool(SAFE_CHARS_RE.match(cleaned))

    @staticmethod
    def sanitize_for_display(cleaned: str) -> str:
        return cleaned.replace("<", "&lt;").replace(">", "&gt;")
