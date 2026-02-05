"""
Guards Service - Centralized input validation and security checks.

This service wraps PromptGuardService and provides validation logic
that can be called from any entry point (chat, CLI, API).
"""
import json
import logging
from typing import AsyncGenerator, Dict

from .prompt_guard_service import PromptGuardService


class GuardsService:
    """
    Service for validating user input through pattern matching and LLM-based checks.
    """

    def __init__(self, log=None):
        """
        Initialize the GuardsService.

        Args:
            log: Logger instance. If None, creates a new logger.
        """
        self.log = log or logging.getLogger("dataops")
        self.prompt_guard_service = PromptGuardService(log=self.log)

    async def validate_input(self, raw_message: str) -> Dict:
        """
        Run complete guard validation on user input.

        Performs:
        1. Pattern-based security analysis (SQL injection, code execution, etc.)
        2. Input normalization and cleaning
        3. LLM-based content safety validation

        Args:
            raw_message: The raw user input to validate

        Returns:
            Dictionary with validation results:
            {
                "guard_decision": "allow" | "block",
                "cleaned_input": str,  # Present if allowed
                "error": str,          # Present if blocked
                "findings": list,      # Present if blocked
                "violations": list     # Present if blocked by LLM guard
            }
        """
        # Step 1: Analyze and validate user input with pattern matching
        analysis = self.prompt_guard_service.analyze(raw_message)
        self.log.info(f"Prompt Guard Analysis: {analysis}")

        if analysis["decision"] == "block":
            self.log.warning(f"Input blocked by pattern analysis: {analysis['findings']}")
            return {
                "guard_decision": "block",
                "error": "Input blocked due to security concerns.",
                "findings": analysis["findings"]
            }

        cleaned_input = analysis["cleaned"]

        # Step 2: Perform LLM Guard Check
        try:
            guard_response = await self.prompt_guard_service.llm_guard_check(cleaned_input)
        except Exception as e:
            self.log.error(f"Error during LLM Guard Check: {e}")
            return {
                "guard_decision": "block",
                "error": f"LLM Guard Check failed: {str(e)}"
            }

        self.log.info("LLM Guard Response:\n%s", json.dumps(guard_response, indent=2))

        if not guard_response.get("is_safe", False):
            return {
                "guard_decision": "block",
                "error": f"Input blocked by LLM Guard: {guard_response.get('reason', 'No reason provided')}",
                "violations": guard_response.get("violations", [])
            }

        # Validation passed
        return {
            "guard_decision": "allow",
            "cleaned_input": cleaned_input
        }

    async def validate_with_events(
        self,
        raw_message: str,
        step_name: str = "validate_request",
        step_number: int = 0
    ) -> AsyncGenerator[Dict, None]:
        """
        Validate input and yield streaming events for the validation step.

        This method handles the complete validation lifecycle:
        1. Yields "started" event
        2. Performs validation
        3. Yields error events if blocked
        4. Yields "completed" event if allowed
        5. Yields final validation result

        Args:
            raw_message: The raw user input to validate
            step_name: Name of the validation step (for event tracking)
            step_number: Number of the validation step (for event tracking)

        Yields:
            Event dictionaries with validation progress and results
        """
        # Yield started event
        yield {
            "event": "step",
            "data": {
                "step_name": step_name,
                "step_number": step_number,
                "message": "Validating request...",
                "status": "started"
            }
        }

        # Perform validation
        try:
            guard_result = await self.validate_input(raw_message)
        except Exception as guard_error:
            self.log.error(f"Error during input guards: {guard_error}")
            yield {
                "event": "step",
                "data": {
                    "step_name": step_name,
                    "step_number": step_number,
                    "message": "Validating request...",
                    "status": "error",
                    "error": str(guard_error)
                }
            }
            yield {"event": "final", "data": {"success": False, "error": str(guard_error)}}
            return

        # Handle blocked input
        if guard_result["guard_decision"] == "block":
            self.log.warning("Input blocked by guards.")

            # Format user-friendly error message
            error_msg = self.format_violation_message(guard_result)

            # Send error as LLM message so it appears in chat
            yield {
                "event": "llm",
                "data": {"delta": f"\n\n❌ {error_msg}\n"}
            }

            # Send step error event
            yield {
                "event": "step",
                "data": {
                    "step_name": step_name,
                    "step_number": step_number,
                    "message": "Validating request...",
                    "status": "error",
                    "error": error_msg
                }
            }

            # Send guard data event (optional - contains technical details)
            yield {"event": "guard", "data": guard_result}
            return

        # Validation passed - yield completed event
        yield {
            "event": "step",
            "data": {
                "step_name": step_name,
                "step_number": step_number,
                "message": "Validating request...",
                "status": "completed"
            }
        }

        # Yield validation result for caller to use
        yield {"event": "validation_result", "data": guard_result}

    @staticmethod
    def format_violation_message(guard_result: Dict) -> str:
        """
        Format a user-friendly error message from guard validation results.

        Args:
            guard_result: The guard validation result dictionary

        Returns:
            Formatted error message string
        """
        error_msg = guard_result.get("error", "Request blocked by security policy.")

        # If there are specific violations, include them
        violations = guard_result.get("violations", [])
        if violations:
            violation_details = []
            for v in violations:
                if v.get("violation_type") == "schedule":
                    violation_details.append(f"Schedule '{v.get('value')}' is not allowed.")
                else:
                    violation_details.append(f"{v.get('violation_type')}: {v.get('value')}")
            if violation_details:
                error_msg = "\n".join(violation_details)

        # If it's only a schedule violation, use just the schedule error
        if violations and len(violations) == 1 and violations[0].get("violation_type") == "schedule":
            error_msg = f"Schedule '{violations[0].get('value')}' is not allowed."

        return error_msg
