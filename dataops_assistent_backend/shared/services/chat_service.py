import json
import logging
import asyncio
from typing import AsyncGenerator

from shared.services.llm_service import LLMService
from pipeline_builder.guards.prompt_guard_service import PromptGuardService
from pipeline_builder import PipelineBuilderService
from shared.services.storage_service import MinioStorage
import logging

from shared.utils.spinner_utils import run_step_with_spinner
logger = logging.getLogger("dataops")

class ChatService:
    def __init__(self):
        self.logger = logger
        self.llm_service = LLMService()
        self.prompt_guard_service = PromptGuardService(log=self.logger)
        self.pipeline_builder_service = PipelineBuilderService()
        self.storage_service = MinioStorage()

    async def process_message(self, raw_message: str, fast: bool = False, mode: str = "chat", run_after_deploy: bool = False) -> dict:
        """
        Process the user message, validate it, and get a response from the LLM.
        """

        # Step 1: Run guards on input
        guard_result, guard_error = await self._run_step(
            "Validating request...",
            0,
            self.run_guards_on_input,
            raw_message,
            mode=mode
        )
        if guard_error:
            self.logger.error(f"Error during input guards: {guard_error}")
            return {"guard_decision": "block", "error": str(guard_error)}

        if guard_result["guard_decision"] == "block":
            self.logger.warning("Input blocked by guards.")
            return guard_result

        build_spec = await self.pipeline_builder_service.build_pipeline(guard_result["cleaned_input"], fast=fast, mode=mode, run_after_deploy=run_after_deploy)

        return {
            "guard_decision": "allow",
            "build_spec": build_spec
        }

    async def process_message_stream(self, raw_message: str, fast: bool = False, run_after_deploy: bool = False) -> AsyncGenerator[dict, None]:
        """
        Stream processing events for the user message and pipeline build steps.
        """
        yield {
            "event": "step",
            "data": {
                "step": "validate_request",
                "step_number": 0,
                "message": "Validating request...",
                "status": "started"
            }
        }

        guard_result, guard_error = await self._run_step(
            "Validating request...",
            0,
            self.run_guards_on_input,
            raw_message,
            mode="chat"
        )

        if guard_error:
            self.logger.error(f"Error during input guards: {guard_error}")
            yield {
                "event": "step",
                "data": {
                    "step": "validate_request",
                    "step_number": 0,
                    "message": "Validating request...",
                    "status": "error",
                    "error": str(guard_error)
                }
            }
            yield {"event": "final", "data": {"success": False, "error": str(guard_error)}}
            return

        if guard_result["guard_decision"] == "block":
            self.logger.warning("Input blocked by guards.")
            yield {
                "event": "step",
                "data": {
                    "step": "validate_request",
                    "step_number": 0,
                    "message": "Validating request...",
                    "status": "completed"
                }
            }
            yield {"event": "guard", "data": guard_result}
            yield {"event": "final", "data": {"success": False, "guard": guard_result}}
            return

        yield {
            "event": "step",
            "data": {
                "step": "validate_request",
                "step_number": 0,
                "message": "Validating request...",
                "status": "completed"
            }
        }

        if getattr(self.llm_service, "async_client", None):
            yield {
                "event": "step",
                "data": {
                    "step": "assistant_guidance",
                    "step_number": 0,
                    "message": "Generating guidance and questions...",
                    "status": "started"
                }
            }

            guidance_prompt = (
                "You are a data engineering assistant. "
                "Given the user request, provide a short guidance on how to frame an ETL request, "
                "and ask up to 5 clarifying questions only if needed. "
                "Keep it concise.\n\n"
                f"User request: {raw_message}"
            )

            async for delta in self.llm_service.stream_response(guidance_prompt):
                yield {"event": "llm", "data": {"phase": "guidance", "delta": delta}}

            yield {
                "event": "step",
                "data": {
                    "step": "assistant_guidance",
                    "step_number": 0,
                    "message": "Generating guidance and questions...",
                    "status": "completed"
                }
            }
        else:
            yield {
                "event": "step",
                "data": {
                    "step": "assistant_guidance",
                    "step_number": 0,
                    "message": "Generating guidance and questions...",
                    "status": "skipped",
                    "reason": "LLM not configured"
                }
            }

        queue: asyncio.Queue = asyncio.Queue()

        async def on_event(payload: dict):
            await queue.put(payload)

        build_task = asyncio.create_task(
            self.pipeline_builder_service.build_pipeline(
                guard_result["cleaned_input"],
                fast=fast,
                mode="chat",
                run_after_deploy=run_after_deploy,
                event_callback=on_event
            )
        )

        while True:
            if build_task.done() and queue.empty():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.2)
                yield event
            except asyncio.TimeoutError:
                continue

        build_spec = await build_task
        if build_spec.get("error") or build_spec.get("success") is False:
            yield {
                "event": "final",
                "data": {
                    "success": False,
                    "error": build_spec.get("error") or build_spec.get("details"),
                    "build_spec": build_spec
                }
            }
            return

        yield {
            "event": "final",
            "data": {
                "success": True,
                "pipeline_id": build_spec.get("pipeline_id"),
                "pipeline_code": build_spec.get("pipeline_code"),
                "build_spec": build_spec
            }
        }


    async def run_guards_on_input(self, raw_message: str) -> dict:
        """
        Run prompt guard analysis and LLM guard checks on the input message.
        """
        # Step 1: Analyze and validate user input
        analysis = self.prompt_guard_service.analyze(raw_message)
        logging.info(f"Prompt Guard Analysis: {analysis}")
        if analysis["decision"] == "block":
            logging.warning(f"Input blocked: {analysis['findings']}")
            return {
                "guard_decision": "block",
                "error": "Input blocked due to security concerns.",
                "findings": analysis["findings"]
            }

        cleaned_input = analysis["cleaned"]
        # Step 2: Perform LLM Guard Check
        try:
            guardResponse = await self.prompt_guard_service.llm_guard_check(cleaned_input)
        except Exception as e:
            logging.error(f"Error during LLM Guard Check: {e}")
            return {
                "guard_decision": "block",
                "error": f"LLM Guard Check failed: {str(e)}"
            }

        logging.info("LLM Guard Response:\n%s", json.dumps(guardResponse, indent=2))
        if not guardResponse.get("is_safe", False):
            return {
                "guard_decision": "block",
                "error": f"Input blocked by LLM Guard: {guardResponse.get('reason', 'No reason provided')}",
                "findings": guardResponse.get("violations", [])
            }
        return {
            "guard_decision": "allow",
            "cleaned_input": cleaned_input
        }

    async def _run_step(self, step_msg: str, step_number: int, coro, *args, mode="chat", **kwargs):
        """
        Helper to run an async step with optional CLI spinner.
        Returns (result, error). If error is not None, result is None.
        """
        if mode == "cmd":
            return await run_step_with_spinner(step_msg, step_number, coro, *args, **kwargs)
        else:
            try:
                result = await coro(*args, **kwargs)
                return result, None
            except Exception as e:
                return None, e
