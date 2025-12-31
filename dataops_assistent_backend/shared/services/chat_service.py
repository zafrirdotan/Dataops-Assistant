import json
import logging

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