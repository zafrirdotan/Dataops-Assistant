import json
import logging

from app.services.llm_service import LLMService
from app.services.pipeline.guards.prompt_guard_service import PromptGuardService
from app.services.pipeline import PipelineBuilderService
from app.services.storage_service import MinioStorage
import logging
logger = logging.getLogger("dataops")

class ChatService:
    def __init__(self):
        self.logger = logger
        self.llm_service = LLMService()
        self.prompt_guard_service = PromptGuardService(log=self.logger)
        self.pipeline_builder_service = PipelineBuilderService()
        self.storage_service = MinioStorage()

    async def process_message(self, raw_message: str, fast: bool = False, mode: str = "chat") -> dict:
        """
        Process the user message, validate it, and get a response from the LLM.
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
        try:
            guardResponse = await self.prompt_guard_service.llm_guard_check(cleaned_input)

        except Exception as e:
            logging.error(f"Error during LLM Guard Check: {e}")
            return 
        
        logging.info("LLM Guard Response:\n%s", json.dumps(guardResponse, indent=2))

        if not guardResponse.get("is_safe", False):
            return {
                "guard_decision": "block",
                "error": f"Input blocked by LLM Guard: {guardResponse.get('reason', 'No reason provided')}",
                "findings": guardResponse.get("violations", [])
            }

        # Step 2: Generate pipeline
        build_spec = await self.pipeline_builder_service.build_pipeline(cleaned_input, fast=fast, mode=mode)

        return {
            "guard_decision": "allow",
            "build_spec": build_spec
        }