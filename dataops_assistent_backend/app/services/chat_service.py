import logging

from app.services.llm_service import LLMService
from app.services.pipeline.guards.prompt_guard_service import PromptGuardService
from app.services.pipeline import PipelineBuilderService
from app.services.storage_service import MinioStorage

class ChatService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.llm_service = LLMService()
        self.prompt_guard_service = PromptGuardService()
        self.pipeline_builder_service = PipelineBuilderService()
        self.storage_service = MinioStorage()

    async def process_message(self, raw_message: str) -> dict:
        """
        Process the user message, validate it, and get a response from the LLM.
        """
        # Step 1: Analyze and validate user input
        analysis = self.prompt_guard_service.analyze(raw_message)
        logging.info(f"Prompt Guard Analysis: {analysis}")
        if analysis["decision"] == "block":
            logging.warning(f"Input blocked: {analysis['findings']}")   
            return {
                "decision": "block",
                "error": "Input blocked due to security concerns.",
                "findings": analysis["findings"]
            }
            
        cleaned_input = analysis["cleaned"]   
        try:
            guardResponse = await self.prompt_guard_service.llm_guard_check(cleaned_input)

        except Exception as e:
            logging.error(f"Error during LLM Guard Check: {e}")
            return 
        
        logging.info(f"LLM Guard Response: {guardResponse}")

        if not guardResponse.get("is_safe", False):
            return {
                "decision": "block",
                "error": f"Input blocked by LLM Guard: {guardResponse.get('reason', 'No reason provided')}"
            }

        # Step 2: Generate pipeline
        build_result = await self.pipeline_builder_service.build_pipeline(cleaned_input)

        return {
            "decision": "allow",
            "response": build_result
        }