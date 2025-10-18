import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

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

        if analysis["decision"] == "block":
            return {
                "decision": "block",
                "error": "Input blocked due to security concerns.",
                "findings": analysis["findings"]
            }

        if analysis["decision"] == "review":

            if len(analysis["findings"]) == 1 and analysis["findings"][0].get("rule") == "python_import":
                # If only python_import is found, allow it

                # TODO: Log this event for auditing and send to LLM for confirmation
                pass
            else:
                return {
                    "decision": "review",
                    "error": "Input requires review.",
                    "findings": analysis["findings"]
                }

        # Step 2: Generate pipeline 
        build_result = await self.pipeline_builder_service.build_pipeline(analysis["cleaned"])

        # Step 3: Store pipeline in MinIO if generation was successful
        # if build_result.get("success"):
        #     pipeline_id = await self._store_pipeline_in_minio(build_result, analysis["cleaned"])
        #     if pipeline_id:
        #         build_result["pipeline_id"] = pipeline_id
        #         build_result["stored_in_minio"] = True
        #         self.logger.info(f"Pipeline {pipeline_id} stored in MinIO successfully")
        #     else:
        #         build_result["stored_in_minio"] = False
        #         self.logger.warning("Failed to store pipeline in MinIO")

        # Step 4: Sanitize the LLM response for display
        # sanitized_response = self.prompt_guard_service.sanitize_for_display(llm_response)

        return {
            "decision": "allow",
            "response": build_result
        }