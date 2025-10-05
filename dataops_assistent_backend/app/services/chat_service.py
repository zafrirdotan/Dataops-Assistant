from app.services.llm_service import LLMService
from app.services.pipeline.guards.prompt_guard_service import PromptGuardService
from app.services.pipeline import PipelineBuilderService

class ChatService:
    def __init__(self):
        self.llm_service = LLMService()
        self.prompt_guard_service = PromptGuardService()
        self.pipeline_builder_service = PipelineBuilderService()

    def process_message(self, raw_message: str) -> dict:
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
        build_result = self.pipeline_builder_service.build_pipeline_with_templates(analysis["cleaned"])

        # Step 4: Sanitize the LLM response for display
        # sanitized_response = self.prompt_guard_service.sanitize_for_display(llm_response)

        return {
            "decision": "allow",
            "response": build_result
        }
