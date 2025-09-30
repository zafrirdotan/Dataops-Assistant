# llm_service.py
"""
Service for handling calls to OpenAI or other LLM providers.
"""

from typing import Optional
import os
import openai

class LLMService:
    def __init__(self, provider: str = "openai", api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        self.provider = provider
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.client = None

        if self.provider == "openai" and self.api_key:
            try:
                self.client = openai.Client(api_key=self.api_key)
            except Exception as e:
                print(f"Error initializing OpenAI client: {e}")
                self.client = None

    def generate_response(self, prompt: str) -> str:
        if self.provider == "openai" and self.api_key:
            try:
                response =  self.client.responses.create(
                    model=self.model,
                    input=[{"role": "user", "content": prompt}]
                )
                return response.output_text
            except Exception as e:
                return f"OpenAI API error: {e}"
        # Add other providers here as needed
        return f"[LLM {self.provider}] Response to: {prompt}"
    

   # basic response create wrapper for openai
    def response_create(self, **kwargs) -> Optional[dict]:
        if self.provider == "openai" and self.api_key:
            try:
                response =  self.client.responses.create(**kwargs)
                return response
            except Exception as e:
                return f"OpenAI API error: {e}"
        return None
    

