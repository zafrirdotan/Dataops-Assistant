# llm_service.py
"""
Service for handling calls to OpenAI or other LLM providers.
"""

from typing import Optional
import os
from xml.parsers.expat import model
import openai
import asyncio

class LLMService:
    def __init__(self, provider: str = "openai", api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        self.provider = provider
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.client = None

        if self.provider == "openai" and self.api_key:
            try:
                self.client = openai.Client(api_key=self.api_key)
                self.async_client = openai.AsyncClient(api_key=self.api_key)
            except Exception as e:
                print(f"Error initializing OpenAI client: {e}")
                self.client = None
                self.async_client = None

    async def response_create_async(self, input, text = None) -> Optional[dict] | str:
        """
        Async wrapper for openai.AsyncClient.responses.create. Accepts same kwargs as the sync version.
        Returns the response (usually a dict-like object) or an error string.
        """
        if self.provider == "openai" and self.api_key and self.async_client:
            try:
                response = await self.async_client.responses.create(
                        model="gpt-4.1",
                        input=input,
                        temperature=0,
                        text=text)
                
                return response
            except Exception as e:
                return f"OpenAI API error: {e}"
    

