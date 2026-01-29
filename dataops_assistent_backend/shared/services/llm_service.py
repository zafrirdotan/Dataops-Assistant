# llm_service.py
"""
Service for handling calls to OpenAI or other LLM providers.
"""

from typing import Optional, AsyncGenerator
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


    async def stream_response(self, input, text=None) -> AsyncGenerator[str, None]:
        """
        Stream response text deltas from the LLM provider.
        Yields text chunks as they arrive.
        """
        if self.provider != "openai" or not self.api_key or not self.async_client:
            yield "LLM is not configured."
            return

        try:
            responses_api = getattr(self.async_client, "responses", None)
            if responses_api and hasattr(responses_api, "stream"):
                async with responses_api.stream(
                    model="gpt-4.1",
                    input=input,
                    temperature=0,
                    text=text,
                ) as stream:
                    async for event in stream:
                        event_type = getattr(event, "type", None)
                        if event_type is None and isinstance(event, dict):
                            event_type = event.get("type")
                        if event_type == "response.output_text.delta":
                            delta = getattr(event, "delta", None)
                            if delta is None and isinstance(event, dict):
                                delta = event.get("delta")
                            if delta:
                                yield delta
                return

            response = await self.response_create_async(input=input, text=text)
            if isinstance(response, str):
                yield response
                return

            output_text = getattr(response, "output_text", None)
            if output_text is None and isinstance(response, dict):
                output_text = response.get("output_text")
            if output_text:
                yield output_text

        except Exception as e:
            yield f"LLM stream error: {e}"
    

