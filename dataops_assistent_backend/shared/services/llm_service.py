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

    async def stream_response_with_tools(self, system_prompt: str, messages: list[dict], tools: list) -> AsyncGenerator[dict, None]:
        """
        Stream response with tool support from the LLM provider.
        Yields dictionaries with delta, tool_call info, or done status.

        Args:
            system_prompt: System prompt for the assistant
            messages: Conversation history [{"role": "user/assistant", "content": "..."}]
            tools: Tool definitions in OpenAI format
        """
        if self.provider != "openai" or not self.api_key or not self.async_client:
            yield {"delta": "LLM is not configured.", "done": True}
            return

        try:
            # Build full messages array with system prompt
            full_messages = [{"role": "system", "content": system_prompt}] + messages

            stream = await self.async_client.chat.completions.create(
                model="gpt-4o",
                messages=full_messages,
                tools=tools,
                temperature=0,
                stream=True
            )

            full_content = ""
            tool_calls = []

            async for chunk in stream:
                delta = chunk.choices[0].delta

                # Handle text content
                if delta.content:
                    full_content += delta.content
                    yield {"type": "text", "delta": delta.content}

                # Handle tool calls
                if delta.tool_calls:
                    for tool_call in delta.tool_calls:
                        if tool_call.index is not None:
                            while len(tool_calls) <= tool_call.index:
                                tool_calls.append({"id": "", "name": "", "arguments": ""})

                            if tool_call.id:
                                tool_calls[tool_call.index]["id"] = tool_call.id
                            if tool_call.function.name:
                                tool_calls[tool_call.index]["name"] = tool_call.function.name
                            if tool_call.function.arguments:
                                tool_calls[tool_call.index]["arguments"] += tool_call.function.arguments

                # Check if done
                if chunk.choices[0].finish_reason:
                    if chunk.choices[0].finish_reason == "tool_calls" and tool_calls:
                        # Parse and yield tool call
                        import json
                        for tool_call in tool_calls:
                            yield {
                                "type": "tool_call",
                                "tool_name": tool_call["name"],
                                "tool_call_id": tool_call["id"],
                                "arguments": json.loads(tool_call["arguments"]) if tool_call["arguments"] else {}
                            }
                    yield {"done": True}
                    return

        except Exception as e:
            yield {"delta": f"LLM stream error: {e}", "done": True}

    async def complete_one_phrase(
        self, prompt: str, user_content: str, max_tokens: int = 80
    ) -> Optional[str]:
        """
        Single completion for short, one-phrase responses (e.g. chat title).
        Returns the assistant reply text or None on failure.
        """
        if self.provider != "openai" or not self.api_key or not self.async_client:
            return None
        try:
            response = await self.async_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=0,
                max_tokens=max_tokens,
            )
            content = response.choices[0].message.content
            return content.strip() if content else None
        except Exception:
            return None
