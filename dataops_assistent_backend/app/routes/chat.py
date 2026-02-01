from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from shared.services.chat_service import ChatService
from dotenv import load_dotenv
import json

load_dotenv()

router = APIRouter()
chat_service = ChatService()

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str

class ChatStreamRequest(BaseModel):
    message: Optional[str] = None
    messages: Optional[List[Message]] = None
    fast: bool = False
    run_after_deploy: bool = False

@router.post("/")
async def chat_endpoint(request: ChatRequest):
    """
    Endpoint to handle chat requests.
    Delegates business logic to ChatService.
    """
    result = await chat_service.process_message(request.message)

    if result.get("guard_decision") == "block":
        raise HTTPException(status_code=400, detail=result)

    if result.get("guard_decision") == "review":
        return result

    return {"response": result.get("build_spec", {})}


def _format_sse(event: dict) -> str:
    event_name = event.get("event")
    data = event.get("data", {})
    payload = json.dumps(data, default=str)
    if event_name:
        return f"event: {event_name}\ndata: {payload}\n\n"
    return f"data: {payload}\n\n"


@router.post("/stream")
async def chat_stream_endpoint(request: ChatStreamRequest):
    """
    SSE endpoint to stream chat events and pipeline build steps.
    Accepts either a single message or full conversation history.
    """

    async def event_generator():
        # Convert Pydantic models to dicts if messages provided
        messages_list = None
        if request.messages:
            messages_list = [msg.model_dump() for msg in request.messages]

        async for event in chat_service.process_message_stream(
            raw_message=request.message,
            messages=messages_list,
            fast=request.fast,
            run_after_deploy=request.run_after_deploy
        ):
            yield _format_sse(event)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )
