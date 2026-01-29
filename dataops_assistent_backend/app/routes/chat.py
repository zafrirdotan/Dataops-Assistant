from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from shared.services.chat_service import ChatService
from dotenv import load_dotenv
import json

load_dotenv()

router = APIRouter()
chat_service = ChatService()
class ChatRequest(BaseModel):
    message: str

class ChatStreamRequest(BaseModel):
    message: str
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
    """

    async def event_generator():
        async for event in chat_service.process_message_stream(
            request.message,
            fast=request.fast,
            run_after_deploy=request.run_after_deploy
        ):
            yield _format_sse(event)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )
