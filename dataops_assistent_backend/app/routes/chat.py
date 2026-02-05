from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from shared.models.chat import ChatRequest, ChatStreamRequest
from shared.services.chat_service import ChatService
from app.core.deps import get_current_active_user
from shared.models.user import User
from dotenv import load_dotenv
import json

load_dotenv()

router = APIRouter()
chat_service = ChatService()


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
async def chat_stream_endpoint(
    request: ChatStreamRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    SSE endpoint to stream chat events and pipeline build steps.
    Accepts either a single message or full conversation history.
    Requires authentication.
    Chat messages are automatically saved to the database.
    """

    async def event_generator():
        # Convert Pydantic models to dicts if messages provided
        messages_list = None
        if request.messages:
            messages_list = [msg.model_dump() for msg in request.messages]

        # Delegate all business logic to service, only handle SSE formatting here
        async for event in chat_service.process_chat_stream_with_persistence(
            chat_id=request.chat_id,
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


@router.get("/history/{chat_id}")
async def get_chat_history(
    chat_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    Get chat history for a specific chat.
    """
    try:
        messages = await chat_service.get_chat_history(chat_id)
        return {"messages": messages}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to retrieve chat history: {str(e)}")


@router.get("/by-pipeline/{pipeline_id}")
async def get_chat_by_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    Get chat ID for a specific pipeline. Used by sidebar to load chat when clicking a pipeline.
    """
    try:
        chat_id = await chat_service.get_chat_id_by_pipeline(pipeline_id)

        if not chat_id:
            raise HTTPException(status_code=404, detail=f"No chat found for pipeline {pipeline_id}")

        return {"chat_id": chat_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve chat: {str(e)}")
