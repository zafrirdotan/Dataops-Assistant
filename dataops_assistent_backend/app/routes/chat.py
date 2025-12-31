from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shared.services.chat_service import ChatService
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()
chat_service = ChatService()
class ChatRequest(BaseModel):
    message: str

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
