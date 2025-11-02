from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.services.chat_service import ChatService
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

    if result["decision"] == "block":
        raise HTTPException(status_code=400, detail=result)

    if result["decision"] == "review":
        return result

    return {"response": result["response"]}
