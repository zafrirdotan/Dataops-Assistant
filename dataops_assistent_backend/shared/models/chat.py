from pydantic import BaseModel
from typing import Optional, List

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
    chat_id: Optional[str] = None  # Chat ID to continue existing conversation
