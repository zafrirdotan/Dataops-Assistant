"""
SQLAlchemy ORM models for chat-related database tables.
"""
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, JSON, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from shared.services.database_service import Base
import uuid


def generate_uuid():
    """Generate a UUID for primary keys."""
    return uuid.uuid4()


class Chat(Base):
    """Chat model representing a conversation."""
    __tablename__ = "chats"
    __table_args__ = {"schema": "dataops_assistent"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=generate_uuid)
    pipeline_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationship to messages
    messages = relationship("ChatMessage", back_populates="chat", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Chat(id={self.id}, pipeline_id={self.pipeline_id})>"


class ChatMessage(Base):
    """ChatMessage model representing individual messages in a conversation."""
    __tablename__ = "chat_messages"
    __table_args__ = {"schema": "dataops_assistent"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(UUID(as_uuid=True), ForeignKey("dataops_assistent.chats.id"), nullable=False)
    role = Column(String(20), nullable=False)  # 'user' or 'assistant'
    content = Column(Text, nullable=False)
    extra_data = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationship to chat
    chat = relationship("Chat", back_populates="messages")

    def __repr__(self):
        return f"<ChatMessage(id={self.id}, chat_id={self.chat_id}, role={self.role})>"

    def to_dict(self):
        """Convert message to dictionary for API responses."""
        return {
            "id": self.id,
            "chat_id": self.chat_id,
            "role": self.role,
            "content": self.content,
            "extra_data": self.extra_data,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
