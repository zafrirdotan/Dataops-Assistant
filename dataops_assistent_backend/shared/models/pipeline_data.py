from sqlalchemy import Column, String, DateTime, Text, JSON, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

from shared.models.pipeline_types import Pipeline

Base = declarative_base()


class PipelineData(Base):
    __tablename__ = 'pipelines'
    __table_args__ = {'schema': 'dataops_assistent'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    created_by = Column(String, nullable=False)
    description = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc), onupdate=lambda: datetime.datetime.now(datetime.timezone.utc))
    status = Column(String, default="draft")
    spec = Column(JSON, default=dict)
    image_id = Column(String, nullable=True)

    # Relationship to chat messages
    # chat_messages = relationship("ChatMessage", back_populates="pipeline", cascade="all, delete-orphan")

    def to_dict(self)-> Pipeline:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'pipeline_id': self.pipeline_id,
            'name': self.name,
            'created_by': self.created_by,
            'description': self.description,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'status': self.status,
            'spec': self.spec,
            'image_id': self.image_id
        }
