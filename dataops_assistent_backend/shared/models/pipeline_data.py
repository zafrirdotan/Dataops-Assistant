from sqlalchemy import Column, String, DateTime, Text, JSON, Integer
from sqlalchemy.ext.declarative import declarative_base
import datetime

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
    run_list = Column(JSON, default=list)
    spec = Column(JSON, default=dict)
    image_id = Column(String, nullable=True)
