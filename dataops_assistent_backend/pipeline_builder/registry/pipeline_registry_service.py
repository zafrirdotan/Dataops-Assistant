

import os
import datetime
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.future import select
from shared.models.pipeline_data import PipelineData, Base
from shared.models.pipeline_types import Pipeline

load_dotenv()
DB_URL = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://dataops_user:dataops_password@postgres:5432/dataops_db")

class PipelineRegistryService:
    """Async service to manage pipeline registry and metadata in PostgreSQL."""
    def __init__(self):
        self.engine = create_async_engine(DB_URL, echo=False, future=True)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def create_pipeline(self, pipeline_id, name, created_by, description=None, spec=None) -> Pipeline:
        async with self.Session() as session:
            now = datetime.datetime.now(datetime.timezone.utc)
            pipeline = PipelineData(
                pipeline_id=pipeline_id,
                name=name,
                created_by=created_by,
                description=description or "",
                created_at=now,
                updated_at=now,
                status="draft",
                spec=spec or {},
            )
            session.add(pipeline)
            await session.commit()
            await session.refresh(pipeline)
            return pipeline.to_dict()

    async def update_pipeline(self, pipeline_id, updates) -> Pipeline | None:
        async with self.Session() as session:
            result = await session.execute(select(PipelineData).filter_by(pipeline_id=pipeline_id))
            pipeline = result.scalar_one_or_none()
            if not pipeline:
                return None
            for key, value in updates.items():
                setattr(pipeline, key, value)
            pipeline.updated_at = datetime.datetime.now(datetime.timezone.utc)
            await session.commit()
            await session.refresh(pipeline)
            return pipeline.to_dict()

    async def get_pipeline(self, pipeline_id) -> Pipeline | None:
        async with self.Session() as session:
            result = await session.execute(select(PipelineData).filter_by(pipeline_id=pipeline_id))
            pipeline = result.scalar_one_or_none()
            return pipeline.to_dict() if pipeline else None

    async def list_pipelines(self) -> list[Pipeline]:
        async with self.Session() as session:
            result = await session.execute(select(PipelineData))
            pipelines = result.scalars().all()
            return [p.to_dict() for p in pipelines]

    async def update_status(self, pipeline_id, status) -> Pipeline | None:
        async with self.Session() as session:
            result = await session.execute(select(PipelineData).filter_by(pipeline_id=pipeline_id))
            pipeline = result.scalar_one_or_none()
            if not pipeline:
                return None
            pipeline.status = status
            pipeline.updated_at = datetime.datetime.now(datetime.timezone.utc)
            await session.commit()
            await session.refresh(pipeline)
            return pipeline.to_dict()

pipelineRegistryService = PipelineRegistryService()

def getPipelineRegistryService() -> PipelineRegistryService:
    return pipelineRegistryService
