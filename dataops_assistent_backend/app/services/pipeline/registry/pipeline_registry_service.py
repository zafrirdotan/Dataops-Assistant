
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.pipeline import Pipeline, Base
import datetime
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://dataops_user:dataops_password@localhost:5432/dataops_db")

class PipelineRegistryService:
    """Service to manage pipeline registry and metadata in PostgreSQL."""
    def __init__(self):
        self.engine = create_engine(DB_URL)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        # Test the connection upon initialization
        try:
            connection = self.engine.connect()
            connection.close()
        except Exception as e:
            raise Exception(f"Failed to connect to the database: {e}")

    def create_pipeline(self, pipeline_id, name, created_by, description=None, spec=None):
        session = self.Session()
        now = datetime.datetime.now(datetime.timezone.utc)
        pipeline = Pipeline(
            pipeline_id=pipeline_id,
            name=name,
            created_by=created_by,
            description=description or "",
            created_at=now,
            updated_at=now,
            status="draft",
            run_list=[],
            spec=spec or {},
        )
        session.add(pipeline)
        session.commit()
        session.refresh(pipeline)
        session.close()
        return pipeline

    def update_pipeline(self, pipeline_id, updates):
        session = self.Session()
        pipeline = session.query(Pipeline).filter_by(pipeline_id=pipeline_id).first()
        if not pipeline:
            session.close()
            return None
        for key, value in updates.items():
            setattr(pipeline, key, value)
        pipeline.updated_at = datetime.datetime.now(datetime.timezone.utc)
        session.commit()
        session.refresh(pipeline)
        session.close()
        return pipeline

    def get_pipeline(self, pipeline_id):
        session = self.Session()
        pipeline = session.query(Pipeline).filter_by(pipeline_id=pipeline_id).first()
        session.close()
        return pipeline

    def list_pipelines(self):
        session = self.Session()
        pipelines = session.query(Pipeline).all()
        session.close()
        return pipelines

    def add_run(self, pipeline_id, run_info):
        session = self.Session()
        pipeline = session.query(Pipeline).filter_by(pipeline_id=pipeline_id).first()
        if not pipeline:
            session.close()
            return None
        pipeline.run_list.append(run_info)
        pipeline.updated_at = datetime.datetime.now(datetime.timezone.utc)
        session.commit()
        session.refresh(pipeline)
        session.close()
        return pipeline

    def update_status(self, pipeline_id, status):
        session = self.Session()
        pipeline = session.query(Pipeline).filter_by(pipeline_id=pipeline_id).first()
        if not pipeline:
            session.close()
            return None
        pipeline.status = status
        pipeline.updated_at = datetime.datetime.now(datetime.timezone.utc)
        session.commit()
        session.refresh(pipeline)
        session.close()
        return pipeline

pipelineRegistryService = PipelineRegistryService()

def getPipelineRegistryService() -> PipelineRegistryService:
    return pipelineRegistryService
