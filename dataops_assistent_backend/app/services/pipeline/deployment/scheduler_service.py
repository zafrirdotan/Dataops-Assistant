
import os
import json
import aiofiles
from datetime import datetime

class SchedulerService:
    """Service for scheduling pipeline runs on Airflow."""

    def __init__(self, log):
        self.log = log
        
    async def save_pipeline_to_catalog(self, pipeline_id: str, spec: dict):
        """Save pipeline DAG to catalog.json"""
        catalog_path = os.path.join(os.path.dirname(__file__), '/app/catalog.json')
        catalog_path = os.path.abspath(catalog_path)
        try:
            if os.path.exists(catalog_path):
                async with aiofiles.open(catalog_path, 'r') as f:
                    content = await f.read()
                    catalog = json.loads(content) if content else {"pipelines": []}
            else:
                catalog = {"pipelines": []}
            # Remove any existing pipeline with same id
            catalog["pipelines"] = [p for p in catalog["pipelines"] if p.get("pipeline_id") != pipeline_id]
            catalog["pipelines"].append({"id": pipeline_id,
                                          "schedule": spec.get("schedule"),
                                          "description": spec.get("description", ""),
                                          "tags": spec.get("tags", []),
                                          "mode": spec.get("mode", "full"),
                                          "start_date": datetime.now().isoformat()
                                          })
            async with aiofiles.open(catalog_path, 'w') as f:
                await f.write(json.dumps(catalog, indent=2))
            self.log.info(f"Pipeline {pipeline_id} saved to catalog.json")
            return {"success": True}
        except Exception as e:
            self.log.error(f"Failed to save pipeline to catalog.json: {e}")
            return {"success": False, "error": str(e)}

