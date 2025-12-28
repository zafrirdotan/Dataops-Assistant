import os
import json
import asyncio
from typing import Dict, Any
from datetime import datetime


class LocalStorageService:
	"""
	Local storage service for testing pipeline storage locally.
	Stores pipeline files in a local 'pipelines' directory, organized by pipeline_id and version.
	"""
	def __init__(self, base_dir: str = None):
		self.base_dir = base_dir or os.path.join(os.getcwd(), "pipelines")
		os.makedirs(self.base_dir, exist_ok=True)

	async def store_pipeline(self, pipeline_id: str, pipeline_data: Dict[str, Any]) -> Dict[str, str]:
		"""
		Store pipeline files locally under pipelines/{pipeline_id}/ (no versioning).
		Returns a dict of file paths.
		"""

		def sync_store():
			pipeline_dir = os.path.join(self.base_dir, pipeline_id)
			os.makedirs(pipeline_dir, exist_ok=True)
			stored_files = {}

			def write_file(filename, content):
				path = os.path.join(pipeline_dir, filename)
				with open(path, "w", encoding="utf-8") as f:
					f.write(content)
				return path

			if 'pipeline' in pipeline_data:
				stored_files['pipeline'] = write_file("pipeline.py", pipeline_data['pipeline'])
			if 'requirements' in pipeline_data:
				stored_files['requirements'] = write_file("requirements.txt", pipeline_data['requirements'])
			if 'test_code' in pipeline_data:
				stored_files['test_code'] = write_file("test.py", pipeline_data['test_code'])
			if 'env_template' in pipeline_data:
				stored_files['.env'] = write_file(".env", pipeline_data['env_template'])
			if 'dockerfile' in pipeline_data:
				stored_files['dockerfile'] = write_file("Dockerfile", pipeline_data['dockerfile'])
			if 'spec' in pipeline_data:
				spec_path = os.path.join(pipeline_dir, "spec.json")
				with open(spec_path, "w", encoding="utf-8") as f:
					json.dump(pipeline_data['spec'], f, indent=2)
				stored_files['spec'] = spec_path
			if 'test_results' in pipeline_data:
				results_path = os.path.join(pipeline_dir, "test_results.json")
				with open(results_path, "w", encoding="utf-8") as f:
					json.dump(pipeline_data['test_results'], f, indent=2)
				stored_files['test_results'] = results_path
			if 'logs' in pipeline_data:
				stored_files['logs'] = write_file("execution.log", pipeline_data['logs'])

			# Save metadata
			metadata = {
				"pipeline_id": pipeline_id,
				"created_at": datetime.now().isoformat(),
				"stored_files": stored_files,
				"file_count": len(stored_files)
			}
			metadata_path = os.path.join(pipeline_dir, "metadata.json")
			with open(metadata_path, "w", encoding="utf-8") as f:
				json.dump(metadata, f, indent=2)
			stored_files['metadata'] = metadata_path

			return stored_files

		return await asyncio.to_thread(sync_store)
	
	async def retrieve_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
		"""
		Retrieve pipeline by ID (no versioning, just files under pipelines/{pipeline_id}/).
		Returns dict with all stored files' contents (no metadata).
		"""
		import traceback
		try:
			pipeline_dir = os.path.join(self.base_dir, pipeline_id)
			if not os.path.exists(pipeline_dir):
				raise ValueError(f"No pipeline found for ID {pipeline_id}")

			metadata_path = os.path.join(pipeline_dir, "metadata.json")
			if not os.path.exists(metadata_path):
				raise ValueError(f"metadata.json not found for pipeline ID {pipeline_id}")
			with open(metadata_path, "r", encoding="utf-8") as f:
				metadata = json.load(f)

			pipeline_data = {}

			# Retrieve all stored files (skip metadata)
			for file_type, file_path in metadata["stored_files"].items():
				if file_type == "metadata":
					continue
				if file_type in ["spec", "test_results"]:
					# JSON files
					with open(file_path, "r", encoding="utf-8") as f:
						pipeline_data[file_type] = json.load(f)
				else:
					# Text files
					with open(file_path, "r", encoding="utf-8") as f:
						pipeline_data[file_type] = f.read()

			return pipeline_data
		except Exception as e:
			import logging
			logging.error(f"Error retrieving pipeline: {e}\n{traceback.format_exc()}")
			raise