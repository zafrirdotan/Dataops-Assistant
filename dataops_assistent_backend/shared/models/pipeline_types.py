from typing import Any
from typing_extensions import TypedDict

class Pipeline(TypedDict, total=False):
    id: int
    pipeline_id: str
    name: str
    created_by: str
    description: str
    created_at: str
    updated_at: str | None
    status: str
    spec: dict[str, Any]
    image_id: str | None

class PipelineBuildResponse(TypedDict, total=False):
    pipeline_name: str | None
    pipeline_id: str | None
    build_steps_completed: str | None
    success: bool
    request_spec: dict[str, Any]
    test_result: dict[str, Any]
    test_runner_result: dict[str, Any]
    message: str | None
    dockerize_result: dict[str, Any]
    scheduling_result: dict[str, Any]
    execution_time: int | None
    error: str | None
