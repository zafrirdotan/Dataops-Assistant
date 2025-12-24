from typing import TypedDict, Any

class PipelineBuildResponse(TypedDict, total=False):
    pipeline_name: str | None
    pipeline_id: str | None
    build_steps_completed: str | None
    success: bool
    request_spec: dict[str, Any]
    test_result: dict[str, Any]
    message: str | None
    dockerize_result: dict[str, Any]
    scheduling_result: dict[str, Any]
    execution_time: int | None
    error: str | None


