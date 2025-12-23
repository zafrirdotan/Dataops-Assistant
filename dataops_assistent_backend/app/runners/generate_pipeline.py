
import sys
import asyncio
from app.services.chat_service import ChatService
from app.models.pipeline_types import PipelineBuildResponse

async def main():
    fast = False
    args = sys.argv[1:]
    if '--fast' in args:
        fast = True
        args.remove('--fast')
    if len(args) < 1:
        print("Usage: python generate_pipeline.py 'your message here' [--fast] [--cmd]")
        return
    message = args[0]
    chat_service = ChatService()
    pipeline_result = await chat_service.process_message(message, fast=fast, mode="cmd")
    pretty_print_pipeline_result(pipeline_result)
    

def pretty_print_pipeline_result(pipeline_result):
    if not isinstance(pipeline_result, dict):
        print(pipeline_result)
        return
    build_spec: PipelineBuildResponse = pipeline_result.get('build_spec', {})
    print("""
──────────────────────────────────────────────────────────────
Pipeline Creation Summary
──────────────────────────────────────────────────────────────""")
    rows = [
        ("Pipeline Name", build_spec.get('pipeline_name', 'N/A')),
        ("Pipeline ID", build_spec.get('pipeline_id', 'N/A')),
        ("Build Steps Completed", build_spec.get('build_steps_completed', 'N/A')),
        ("Success", build_spec.get('success', 'N/A')),
        ("request_spec", build_spec.get('request_spec', 'N/A')),
        ("test_result", build_spec.get('test_result', 'N/A')),
        ("message", build_spec.get('message', 'N/A')),
        ("dockerize_result", build_spec.get('dockerize_result', 'N/A')),
        ("scheduling_result", build_spec.get('scheduling_result', 'N/A')),
        ("error", build_spec.get('error', 'N/A')),
    ]

    for label, value in rows:
        if value is not None:
            print(f"{label:<28}: {value}")
    print("──────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    asyncio.run(main())
