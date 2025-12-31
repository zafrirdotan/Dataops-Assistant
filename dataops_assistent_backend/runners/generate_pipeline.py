
import sys
import asyncio
from shared.services.chat_service import ChatService
from shared.models.pipeline_types import PipelineBuildResponse

async def main():
    fast = False
    run_after_deploy = False
    args = sys.argv[1:]
    if '--fast' in args:
        fast = True
        args.remove('--fast')
    if '--run-after-deploy' in args:
        run_after_deploy = True
        args.remove('--run-after-deploy')
    if len(args) < 1:
        print("Usage: python generate_pipeline.py 'your message here' [--fast] [--run-after-deploy] [--cmd]")
        return
    message = args[0]
    chat_service = ChatService()

    print("""
──────────────────────────────────────────────────────────────
    \033[94mStarting pipeline generation\033[0m
──────────────────────────────────────────────────────────────
          """)
    
    pipeline_result = await chat_service.process_message(message, fast=fast, mode="cmd", run_after_deploy=run_after_deploy)
    pretty_print_pipeline_result(pipeline_result)
    

def pretty_print_pipeline_result(pipeline_result):
    if not isinstance(pipeline_result, dict):
        print(pipeline_result)
        return
    build_spec: PipelineBuildResponse = pipeline_result.get('build_spec', {})
    print("\033[94m" + """
──────────────────────────────────────────────────────────────
    Pipeline Creation Summary
──────────────────────────────────────────────────────────────""" + "\033[0m")
    rows = [
        ("Pipeline Name", build_spec.get('pipeline_name', 'N/A')),
        ("Pipeline ID", build_spec.get('pipeline_id', 'N/A')),
        ("Container ID", build_spec.get('container_id', 'N/A')),
        ("Build Steps Completed", build_spec.get('build_steps_completed', 'N/A')),
        ("Success", build_spec.get('success', 'N/A')),
        ("Message", build_spec.get('message', 'N/A')),
        ("Error", build_spec.get('error', 'N/A')),
    ]

    for label, value in rows:
        if value is not None:
            print(f"{label:<28}: {value}")
    print("──────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    asyncio.run(main())
