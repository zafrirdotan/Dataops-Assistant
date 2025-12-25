# step_runner.py

import sys

import asyncio
import inspect
import importlib.util
import os

def run_step(step_number, *args):
    steps_dir = os.path.join(os.path.dirname(__file__), "steps")
    step_file = os.path.join(steps_dir, f"step{step_number}.py")
    if not os.path.isfile(step_file):
        print(f"Step file {step_file} not found.")
        sys.exit(1)
    spec = importlib.util.spec_from_file_location(f"step{step_number}", step_file)
    step_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(step_module)
    if hasattr(step_module, "main"):
        main_func = step_module.main
        if inspect.iscoroutinefunction(main_func):
            asyncio.run(main_func(*args))
        else:
            main_func(*args)
    else:
        print(f"Step {step_number} does not have a main() function.")
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m runners.step_runner <step_number> [args...]")
        sys.exit(1)
    step_number = sys.argv[1]
    args = sys.argv[2:]
    run_step(step_number, *args)


if __name__ == "__main__":
    main()
