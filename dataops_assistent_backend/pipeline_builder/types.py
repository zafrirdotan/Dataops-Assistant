from typing import TypedDict

class CodeGenResult(TypedDict):
    pipeline: str
    requirements: str
    tests: str
