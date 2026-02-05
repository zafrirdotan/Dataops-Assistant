from typing_extensions import TypedDict

class CodeGenResult(TypedDict):
    pipeline: str
    requirements: str
    tests: str
