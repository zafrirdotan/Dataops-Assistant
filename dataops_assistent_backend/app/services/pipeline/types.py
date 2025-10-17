from typing import TypedDict

class CodeGenResult(TypedDict):
    code: str
    requirements: str
    tests: str
