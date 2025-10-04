from typing import List, Optional
from app.utils.json_utils import safe_json_dumps

class code_prompt_v3:
    
    def __init__(self):
        self.context = "You are an expert Python 3.13 engineer generating production-quality ETL pipeline code."
        self.allowed_packages = [
            "pandas>=2.0.0",
            "numpy>=1.24.0", 
            "python-dotenv>=1.0.0",
            "pyarrow>=14.0.0",
            "pytest>=7.0.0"
        ]
    
    def _build_context_section(self, spec: dict, data_preview: dict) -> str:
        """Build the context section of the prompt."""
        return f"""## CONTEXT
{self.context}

**Pipeline Specification:**
{safe_json_dumps(spec, indent=2)}

**Data Preview:**
{safe_json_dumps(data_preview, indent=2)}"""
        
    def _build_constraints_section(self) -> str:
        """Build the constraints section of the prompt."""
        return f"""## CONSTRAINTS
- **Python Version:** 3.13 with best practices
- **Allowed Packages:** {', '.join(self.allowed_packages)}
- **Code Quality:** Use type hints, modular structure, and error handling"""
    
    def _build_file_structure_section(self, pipeline_name: str) -> str:
        """Build the file structure requirements section."""
        return f"""## FILE STRUCTURE REQUIREMENTS
- **Main Code:** `../pipelines/{pipeline_name}/{pipeline_name}.py`
- **Requirements:** `../pipelines/{pipeline_name}/requirements.txt`
- **Unit Test:** `../pipelines/{pipeline_name}/{pipeline_name}_test.py`
- **Output Directory:** `../pipelines/{pipeline_name}/output/`
- Ensure this folder exists before writing files"""
    
    def _build_data_handling_section(self) -> str:
        """Build the data handling requirements section."""
        return """## DATA HANDLING REQUIREMENTS
### Input Data Loading
```python
from dotenv import load_dotenv
import os
load_dotenv()
DATA_FOLDER = os.getenv('DATA_FOLDER')
```
- Use `DATA_FOLDER` for all input file paths

### ETL Processing
- **Data Ingestion:** Process ALL available data regardless of records/partitions
- **Date Column:** Ensure DataFrame has 'date' column (add today's date if missing)
- **Parquet Partitioning:** 
  - Prefer year/month grouping to avoid system limits
  - Write without partitioning if too many unique dates"""
    
    def _build_testing_section(self, pipeline_name: str) -> str:
        """Build the testing requirements section."""
        return f"""## TESTING REQUIREMENTS
- **Framework:** pytest
- **Import Pattern:** `from {pipeline_name} import ...`
- **Coverage:** Test main transformation function for correctness"""
    
    def _build_output_format_section(self) -> str:
        """Build the output format section."""
        return """## OUTPUT FORMAT
Return exactly three code blocks in this order:
1. ```python
[main pipeline code]
```
2. ```requirements.txt
[package dependencies]
```
3. ```python test
[unit test code]
```

**Important:** Return ONLY the three code blocks. No explanations or extra text."""
    
    def _build_error_correction_section(self, last_code: str, last_error: str, python_test: str) -> str:
        """Build the error correction section when fixing previous errors."""
        return f"""## ERROR CORRECTION
The previous code execution failed. Please fix the following:

**Error:**
{last_error}

**Previous Code:**
{last_code}

**Previous Test:**
{python_test}

Fix the issues and regenerate all three code blocks."""
    
    def generate_prompt(self, spec: dict, data_preview: dict, 
                       last_code: Optional[str] = None, 
                       last_error: Optional[str] = None, 
                       python_test: Optional[str] = None) -> str:
        """
        Generate the complete prompt for pipeline code generation.
        
        Args:
            spec: Pipeline specification dictionary
            data_preview: Data preview dictionary
            last_code: Previous code that failed (optional)
            last_error: Error message from previous code (optional)
            python_test: Previous test code (optional)
        
        Returns:
            Complete formatted prompt string
        """
        pipeline_name = spec.get("pipeline_name", "pipeline")
        
        sections = [
            self._build_context_section(spec, data_preview),
            self._build_constraints_section(),
            self._build_file_structure_section(pipeline_name),
            self._build_data_handling_section(),
            self._build_testing_section(pipeline_name),
            self._build_output_format_section()
        ]
        
        # Add error correction section if we have error information
        if last_code and last_error:
            sections.append(self._build_error_correction_section(last_code, last_error, python_test))
        
        return "\n\n".join(sections)


