"""
DSPy implementation of Pipeline Code Generator.
Provides a structured, type-safe approach to generating ETL pipeline code.
"""

import dspy
from typing import Dict, Optional, Tuple, List
from app.utils.json_utils import safe_json_dumps

# Define allowed packages for security
ALLOWED_PACKAGES = [
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "python-dotenv>=1.0.0",
    "pyarrow>=14.0.0",
    "pytest>=7.0.0"
]


class PipelineCodeSignature(dspy.Signature):
    """
    DSPy signature for generating complete ETL pipeline code packages.
    Defines structured inputs and outputs for consistent code generation.
    """
    
    # Input fields with detailed descriptions
    pipeline_specification = dspy.InputField(
        desc="Complete pipeline specification dictionary containing transformation requirements, "
             "source/destination details, and processing instructions"
    )
    
    data_preview = dspy.InputField(
        desc="Sample data preview showing structure, column names, data types, and example values "
             "to inform code generation decisions"
    )
    
    constraints = dspy.InputField(
        desc="Technical constraints including Python version, allowed packages, code quality requirements, "
             "and file structure specifications"
    )
    
    error_context = dspy.InputField(
        desc="Previous execution error information for code correction, including error message, "
             "failed code, and test results. Empty if this is the first generation attempt.",
        default=""
    )
    
    # Output fields with structured requirements
    main_pipeline_code = dspy.OutputField(
        desc="Complete Python pipeline code with proper imports, type hints, error handling, "
             "data loading from DATA_FOLDER env var, transformation logic, and output file writing. "
             "Must include all necessary functions and follow Python 3.13 best practices."
    )
    
    requirements_file = dspy.OutputField(
        desc="Complete requirements.txt content listing all necessary Python packages with version "
             "constraints. Must only include packages from the allowed list for security."
    )
    
    unit_test_code = dspy.OutputField(
        desc="Complete pytest unit test code that imports from the main pipeline module and tests "
             "the core transformation function for correctness with sample data validation."
    )


class PipelineCodeReasoningModule(dspy.Module):
    """
    DSPy module that implements chain-of-thought reasoning for pipeline code generation.
    Uses structured prompting to ensure consistent, high-quality outputs.
    """
    
    def __init__(self):
        super().__init__()
        self.code_generator = dspy.ChainOfThought(PipelineCodeSignature)
        self.allowed_packages = ALLOWED_PACKAGES
    
    def forward(self, spec: Dict, data_preview: Dict, 
                last_code: Optional[str] = None, 
                last_error: Optional[str] = None, 
                python_test: Optional[str] = None) -> dspy.Prediction:
        """
        Generate pipeline code using structured DSPy reasoning.
        
        Args:
            spec: Pipeline specification dictionary
            data_preview: Data preview dictionary with sample data
            last_code: Previous code that failed (optional)
            last_error: Error message from previous attempt (optional)
            python_test: Previous test code (optional)
            
        Returns:
            DSPy Prediction with structured code outputs
        """
        pipeline_name = spec.get("pipeline_name", "pipeline")
        
        # Build structured constraint information
        constraints = self._build_constraints(pipeline_name)
        
        # Build error context if we have previous failure
        error_context = ""
        if last_code and last_error:
            error_context = self._build_error_context(last_code, last_error, python_test)
        
        # Execute structured code generation
        return self.code_generator(
            pipeline_specification=safe_json_dumps(spec, indent=2),
            data_preview=safe_json_dumps(data_preview, indent=2),
            constraints=constraints,
            error_context=error_context
        )
    
    def _build_constraints(self, pipeline_name: str) -> str:
        """Build comprehensive constraint specification."""
        return f"""
TECHNICAL CONSTRAINTS:
- Python Version: 3.13 with modern best practices
- Allowed Packages: {', '.join(self.allowed_packages)}
- Code Quality: Type hints, modular structure, comprehensive error handling

FILE STRUCTURE REQUIREMENTS:
- Main Code: ../pipelines/{pipeline_name}/{pipeline_name}.py
- Requirements: ../pipelines/{pipeline_name}/requirements.txt  
- Unit Test: ../pipelines/{pipeline_name}/{pipeline_name}_test.py
- Output Directory: ../pipelines/{pipeline_name}/output/
- Ensure output directory exists before writing files

DATA HANDLING REQUIREMENTS:
- Load input data using DATA_FOLDER environment variable from .env file
- Required environment loading pattern:
  ```python
  from dotenv import load_dotenv
  import os
  load_dotenv()
  DATA_FOLDER = os.getenv('DATA_FOLDER')
  ```
- Process ALL available data regardless of size or partitions
- Ensure DataFrame has 'date' column (add current date if missing)
- Use smart Parquet partitioning (group by year/month to avoid system limits)
- Write without partitioning if too many unique partition values

TESTING REQUIREMENTS:
- Framework: pytest
- Import pattern: from {pipeline_name} import [functions]
- Test main transformation function for correctness
- Validate output data structure and content

OUTPUT FORMAT REQUIREMENTS:
- Return structured outputs only, no explanations
- Each component must be complete and executable
- Follow consistent coding standards throughout
"""
    
    def _build_error_context(self, last_code: str, last_error: str, python_test: str) -> str:
        """Build error context for code correction."""
        return f"""
PREVIOUS EXECUTION FAILED - ANALYSIS AND CORRECTION NEEDED:

Error Message:
{last_error}

Previous Main Code:
{last_code}

Previous Test Code:
{python_test or 'No test code provided'}

CORRECTION REQUIREMENTS:
- Analyze the root cause of the error
- Fix all identified issues in the code
- Ensure the corrected code handles edge cases
- Validate that imports and dependencies are correct
- Test data processing logic for robustness
- Generate improved error handling where needed
"""


class PipelineCodeGeneratorDSPy:
    """
    DSPy-based Pipeline Code Generator providing structured, consistent code generation.
    
    This implementation offers advantages over traditional string-based prompting:
    - Type-safe inputs and outputs
    - Structured reasoning chains
    - Consistent output formatting
    - Built-in error handling and retry logic
    - Modular, composable components
    """
    
    def __init__(self, lm_model=None):
        """
        Initialize DSPy pipeline code generator.
        
        Args:
            lm_model: Optional DSPy language model. If None, uses OpenAI GPT-4.
        """
        import os
        
        try:
            if lm_model:
                dspy.settings.configure(lm=lm_model)
            else:
                # Configure with OpenAI GPT-4 by default
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    raise ValueError("OPENAI_API_KEY environment variable is required for DSPy initialization")
                
                # Configure DSPy with OpenAI using the correct API
                openai_lm = dspy.LM(model="openai/gpt-4", api_key=api_key, max_tokens=4000)
                dspy.settings.configure(lm=openai_lm)
        except Exception as e:
            raise Exception(f"Failed to configure DSPy language model: {str(e)}")
        
        self.code_module = PipelineCodeReasoningModule()
        self.allowed_packages = ALLOWED_PACKAGES
    
    def generate_code(self, spec: Dict, data_preview: Dict, 
                     last_code: Optional[str] = None, 
                     last_error: Optional[str] = None, 
                     python_test: Optional[str] = None) -> Tuple[str, str, str]:
        """
        Generate complete pipeline code package using DSPy structured approach.
        
        Args:
            spec: Pipeline specification dictionary
            data_preview: Data preview dictionary
            last_code: Previous failed code (optional)
            last_error: Previous error message (optional)
            python_test: Previous test code (optional)
            
        Returns:
            Tuple of (main_code, requirements_txt, test_code)
            
        Raises:
            ValueError: If generated requirements contain disallowed packages
            Exception: If DSPy code generation fails
        """
        try:
            # Generate code using DSPy structured reasoning
            prediction = self.code_module(
                spec=spec,
                data_preview=data_preview,
                last_code=last_code,
                last_error=last_error,
                python_test=python_test
            )
            
            # Extract structured outputs
            main_code = prediction.main_pipeline_code
            requirements = prediction.requirements_file
            test_code = prediction.unit_test_code
            
            # Validate generated requirements for security
            if not self.check_requirements(requirements):
                raise ValueError("Generated requirements.txt contains disallowed packages.")
            
            return main_code, requirements, test_code
            
        except Exception as e:
            raise Exception(f"DSPy pipeline code generation failed: {str(e)}")
    
    def check_requirements(self, requirements: str) -> bool:
        """
        Validate generated requirements against allowed packages list.
        
        Args:
            requirements: Generated requirements.txt content
            
        Returns:
            True if all packages are allowed, False otherwise
        """
        if not requirements.strip():
            return False
        
        lines = requirements.strip().split("\n")
        allowed_base_packages = {pkg.split(">=")[0].strip().lower() for pkg in self.allowed_packages}
        
        for line in lines:
            if line.strip() and not line.strip().startswith("#"):
                # Extract package name (handle various version specifiers)
                pkg_name = line.split(">=")[0].split("==")[0].split("~=")[0].strip().lower()
                if pkg_name not in allowed_base_packages:
                    return False
        
        return True
    
    def validate_generated_code(self, main_code: str, requirements: str, test_code: str) -> Dict[str, bool]:
        """
        Validate the generated code components for completeness and correctness.
        
        Args:
            main_code: Generated main pipeline code
            requirements: Generated requirements.txt
            test_code: Generated test code
            
        Returns:
            Dictionary with validation results for each component
        """
        validation = {}
        
        # Validate main code
        validation['has_main_code'] = bool(main_code.strip())
        validation['has_imports'] = 'import' in main_code
        validation['has_env_loading'] = all(x in main_code for x in ['load_dotenv', 'DATA_FOLDER', 'os.getenv'])
        validation['has_functions'] = 'def ' in main_code
        validation['has_type_hints'] = '->' in main_code
        validation['has_error_handling'] = any(x in main_code for x in ['try:', 'except:', 'raise'])
        
        # Validate requirements
        validation['has_requirements'] = bool(requirements.strip())
        validation['has_pandas'] = 'pandas' in requirements
        validation['has_dotenv'] = 'python-dotenv' in requirements
        validation['requirements_allowed'] = self.check_requirements(requirements)
        
        # Validate test code
        validation['has_test_code'] = bool(test_code.strip())
        validation['has_pytest_tests'] = 'def test_' in test_code
        validation['has_imports_in_test'] = 'import' in test_code
        validation['has_from_import'] = 'from ' in test_code
        
        return validation
    
    def get_generation_summary(self, spec: Dict) -> Dict[str, str]:
        """
        Get a summary of what will be generated for the given specification.
        
        Args:
            spec: Pipeline specification dictionary
            
        Returns:
            Dictionary with generation summary information
        """
        pipeline_name = spec.get("pipeline_name", "pipeline")
        
        return {
            'pipeline_name': pipeline_name,
            'main_file': f'../pipelines/{pipeline_name}/{pipeline_name}.py',
            'requirements_file': f'../pipelines/{pipeline_name}/requirements.txt',
            'test_file': f'../pipelines/{pipeline_name}/{pipeline_name}_test.py',
            'output_directory': f'../pipelines/{pipeline_name}/output/',
            'allowed_packages': ', '.join(self.allowed_packages),
            'generation_approach': 'DSPy Structured Reasoning',
            'features': [
                'Type-safe inputs/outputs',
                'Chain-of-thought reasoning',
                'Automatic error correction',
                'Modular code structure',  
                'Comprehensive validation'
            ]
        }


# Configuration helper for easy setup
def setup_dspy_pipeline_generator(model_name: str = "gpt-4", max_tokens: int = 4000) -> PipelineCodeGeneratorDSPy:
    """
    Set up DSPy pipeline generator with specified model configuration.
    
    Args:
        model_name: Language model to use (default: gpt-4)
        max_tokens: Maximum tokens for generation (default: 4000)
        
    Returns:
        Configured PipelineCodeGeneratorDSPy instance
        
    Example:
        generator = setup_dspy_pipeline_generator("gpt-4")
        main_code, requirements, test_code = generator.generate_code(spec, data_preview)
    """
    import os
    
    # Configure DSPy with specified model
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    lm = dspy.LM(model=f"openai/{model_name}", api_key=api_key, max_tokens=max_tokens)
    dspy.settings.configure(lm=lm)
    
    return PipelineCodeGeneratorDSPy()


# Example usage and comparison utilities
class DSPyBenefitsDemo:
    """Utility class to demonstrate DSPy benefits over traditional approach."""
    
    @staticmethod
    def compare_approaches():
        """Compare traditional vs DSPy approaches."""
        return {
            "Traditional Approach": {
                "prompt_building": "Manual string concatenation",
                "type_safety": "No type checking",
                "error_handling": "Manual error recovery",
                "consistency": "Variable output quality",
                "optimization": "Manual prompt tuning",
                "modularity": "Monolithic prompt strings",
                "debugging": "Print statements and inspection"
            },
            "DSPy Approach": {
                "prompt_building": "Structured signatures",
                "type_safety": "Typed inputs and outputs",
                "error_handling": "Built-in error recovery",
                "consistency": "Structured, consistent outputs",
                "optimization": "Automatic prompt optimization",
                "modularity": "Composable modules",
                "debugging": "Built-in tracing and inspection"
            }
        }
    
    @staticmethod
    def get_migration_benefits():
        """List benefits of migrating to DSPy."""
        return [
            "🔒 Type Safety: Structured inputs/outputs prevent errors",
            "🔄 Auto Optimization: DSPy can optimize prompts automatically",
            "🧩 Modularity: Compose complex systems from simple modules",
            "📊 Consistency: Structured outputs with predictable format",
            "🛠 Better Debugging: Built-in tracing and inspection tools",
            "⚡ Error Recovery: Automatic retry and error handling",
            "📈 Scalability: Easier to build and maintain complex systems",
            "🎯 Focused Reasoning: Chain-of-thought for better results"
        ]
