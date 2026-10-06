import importlib
from typing import Any


def load_test_cases(file_name: str, variable: str = None) -> Any:
    """
    Generic Python test data loader (similar to JSON loader)

    :param file_name: Python file name inside etl_test/testdata
                      e.g. "tpm_planning_fact.py"
    :param variable: variable to fetch (optional)
    :return: data (list/dict)
    """

    # Convert file path → module path
    module_name = f"etl_test.test_data.{file_name.replace('.py', '')}"

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        raise FileNotFoundError(f"Test data module not found: {module_name}")

    # If variable specified → return that
    if variable:
        if not hasattr(module, variable):
            raise KeyError(f"{variable} not found in {module_name}")
        return getattr(module, variable)

    # Default: return full module
    return module