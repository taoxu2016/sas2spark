# src/sas2spark/__init__.py
"""
sas2spark: A package mimicking common SAS procedures in Python for Spark/Pandas.
"""
__version__ = "0.1.0" # Define package version

# Import key functions to make them available directly
# e.g., from sas2spark import proc_freq
from .procs import proc_freq

# Optionally control what `from sas2spark import *` imports
__all__ = ['proc_freq']