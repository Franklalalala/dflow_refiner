"""
refiner
Automated refinement procedure integrating multi-calculators for high-throughput molecular screening.
"""
import sys
if sys.version_info < (3, 10):
    from importlib_metadata import version
else:
    from importlib.metadata import version
from .hello import Hello

__all__ = ['Hello']
