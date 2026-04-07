# src/usdeathspy/__init__.py
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("usdeathspy")
except PackageNotFoundError:
    __version__ = "0.1.0"

from .api import load_data
from .parser import parse_cdc_data  # Add this

__all__ = ["load_data", "parse_cdc_data"]