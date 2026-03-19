# read version from installed package
from importlib.metadata import version
__version__ = version("usdeathspy")

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("usdeathspy")
except PackageNotFoundError:
    __version__ = "0.1.0"

from .api import load_data

__all__ = ["load_data"]