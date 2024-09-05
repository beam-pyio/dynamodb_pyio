# read version from installed package
from importlib.metadata import version
__version__ = version("dynamodb_pyio")