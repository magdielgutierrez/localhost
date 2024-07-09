""" Init module """
from .config_utils import Config_utils
from .parametrizador import Parametrizador
from ._version  import get_versions
__version__ = get_versions()['version']
del get_versions
