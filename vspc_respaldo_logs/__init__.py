""" Init module """
from .respaldo_logs import Respaldo_logs
from .save_log_file import SaveLogFile
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
