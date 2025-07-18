import sys
import configparser

import pathlib
from appdirs import user_data_dir

import pylablogger

if sys.version_info[:2] >= (3, 8):
    # TODO: Import directly (no need for conditional) when `python_requires = >= 3.8`
    from importlib.metadata import PackageNotFoundError, version  # pragma: no cover
else:
    from importlib_metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
    dist_author = "SG"

config = configparser.ConfigParser(defaults={})
config_path = pathlib.Path(user_data_dir(pylablogger.dist_name, pylablogger.dist_author))