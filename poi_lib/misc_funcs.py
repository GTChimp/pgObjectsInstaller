import sys
from os import path
from pathlib import Path


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', str(Path(__file__).parents[1]))
    return path.join(base_path, relative_path)
