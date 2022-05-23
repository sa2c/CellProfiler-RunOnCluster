import os
import types
from PySide2.QtUiTools import QUiLoader
from PySide2.QtCore import QFile

loader = QUiLoader()


def load_ui(filename):
    filepath = os.path.join(os.path.dirname(__file__), filename)

    file = QFile(filepath)
    file.open(QFile.ReadOnly)

    window = loader.load(file)

    return window


def build_config_view(filepath, extract_data):
    file = QFile(filepath)
    file.open(QFile.ReadOnly)

    loader = QUiLoader()
    window = loader.load(file)
    window.data = types.MethodType(extract_data, window)

    return window
