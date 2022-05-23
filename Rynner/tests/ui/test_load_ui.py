from rynner.ui import load_ui, loader as ui_loader
from PySide2.QtUiTools import QUiLoader
from tests.qtest_helpers import *
from unittest.mock import patch
import sys, os
import unittest


class TestLoadUi(unittest.TestCase):
    @patch('rynner.ui.loader')
    @patch('rynner.ui.QFile')
    def test_ui_loads(self, MockQFile, MockUiLoader):
        window = load_ui('list_view.ui')
        self.assertEqual(window, MockUiLoader.load())

    def test_loader_is_a_QUiLoader(self):
        self.assertIsInstance(ui_loader, QUiLoader)


if __name__ == '__main__':
    unittest.main()
