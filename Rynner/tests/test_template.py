import unittest
import os
from unittest.mock import patch, MagicMock
from rynner.template import *


class TestTemplate(unittest.TestCase):
    def current_file(self):
        return os.path.realpath(__file__)

    def current_file_content(self):
        return open(self.current_file()).read()

    def test_instance(self):
        template = Template('template/path')

    def test_call_file(self):
        template = Template.from_file(self.current_file())

    def test_call_file(self):
        with self.assertRaises(Exception):
            template = Template.from_file('/my/test/file/poaij4oivnr')

    def test_from_file_return_template(self):
        template = Template.from_file(self.current_file())
        assert isinstance(template, Template)

    def test_substitutes_values(self):
        template = Template('this is my {var1}')
        string = template.format({'var1': 'one'})

        self.assertEqual(string, 'this is my one')

    def test_fails_not_enough_options(self):
        template = Template('this is my {var1} and {var2}')

        with self.assertRaises(TemplateArgumentException):
            string = template.format({'var1': 'one'})

    def test_fails_too_many_options(self):
        template = Template('this is my {var1}')

        with self.assertRaises(TemplateArgumentException):
            string = template.format({'var1': 'one', 'var2': 'two'})

    def test_computes_keys(self):
        template = Template('this is my {var1} and {var2}')
        self.assertEqual(template.keys(), {'var1', 'var2'})

    def test_fails_invalid_options(self):
        template = Template('this is my {var1}')

        with self.assertRaises(TemplateArgumentException):
            template.format({'var1one', 'var2two'})
