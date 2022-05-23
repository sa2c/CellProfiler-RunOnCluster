import unittest
from unittest.mock import MagicMock as MM
from rynner.pattern_parser import *


class TestPatternParser(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MM()
        self.mock_conn.run_command.return_value = (0, "std out", "std error")
        self.submit_cmd = 'some_submission_cmd'

    def instantiate(self, opt_map=None):
        self.opt_map = MM()
        if opt_map is not None:
            self.opt_map = opt_map

        self.defaults = MM()
        self.pattern_parser = PatternParser(self.opt_map, self.submit_cmd,
                                            self.defaults)

    def assert_parse(self, opt_map, input, output):
        '''
        assert that passing input results in output (expected), for a given opt_map
        '''
        self.instantiate(opt_map)
        context = self.pattern_parser.parse(input)
        self.assertEqual(context['options'], output)
        return context

    def test_instantiation(self):
        self.instantiate()

    def test_pattern_parser_can_call_run(self):
        self.instantiate()
        self.pattern_parser.run(
            self.mock_conn, {
                'options': ['Some Option Result', 'Another Option Result'],
                'script': 'command'
            }, '/some/remote/path')

    def test_pattern_parser_single_string_opt_map_parsed(self):
        opt_map = [
            ('#FAKE --memory={}', 'memory'),
        ]
        input = {'memory': 'MEMORY_VALUE', 'script': 'script'}
        output = [
            '#FAKE --memory=MEMORY_VALUE',
        ]

        self.assert_parse(opt_map, input, output)

    def test_pattern_parser_multiple_string_opt_map_parsed(self):
        opt_map = [
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]
        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'script': 'script'
        }
        output = [
            '#FAKE --memory=MEMORY_VALUE',
            '#FAKE --cpus=CPU_VALUE',
        ]

        self.assert_parse(opt_map, input, output)

    def test_pattern_parser_duplicate_string_matches(self):
        opt_map = [
            ('#FAKE --memory={}', 'memory'),
            ('#SHOULD SKIP ME', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]
        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'script': 'script'
        }
        output = [
            '#FAKE --memory=MEMORY_VALUE',
            '#FAKE --cpus=CPU_VALUE',
        ]

        self.assert_parse(opt_map, input, output)

    def test_pattern_parser_compound_string_matches(self):
        opt_map = [
            ('#FAKE --memory={} --cpus={}', ('memory', 'cpus')),
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]

        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'script': 'script'
        }

        output = ['#FAKE --memory=MEMORY_VALUE --cpus=CPU_VALUE']

        self.assert_parse(opt_map, input, output)

    def test_not_all_map_matched(self):
        opt_map = [
            ('#FAKE --memory={} --cpus={}', ('memory', 'cpus')),
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
            ('#FAKE --cpus={}', 'another_variable'),
        ]

        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'script': 'script'
        }

        output = ['#FAKE --memory=MEMORY_VALUE --cpus=CPU_VALUE']

        self.assert_parse(opt_map, input, output)

    def test_error_if_unmatched_context_keys(self):
        opt_map = [
            ('#FAKE --memory={} --cpus={}', ('memory', 'cpus')),
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]

        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'another-var': 'ERROR!',
            'script': 'script'
        }

        self.instantiate(opt_map)
        with self.assertRaises(InvalidContextOption) as context:
            context = self.pattern_parser.parse(input)

        assert 'invalid option(s): ' in str(context.exception)
        assert 'another-var' in str(context.exception)

    def test_reverse_argument_order(self):
        opt_map = [
            ('#FAKE --memory={} --cpus={}', ('cpus', 'memory')),
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]

        input = {
            'memory': 'MEMORY_VALUE',
            'cpus': 'CPU_VALUE',
            'script': 'script'
        }

        output = ['#FAKE --memory=CPU_VALUE --cpus=MEMORY_VALUE']

        self.assert_parse(opt_map, input, output)

    def test_input_dict_untouched(self):
        input = {'memory': 1, 'cpus': 1, 'script': 'script'}
        input_copy = input.copy()
        opt_map = [
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
        ]
        self.instantiate(opt_map)
        self.pattern_parser.parse(input)
        self.assertEqual(input, input_copy)

    def test_script_returned_seperately(self):
        opt_map = [
            ('#FAKE --memory={}', 'memory'),
            ('#FAKE --cpus={}', 'cpus'),
            ('#FAKE --cpus={}', 'script'),
        ]

        input = {'memory': 'MEMORY_VALUE', 'script': 'my script'}

        output = ['#FAKE --memory=MEMORY_VALUE']

        context = self.assert_parse(opt_map, input, output)
        assert context['script'] == 'my script'

    def test_with_function(self):
        def parsing_function(a, k):
            return {'function-return': [a.copy(), k]}

        opt_map = [
            (parsing_function, ('cpus', 'memory')),
        ]

        mock_mem, mock_cpu, mock_template = (MM(), MM(), MM())

        input = {'memory': mock_mem, 'cpus': mock_cpu, 'script': 'script'}

        output = [{
            'function-return': [{
                'memory': mock_mem,
                'cpus': mock_cpu
            }, ('cpus', 'memory')]
        }]

        context = self.assert_parse(opt_map, input, output)

    @unittest.skip("classes as cluster config not implemented yet")
    def test_with_class(self):
        pass

    def test_run_method_calls_connection(self):
        self.instantiate()
        context = {'options': ['one', 'two', 'three'], 'script': 'four'}
        self.pattern_parser.run(self.mock_conn, context, '/some/remote/path')
        self.mock_conn.put_file_content('one\ntwo\nthree\nfour\n',
                                        '/some/remote/path/jobcard')

    def test_run_calls_submit(self):
        self.instantiate()
        context = {'options': ['one', 'two', 'three'], 'script': 'four'}
        self.pattern_parser.run(self.mock_conn, context, '/some/remote/path'):
        self.mock_conn.run_command.assert_called_once_with(
            'some_submission_cmd', pwd='/some/remote/path')

    @unittest.skip("not impletemented yet by default")
    def test_uses_defaults(self):
        '''
        defaults passed to __init__ should be used if nothing provided
        '''
        pass

    def test_with_boolean_false(self):
        stringy = '#FAKE --flag'
        opt_map = [(stringy, 'flag')]

        input = {'flag': False, 'script': 'mpirun -n 8 ./myprogram'}

        output = []

        self.assert_parse(opt_map, input, output)

    def test_with_boolean_true(self):
        stringy = '#FAKE --flag'
        opt_map = [(stringy, 'flag')]

        input = {'flag': True, 'script': 'mpirun -n 8 ./myprogram'}

        output = [stringy]

        self.assert_parse(opt_map, input, output)


if __name__ == '__main__':
    unittest.main()
