import unittest
import pytest
from unittest.mock import MagicMock as MM
from rynner.pattern_parser import PatternParser, InvalidContextOption
from rynner.host import Host
from rynner.run import RunManager
from rynner.template import Template
from rynner.datastore import Datastore


class TestPatternParserIntegration(unittest.TestCase):
    def setUp(self):
        self.connection = MM()
        self.connection.run_command.return_value = (0, "std out", "std err")

    def instantiate(self, opt_map=None):
        host_pattern = [
            ('#FAKE --walltime={}', 'walltime'),
            ('#FAKE --num-nodes={}', 'num_nodes'),
        ]
        defaults = MM()
        self.pattern_parser = PatternParser(host_pattern, 'submit_cmd',
                                            defaults)
        self.datastore = Datastore(self.connection)
        self.host = Host(self.pattern_parser, self.connection, self.datastore)

    def create_run(self, **kwargs):
        runner = RunManager('my-plugin-id', {})
        return runner.new(**kwargs)

    def test_instantiation(self):
        self.instantiate()

    def test_instantiate_run(self):
        self.instantiate()
        run = self.create_run(
            host=self.host, walltime='10:0:00', script='my_script')

    def test_instantiate_run_with_walltime(self):
        self.instantiate()
        self.host.run = MM()
        id = self.create_run(
            host=self.host,
            walltime='10:0:00',
            num_nodes=10,
            script='this is my script')
        context = {
            'options': ['#FAKE --walltime=10:0:00', '#FAKE --num-nodes=10'],
            'script': 'this is my script'
        }
        self.host.run.assert_called_once_with(id, context)

    @pytest.mark.xfail(
        reason=
        'changes in context contents mean that there are additional key in arguments to host.run'
    )
    def test_instantiate_run_with_walltime(self):
        self.instantiate()
        self.host.run = MM()
        id = self.create_run(
            host=self.host,
            walltime='10:0:00',
            num_nodes=10,
            script=Template('this is my {var}').format({
                'var': 'script'
            }))
        context = {
            'options': ['#FAKE --walltime=10:0:00', '#FAKE --num-nodes=10'],
            'script': 'this is my script'
        }
        self.host.run.assert_called_once_with('my-plugin-id', id, context)

    def test_throw_exception_on_invalid_option(self):
        self.instantiate()
        self.host.run = MM()
        with self.assertRaises(InvalidContextOption):
            run = self.create_run(
                # script, uploads and downloads are "special" and are handled differently
                host=self.host,
                script='this is my script',
                walltime='10:0:00',
                num_nodes=10,
                invalid=5,
            )

    def test_upload_incorrect_formats(self):
        self.instantiate()
        self.host.run = MM()
        with self.assertRaises(InvalidContextOption):
            run = self.create_run(
                host=self.host, script='my_script', uploads=['throw an error'])

    def test_uploads_correct_format(self):
        self.instantiate()
        self.host.run = MM()
        run = self.create_run(
            host=self.host, script='my_script', uploads=[('local', 'remote')])
        self.connection.put_file.assert_called_once_with('local', 'remote')

    @unittest.skip("running not implemented yet")
    def test_files_run(self):
        pass

    @unittest.skip("datastore not implemented yet")
    def test_datastore(self):
        pass

    @unittest.skip("not implemented yet")
    def test_download_files(self):
        pass


if __name__ == '__main__':
    unittest.main()
