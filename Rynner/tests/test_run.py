import unittest
from unittest.mock import patch, MagicMock, call, ANY
from rynner.run import *


class TestRun(unittest.TestCase):
    def setUp(self):
        self.mock_host = MagicMock()
        self.mock_data = {'host': self.mock_host, 'memory': 100}

        # patch uuid
        self.uuid_patch = patch('rynner.run.uuid')
        uuid = self.uuid_patch.start()
        self.mock_uuid = str(uuid.uuid1())

    def tearDown(self):
        self.uuid_patch.stop()

    def instantiate(self):
        self.plugin_id = 'my-plugin-id'
        self.runner = RunManager(self.plugin_id)
        self.run_id = self.runner.new(**self.mock_data)

    def test_instantiation(self):
        self.instantiate()

    def test_calls_parse_on_host(self):
        self.instantiate()
        assert self.mock_host.parse.called

    def test_filters_host_out_of_data(self):
        self.instantiate()
        data = self.mock_data.copy()
        del data['host']
        self.mock_host.parse.assert_called_once_with(self.plugin_id,
                                                     self.mock_uuid, data)

    def test_filter_uploads(self):
        # prepare data to compare against
        data = self.mock_data.copy()
        del data['host']

        # add uploads to mock data
        self.mock_data['uploads'] = MagicMock()
        self.instantiate()
        self.mock_host.parse.assert_called_once_with(self.plugin_id,
                                                     self.mock_uuid, data)

    def test_raises_host_not_specified(self):
        del self.mock_data['host']
        with self.assertRaises(HostNotSpecifiedException):
            self.instantiate()

    def test_run_knows_id(self):
        self.instantiate()
        self.run_id == self.mock_uuid

    def test_run_uploads(self):
        self.mock_data['uploads'] = MagicMock()
        self.instantiate()
        self.mock_host.upload.assert_called_once_with(
            self.plugin_id, self.mock_uuid, self.mock_data['uploads'])

    def test_upload_not_called_if_no_uploads(self):
        self.instantiate()
        self.assertFalse(self.mock_host.upload.called)

    def test_run_called_with_the_output_of_parse(self):
        self.instantiate()
        context = self.mock_host.parse()
        self.mock_host.run.assert_called_once_with(self.plugin_id, self.run_id,
                                                   context)

    def test_upload_called_before_run(self):
        self.mock_data['uploads'] = MagicMock()
        self.instantiate()
        calls = [
            call.upload(ANY, ANY, ANY),
            call.run(self.plugin_id, self.run_id, ANY)
        ]
        self.mock_host.assert_has_calls(calls)

    def test_converts_integer_classes_to_integers(self):
        class SomeIntegerType:
            def __rynner_value__(self):
                return 1234

        self.mock_data['memory'] = SomeIntegerType()
        self.instantiate()

        # setup return value
        data = self.mock_data.copy()
        data['memory'] = 1234
        del data['host']

        self.mock_host.parse.assert_called_once_with(self.plugin_id,
                                                     self.mock_uuid, data)

    def test_converts_string_classes_to_strings(self):
        class SomeStringType:
            def __rynner_value__(self):
                return "Test String"

        self.mock_data['memory'] = SomeStringType()
        self.instantiate()

        # setup return value
        data = self.mock_data.copy()
        data['memory'] = "Test String"
        del data['host']
        self.mock_host.parse.assert_called_once_with(self.plugin_id,
                                                     self.mock_uuid, data)

    def test_throws_exception_if_object_not_convertable(self):
        class SomeRandomType:
            pass

        self.mock_data['memory'] = SomeRandomType()
        with self.assertRaises(UnconvertableOptionType) as context:
            self.instantiate()

        assert 'no __rynner_value__ method' in str(context.exception)

    def test_throws_exception(self):
        runner = RunManager('my-plugin-id')
        with self.assertRaises(InvalidHostSpecifiedException):
            runner.new(host='InvalidHost')


if __name__ == '__main__':
    unittest.main()
