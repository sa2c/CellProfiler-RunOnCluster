import unittest
from unittest.mock import MagicMock as MM, patch
from rynner.plugin import Plugin, RunAction, PluginCollection
from PySide2.QtCore import Signal
from PySide2.QtWidgets import QWidget
from tests.qtest_helpers import *
import pytest


class TestPlugin(unittest.TestCase):
    def setUp(self):
        self.runner = MM()
        self.create_view = MM()
        self.data = {'a': 'a', 'b': 'b'}

        # set up some mock hosts
        self.host1 = MM()
        self.host1.jobs.return_value = ['host1-job1', 'host1-job2']
        self.host2 = MM()
        self.host2.jobs.return_value = ['host2-job1', 'host2-job2']
        self.hosts = [self.host1, self.host2]

        self.domain = 'rynner.swansea.ac.uk'
        self.name = 'Test Plugin'

    def instance(self, **kwargs):
        self.create_view.data.return_value = self.data
        self.create_view.invalid.return_value = []

        self.plugin = Plugin(self.domain, self.name, **kwargs)

    def test_instance(self):
        self.instance()

    @pytest.mark.xfail(
        reason='plugin.create is now non-blocking: need to refactor tests')
    def test_create_calls_setup(self):
        self.instance(runner=self.runner, create_view=self.create_view)
        self.plugin.create()
        self.runner.assert_called_once_with(self.data)

    def test_create_view_show_method_called(self):
        self.instance(create_view=self.create_view, runner=self.runner)
        self.assertFalse(self.create_view.show.called)
        self.plugin.create()
        self.assertTrue(self.create_view.show.called)

    def test_call_runner_if_no_instance(self):
        self.instance(runner=self.runner, create_view=None)
        self.plugin.create()
        self.assertFalse(self.create_view.exec_.called)
        self.assertTrue(self.runner.called)

    def test_can_add_action(self):
        some_action = lambda data: None

        self.instance()
        action = self.plugin.add_action('action label', some_action)
        self.assertIn(action, self.plugin.actions)

    def test_action_is_instance_of_action_class(self):
        some_action = lambda data: None

        self.instance()
        action = self.plugin.add_action('action label', some_action)
        actions = self.plugin.actions
        self.assertIs(type(actions[0]), RunAction)

    def test_action_created_with_pattern_parser_and_label(self):
        self.instance()

        action_function = MM()
        action_label = MM()

        action = self.plugin.add_action(action_label, action_function)
        self.assertEqual(action.label, action_label)
        self.assertEqual(action.function, action_function)

    def test_create_can_handle_empty_data(self):
        self.instance(runner=self.runner)
        self.data = {}
        self.plugin.create()

    def test_call_runner_if_create_view_valid_and_accepted(self):
        # runner is called on create when create_view is valid
        self.instance(runner=self.runner)
        self.create_view.exec_.return_value = True
        self.create_view.invalid.return_value = []
        self.plugin.create()
        self.assertTrue(self.runner.called)

    @pytest.mark.xfail(reason='plugin.create is non-blocking now')
    def test_doesnt_call_runner_if_create_view_is_invalid(self):
        # runner is not called when invalid
        self.instance(runner=self.runner, create_view=self.create_view)
        self.create_view.exec_.return_value = True
        self.create_view.invalid.return_value = ['a', 'b']
        self.plugin.create()
        self.assertTrue(self.create_view.invalid.called)
        self.assertFalse(self.runner.called)

    def test_hosts_none_by_default(self):
        self.instance()
        self.assertEqual(self.plugin.hosts, [])

    def test_doesnt_call_runner_if_exec_cancelled(self):
        # runner is not called when invalid
        self.instance(runner=self.runner, create_view=self.create_view)
        self.create_view.invalid.return_value = []
        self.create_view.exec_.return_value = False
        self.plugin.create()
        self.assertTrue(self.create_view.show.called)
        self.assertFalse(self.runner.called)

    @pytest.mark.xfail(reason='plugin.create is now non-blocking')
    @patch('rynner.plugin.Run')
    def test_doesnt_call_runner_default(self, MockRun):
        plugin = Plugin(self.domain, self.name, self.create_view)

        self.create_view.invalid.return_value = []
        self.create_view.exec_.return_value = True
        self.create_view.data.return_value = {'my': 'test', 'data': 'dict'}
        plugin.create()

        MockRun.assert_called_once_with(my='test', data='dict')

    def test_set_view_keys_stored(self):
        view_keys = MM()
        plugin = Plugin(
            self.domain, self.name, self.create_view, view_keys=view_keys)
        self.assertEqual(view_keys, plugin.view_keys)

    def test_set_view_keys_default(self):
        plugin = Plugin(self.domain, self.name, self.create_view)
        self.assertEqual(plugin.view_keys, Plugin.view_keys)

    def test_assert_default_view_keys_values(self):
        self.assertEqual(Plugin.view_keys, (
            "id",
            "name",
        ))

    def test_default_param_values(self):
        plugin = Plugin(self.domain, self.name, self.create_view)
        self.assertEqual(plugin.view_keys, ("id", "name"))

    def test_add_list_jobs(self):
        self.instance()
        ret = self.plugin.list_jobs([])
        self.assertEqual(ret, [])

    def test_add_list_jobs(self):
        self.instance()
        self.plugin.hosts = [self.host1]
        ret = self.plugin.list_jobs()
        self.assertEqual(ret, ['host1-job1', 'host1-job2'])

    def test_add_list_jobs_multi_hosts(self):
        self.instance()
        self.plugin.hosts = self.hosts
        ret = self.plugin.list_jobs()
        self.assertEqual(
            ret, ['host1-job1', 'host1-job2', 'host2-job1', 'host2-job2'])

    def test_calls_host_jobs_with_domain(self):
        self.instance()
        self.plugin.hosts = self.hosts
        self.plugin.list_jobs()
        self.host1.jobs.assert_called_once_with(self.domain)
        self.host2.jobs.assert_called_once_with(self.domain)

    def test_add_labels(self):
        labels = MM()
        plugin = Plugin(self.domain, self.name, labels=labels)
        self.assertEqual(plugin.labels, labels)

    def test_add_labels(self):
        labels = MM()
        plugin = Plugin(self.domain, self.name)
        self.assertEqual(plugin.labels, None)

    def test_build_index_view(self):
        view_class = MM()
        self.instance(build_index_view=view_class)
        self.assertEqual(self.plugin.build_index_view, view_class)

    def test_build_index_view_default(self):
        self.instance()
        self.assertEqual(self.plugin.build_index_view, None)

    def test_create_view_is_stored(self):
        create_view = MM()
        self.instance(create_view=create_view)
        self.assertEqual(self.plugin.create_view, create_view)

    def test_create_view_none_by_default(self):
        self.instance()
        self.assertEqual(self.plugin.create_view, None)

    def test_has_runs_changed_signal(self):
        self.instance()
        self.assertIsInstance(self.plugin.runs_changed, Signal)

    def test_plugin_signal_connectable(self):
        # note: signal cannot be connected to slot if QObject init not called
        a = Plugin('name', [])
        a.runs_changed.connect(lambda x: None)

    def test_plugin_set_parent(self):
        parent = QWidget()
        a = Plugin('name', [], parent=parent)
        self.assertEqual(a.parent(), parent)

    def test_none_parent_by_default(self):
        parent = QWidget()
        a = Plugin('name', [])
        self.assertEqual(a.parent(), None)

    def test_create_view_signal_connected(self):
        self.instance(create_view=self.create_view)
        self.create_view.accepted.connect.assert_called_once_with(
            self.plugin.config_accepted)


class TestPluginCollection(unittest.TestCase):
    def setUp(self):
        self.name = 'Test Collection Name'
        self.plugins = [MM(), MM()]

    def instance(self, **kwargs):
        self.rc = PluginCollection(self.name, self.plugins, **kwargs)

    def test_instance(self):
        self.instance()

    def test_has_name_attr(self):
        self.instance()
        self.assertEqual(self.rc.name, self.name)

    def test_has_plugins(self):
        self.instance()
        self.assertEqual(self.rc.plugins, self.plugins)

    def test_has_view_keys_with_default(self):
        self.instance()
        self.assertEqual(self.rc.view_keys, Plugin.view_keys)

    def test_has_view_keys_as_specified(self):
        view_keys = MM()
        self.instance(view_keys=view_keys)
        self.assertEqual(self.rc.view_keys, view_keys)

    def test_add_labels(self):
        labels = MM()
        plugin = PluginCollection(self.name, self.plugins, labels=labels)
        self.assertEqual(plugin.labels, labels)

    def test_labels_none_by_default(self):
        plugin = PluginCollection(self.name, self.plugins)
        self.assertEqual(plugin.labels, None)

    def test_create_view_none_by_default(self):
        self.instance()
        self.assertEqual(self.rc.create_view, None)

    def test_has_build_index_view_none(self):
        self.instance()
        self.assertEqual(self.rc.build_index_view, None)

    def test_has_runs_changed_signal(self):
        self.instance()
        self.assertIsInstance(self.rc.runs_changed, Signal)

    def test_plugin_collection_signal_connectable(self):
        # note: signal cannot be connected to slot if QObject init not called
        a = PluginCollection('name', [])
        a.runs_changed.connect(lambda x: None)

    def test_plugin_collection_set_parent(self):
        parent = QWidget()
        a = PluginCollection('name', [], parent=parent)
        self.assertEqual(a.parent(), parent)

    def test_none_parent_by_default(self):
        parent = QWidget()
        a = PluginCollection('name', [])
        self.assertEqual(a.parent(), None)

    def test_hosts_of_a_plugin_empty_by_default(self):
        a = PluginCollection('name', [])
        self.assertEqual(a.hosts, [])

    def test_list_jobs(self):
        self.instance()

        # build hosts
        host1 = MM()
        host1.jobs.return_value = ['host1-job1', 'host1-job2']
        host2 = MM()
        host2.jobs.return_value = ['host2-job2', 'host2-job2']
        self.rc.hosts = [host1, host2]

        # list hosts
        ret = self.rc.list_jobs()

        # list repeated twice (once for each Plugin)
        # with all host1 first and host2 second
        jobs = [
            'host1-job1', 'host1-job2', 'host1-job1', 'host1-job2',
            'host2-job2', 'host2-job2', 'host2-job2', 'host2-job2'
        ]

        # check calls to host.jobs
        self.assertEqual(ret, jobs)

        # check for host1
        for host in [host1, host2]:
            call_arg_list = host.jobs.call_args_list
            call_type1, call_type2 = call_arg_list

            args, vals = call_type1
            self.assertEqual(args, (self.plugins[0].domain, ))

            args, vals = call_type2
            self.assertEqual(args, (self.plugins[1].domain, ))
