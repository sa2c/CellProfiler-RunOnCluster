import unittest
from PySide2.QtCore import *
from PySide2.QtGui import *
from unittest.mock import MagicMock as MM, patch
from rynner.index_view import *
from rynner.plugin import *


class TestRunListModel(unittest.TestCase):
    def setUp(self):
        self.plugin = Plugin('some-domain', 'plugin name')
        self.jobs = [{
            'id': 'My ID',
            'name': 'My Name'
        }, {
            'id': 'Another ID',
            'name': 'Another Name'
        }]
        self.keys = ['id', 'name']

        self.plugin.view_keys = self.keys
        self.plugin.list_jobs = lambda: self.jobs

    def instance(self):
        return RunListModel(self.plugin)

    def test_instance(self):
        self.instance()

    def test_contains_plugin(self):
        i = self.instance()
        self.assertEqual(i.plugin, self.plugin)

    def test_horizontal_headers(self):
        i = self.instance()
        labels = (i.headerData(j, Qt.Orientation.Horizontal)
                  for j in range(i.columnCount()))
        labels = list(labels)
        self.assertEqual(labels, self.plugin.view_keys)

    def test_connects_plugin_to_update_jobs(self):
        self.plugin = MM()
        i = self.instance()
        i.plugin.runs_changed.connect.assert_called_once_with(i.update_jobs)

    def test_update_jobs_updates_content(self):
        i = self.instance()

        # expected table sizes
        num_cols = len(self.keys)
        num_rows = len(self.jobs)

        # model empty before
        self.assertEqual(i.columnCount(), num_cols)
        self.assertEqual(i.rowCount(), num_rows)

        added_job = {'id': 'added job', 'name': 'added job name'}
        self.jobs.append(added_job)

        i.update_jobs()

        # check that jobs are added
        self.assertEqual(i.columnCount(), num_cols)
        self.assertEqual(i.rowCount(), num_rows + 1)

        self.assertEqual(i.item(num_rows, 0).data(Qt.UserRole), added_job)

    def test_model_contains_data_on_init(self):
        i = self.instance()

        # expected table sizes
        num_cols = len(self.keys)
        num_rows = len(self.jobs)

        # model empty before
        self.assertEqual(i.columnCount(), num_cols)
        self.assertEqual(i.rowCount(), num_rows)

    def test_connects_plugin_to_update_jobs(self):
        i = self.instance()

        # expected table sizes
        num_cols = len(self.keys)
        num_rows = len(self.jobs)

        i.update_jobs()

        for row in range(num_rows):
            for col in range(num_cols):
                job = self.jobs[row]
                key = self.keys[col]
                item = i.item(row, col)
                self.assertEqual(item.text(), job[key])

    def test_has_slots(self):
        i = self.instance()
        slots = ['create_new_run', 'stop_run', 'run_action']
        for slot in slots:
            self.assertIn(slot, dir(i))
            assert callable(getattr(i, slot))

    def test_slots_raise_exception_with_invalid_model_index(self):
        i = self.instance()
        action = MM()
        model_index = i.index(-1, -1)

        slots = [('stop_run', [model_index]), ('run_action', action,
                                               [model_index])]

        for slot in slots:
            attr = getattr(i, slot[0])
            with self.assertRaises(InvalidModelIndex) as exception:
                attr(*slot[1:])

    def test_creates_run(self):
        self.plugin = MM()
        i = self.instance()
        i.create_new_run()
        self.plugin.create.assert_called_once()

    @unittest.skip('Expected Failure: run stopping not implemented')
    def test_stops_run(self):
        i = self.instance()
        i.update_jobs()
        for index, job in enumerate(self.jobs):
            model_index = i.index(index, index)
            i.stop_run(model_index)
            self.plugin.stop_run.assert_called_once_with(1)

    def test_run_action(self):
        i = self.instance()
        i.update_jobs()
        for index, job in enumerate(self.jobs):
            action = MM()
            # move across diagonal of indexing, to ensure the
            # item data always returned correctly
            model_index = i.index(index, index)
            i.run_action(action, [model_index])
            action.run.assert_called_once_with([job])

    def test_gets_jobs_from_list_jobs(self):
        self.plugin = MM()
        i = self.instance()
        self.plugin.list_jobs.assert_called_once_with()

    def test_gets_jobs_from_list_jobs(self):
        from unittest.mock import call, Mock
        self.plugin = MM()
        i = self.instance()
        i.update_jobs()

        self.assertEqual(self.plugin.mock_calls.count(call.list_jobs()), 2)


if __name__ == '__main__':
    unittest.main()
