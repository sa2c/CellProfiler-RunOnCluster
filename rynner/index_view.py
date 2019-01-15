from PySide2.QtCore import Qt
from PySide2.QtGui import QStandardItemModel, QStandardItem
from rynner.plugin import RunAction


class InvalidModelIndex(Exception):
    pass


class RunListModel(QStandardItemModel):
    '''
    Public API:
    self.plugin : the plugin instance to display
    self.view_keys : the keys from the plugin to display
    '''

    def __init__(self, plugin, parent=None):
        super().__init__(parent)
        # TODO should handle a plugin which has no "view_keys" property
        self.plugin = plugin

        # Add header labels, leaving the first for "ID"
        headers = ["ID"]
        headers.extend([label for label, index in plugin.view_keys])

        self.setHorizontalHeaderLabels(headers)

        self._visible_runs = []

        self.plugin.runs_changed.connect(self.update_runs)

    def update_runs(self, all_runs):
        '''
        Update the run data according to data made available in all_runs. Note that currently the order is arbitrarily.
        Some sorting method should be implemented here.
        '''

        relevant_runs = {}

        for pid, runs in all_runs.items():
            if self.plugin.manages(pid):
                relevant_runs.update(all_runs[pid])

        new_run_ids = list(set(relevant_runs.keys()) - set(self._visible_runs))

        # show the runs
        row = len(self._visible_runs)
        for run_id in new_run_ids:
            run_data = relevant_runs[run_id]

            # set first column to id
            value = run_id[0:8]
            item = QStandardItem(value)
            self.setItem(row, 0, item)
            item.setData(run_data, Qt.UserRole)

            # set remaining
            for col, key_string in enumerate(self.plugin.view_keys):
                try:
                    keys = key_string[1].split('.')
                    value = run_data[keys[0]]
                    for key in keys[1:]:
                        value = value[key]
                except KeyError:
                    value = 'N/A'
                item = QStandardItem(value)
                self.setItem(row, col + 1, item)

            row = row + 1

        self._visible_runs.extend(new_run_ids)

    def create_new_run(self):
        self.plugin.create()

    def stop_run(self, model_indicies):
        run_data = self._run_id_from_model_index(model_indicies)
        self.plugin.stop_run(run_data)

    def run_action(self, action, model_indicies):
        run_data = self._run_id_from_model_index(model_indicies)
        action.run(run_data)

    def _run_id_from_model_index(self, model_indicies):
        data = []
        for index in model_indicies:
            if not index.isValid():
                raise InvalidModelIndex(
                    f'model index not found {model_indicies}')

            data_index = self.index(index.row(), 0)
            data.append(self.data(data_index, Qt.UserRole))
        return data
