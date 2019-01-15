from abc import ABC, abstractmethod
from PySide2.QtWidgets import QWidget, QVBoxLayout, QTableView, QTableWidgetItem, QDialog, QAbstractItemView, QTabWidget, QPushButton, QHBoxLayout, QAbstractItemView, QComboBox, QLabel, QSpacerItem, QSizePolicy, QItemDelegate, QMainWindow
import collections
from PySide2.QtCore import QAbstractTableModel, Qt, QObject, Signal
from PySide2.QtGui import QStandardItemModel, QStandardItem
from PySide2.QtQuick import QQuickView
from PySide2.QtCore import QUrl, Slot
from rynner.plugin import Plugin, RunAction
from rynner.index_view import RunListModel
from rynner.ui import load_ui

# TODO - no unit testing on this file (but some integration tests)


class InvalidDuplicateWidget(Exception):
    pass


class MainView(QMainWindow):
    '''
    Periodically, for each host, we should fetch a job list of the data visible
    (e.g. from the datastore of the job) for all jobs of the currently visible
    type. Jobs of a given type can be deduced by using their entry in Plugin
    (i.e. something like a URL)

    Upshot -> Plugin needs a URL + a label
    jobs = [ host.get_jobs(type=plugin.uid) for host in hosts ].flatten()
    '''

    def __init__(self, hosts, plugins):
        super().__init__(None)

        self._check_for_duplicate_widgets(plugins)

        self.plugins = plugins
        for plugin in plugins:
            plugin.hosts = hosts

        self.tabs = QTabWidget()
        self.resize(800, 600)

        # Add a new tab for each run type
        self.models = {}
        for plugin in plugins:
            self.models[plugin] = RunListModel(plugin)
            if plugin.build_index_view is not None:
                view = plugin.build_index_view(self.models[plugin])
            else:
                # TODO - passing plugin in here is dubious at best...
                # maybe pass be a proxy object with a bunch of slots?
                view = build_index_view(self.models[plugin], 'list_view.ui')

            self.tabs.addTab(view, plugin.name)

        self.setCentralWidget(self.tabs)
        self.setContentsMargins(10, 10, 10, 10)

        for model in self.models.values():
            for host in hosts:
                host.runs_updated.connect(model.update_runs)

    def _check_for_duplicate_widgets(self, plugins):
        all_widgets = set()

        for plugin in plugins:
            if plugin.create_view is not None:
                # maintain a global database of used widgets (to prevent second use)
                plugin_widgets = {
                    id(child)
                    for child in plugin.create_view.findChildren(QObject)
                }

                intersection = plugin_widgets.intersection(all_widgets)

                ndups = len(intersection)
                if ndups > 0:
                    raise InvalidDuplicateWidget(
                        f'{ndups} duplicate QWidgets found in different plugin instances'
                    )
                all_widgets = plugin_widgets | all_widgets


# takes in the table model and returns a view widget! with links signals?
# contains plugin so that signals can be linked?? Maybe I need a run type proxy??
def build_index_view(model, ui_file):
    # could potentially also just put it in the right place??
    view = load_ui(ui_file)

    # create the a table view and model

    view.table.setModel(model)

    # can probably set these in view
    view.table.setSelectionBehavior(QAbstractItemView.SelectRows)
    view.table.setEditTriggers(QAbstractItemView.NoEditTriggers)

    def stop_run():
        model.stop_run(view.table.selectionModel().selectedRows())

    view.newButton.clicked.connect(model.create_new_run)
    view.stopButton.clicked.connect(stop_run)

    def set_action(action):
        job_idx = view.table.currentIndex()
        if action > 0:
            model.run_action(action,
                             view.table.selectionModel().selectedRows())
        view.actionComboBox.setCurrentIndex(0)

    view.actionComboBox.currentIndexChanged.connect(set_action)
    # TODO - also need to set up view here

    return view
