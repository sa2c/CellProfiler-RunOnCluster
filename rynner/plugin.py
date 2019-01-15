from rynner.run import RunManager
from PySide2.QtCore import Signal, QObject
from rynner.create_view import RunCreateView
from box import Box


class RunAction:
    def __init__(self, label, function):
        self.label = label
        self.function = function


class Plugin(QObject):
    '''
    The runner object (function) is the thing that is responsible for running
    the job, usually by creating a Run object.
    Links the GUI/UI and the 'run' logic.
    (see design.org example)


   '''

    build_index_view = None

    view_keys = []

    runs_changed = Signal()

    def __init__(self,
                 plugin_id,
                 name,
                 create_view=None,
                 runner=None,
                 view_keys=None,
                 labels=None,
                 build_index_view=None,
                 parent=None):
        '''
        Parameters
        ----------
        `plugin_id`: str
           A string giving a globally unique name for the plugin. Clients on
           different machines will use this name to associate jobs with a given
           Plugin class. The recommended appraoach is to use a web URL (such as
           a github repository URL) which is unique for this plugin. This
           string is never displayed in the UI by default.
        `name`: str
           A string giving the human readable Plugin name. This is the string
           which is displayed to the user in the UI to identify the runs of
           this plugin.
        `create_view` : RunCreateView / iterable of BaseField
           Defines the view used by the application user to configure a job,
           and the mapping of that configuration to a set of options which will
           be passed to Run.
        `runner`: callable
           A function which will be called to run a job. Typically this
           function will instantiate one or more objects of type Run. The input
           to the method will be a dictionary in which the keys correspond to
           the 'key' properties of the visible UI objects in the . See the
           RynCreateView class documentation for details of keys. If not
           specified, all keys of the RynCreateView object will be passed as
           keyword arguments to instantiate a single Run object. In this case,
           the keys of the children of the RynCreateView should correspond
           directly to keyword arguments of Run.
        `view_keys`: iterable of strings
           A list of keys to show in the (default) main/index view
        `labels`: dict ,optional
           Dictionary giving a human readable name for each label. If not
           specified, the values of the key of each entry in  is used.
        `view`: callable
           A callable that when called returns a QWidget object (this should be
           a QWidget class or a function which returns a QWidget instance). If
           not set, RynCreateView will be used. view keyword argument which can
           be used to override the default main/index view to render for a
           Plugin.

        '''
        super().__init__(parent)
        self.name = name
        self.plugin_id = plugin_id
        self.create_view = create_view
        self.actions = []
        self.runner = runner
        self.labels = labels

        if build_index_view is not None:
            self.build_index_view = build_index_view

        if view_keys is not None:
            self.view_keys = view_keys

        if isinstance(view_keys, str):
            raise ValueError(
                'view_keys kwarg tuple or list expected, not string')

        if not isinstance(self.create_view, RunCreateView):
            self.create_view = RunCreateView(self.create_view)

        if create_view is not None:
            self.create_view.accepted.connect(self.config_accepted)

    def _run(self, config):
        run_manager = RunManager(self.plugin_id, config)
        if self.runner is None:
            run = run_manager.new(**config)
        else:
            self.runner(run_manager, config)
        run_manager.store()

    def add_action(self, label, function):
        action = RunAction(label, function)
        self.actions.append(action)
        return action

    def create(self):
        if self.create_view is None:
            self._run({})
        else:
            # display configuration window
            self.create_view.show()

    def config_accepted(self):
        if len(self.create_view.invalid()) == 0:
            data = Box(self.create_view.data())
            self._run(data)
        else:
            raise Exception()

    def stop_run(self, run_data):
        raise NotImplementedError()

    def manages(self, plugin_id):
        '''
        Checks if plugin_id is the id of this plugin instance.

        Implemented to maintain compatibility with PluginCollection.
        '''
        return plugin_id == self.plugin_id


class PluginCollection(QObject):
    '''
    This class allows a collection of Plugin objects to be used with the same API as a single object.
    '''

    build_index_view = None

    runs_changed = Signal()

    def __init__(self, name, plugins, view_keys=None, labels=None,
                 parent=None):
        super().__init__(parent)
        self.name = name
        self.plugins = plugins
        if view_keys is None:
            self.view_keys = Plugin.view_keys
        else:
            self.view_keys = view_keys

        self.actions = []

        self.labels = labels
        self.create_view = None

    def create(self):
        raise NotImplementedError()

    def manages(self, plugin_id):
        '''
        Returns True if this plugin is managed by PluginCollection and False otherwise
        '''
        for plugin in self.plugins:
            if plugin_id == plugin.plugin_id:
                return True

        return False
