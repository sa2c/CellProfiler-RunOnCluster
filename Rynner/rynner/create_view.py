from abc import ABC, abstractmethod
from PySide2.QtWidgets import QLabel, QWidget, QLineEdit, QVBoxLayout, \
    QFormLayout, QDialog, QDialogButtonBox, QGroupBox, QCheckBox, QComboBox


class DuplicateKeyException(Exception):
    pass


class RunCreateView(QDialog):
    def __init__(self, fields, title="Set up run"):
        super().__init__(None)

        # store fields in instance
        self.fields = fields

        # check for duplicate keys
        seen = set()
        for field in fields:
            keys = field.key

            if isinstance(keys, str):
                keys = [keys]

            for key in keys:
                if key in seen:
                    # raise an error if a duplicate key found
                    raise DuplicateKeyException(f"duplicate entries for "
                                                f"key '{field.key}'")
                else:
                    # collect seen keys, to check for duplicates
                    seen.add(key)

        # layout fields
        container = QWidget()
        container.setLayout(QFormLayout())
        for field in self.fields:
            container.layout().addRow(field.label, field.widget)

        self.setWindowTitle(title)
        self.setLayout(QVBoxLayout())
        self.layout().addWidget(container)
        self._button_box = QDialogButtonBox(QDialogButtonBox.Ok
                                            | QDialogButtonBox.Cancel)
        self._button_box.accepted.connect(self.accept)
        self._button_box.rejected.connect(self.reject)
        self.layout().addWidget(self._button_box)

    def show(self):
        # reset field values
        [field.init() for field in self.fields]
        super().show()

    def data(self):
        return {field.key: field.value() for field in self.fields}

    def invalid(self):
        return [field for field in self.fields if not field.valid()]


class BaseField(ABC):
    """A base class for input fields.

    Object of this class interface themself with Rynner through a key
    (or set of keys). The value(s) of the fields can be retrieved through the
    value()
    method.
    """

    def __init__(self, key, label, default=None, remember=True):
        """
        `key` : str
           The key in the corresponding dictionary

        `widget` : QWidget
           The underlying Qt widget (which contains a layout and the set of
           checkboxes.)
        """

        self.__default_value = default
        self.__remember = remember

        # public attributes
        self.key = key
        self.label = QLabel(label)
        self.widget = self._widget()

        # initialise with default value
        self.set_value(self.__default_value)

    @abstractmethod
    def _widget(self):
        pass

    @abstractmethod
    def value(self):
        pass

    @abstractmethod
    def set_value(self, value):
        pass

    def init(self):
        if not self.__remember:
            self.set_value(self.__default_value)

    def cli(self):
        return input(self.label.text())

    def valid(self):
        return True


class TextField(BaseField):
    """A simple Text field.
    """

    def _widget(self):
        return QLineEdit()

    def value(self):
        return self.widget.text()

    def set_value(self, value):
        self.widget.setText(value)


class CheckBoxesField:
    """ A set of checkboxes which can be interacted with separately.

    The dictionary of statuses (checked or not) can be retrieved with value().
    """

    def __init__(
            self,
            key,
            label,  # list N
            values,
            default=None,  #list N
            remember=True):
        """
        Parameters
        ----------

        `key` : str
           A identifier for value

        `label` : str
           The label of the widget, which contains all the checkboxes.

        `values` : list of strings
           The label given to each individual check box.

        `default` : List of bools, optional
           Default values. If provided, it must have the same length as values.

        `remember` : bool, optional
           When set to false, the field will reset to default when init() is
           called.
        """

        if default is None:
            default = [False for _ in values]

        if type(values) != list:
            raise TypeError('"values" is not a list.')

        for value in values:
            if type(value) is not str:
                raise TypeError('values are not strings.')

        for d in default:
            if type(d) is not bool:
                raise TypeError('default are not booleans.')

        self.widget = self._widget(key, values, label, default)
        self.key = key
        self.values = values
        self.label = label
        self.__default_value = dict(zip(values, default))
        self.__remember = remember

    def _widget(self, key, values, label, default):
        w = QGroupBox()
        layout = QVBoxLayout()
        self._optionwidgets = {}
        self._layout = layout
        for value, default in zip(values, default):
            checkbox = QCheckBox(value)
            checkbox.setChecked(default)
            self._optionwidgets[key] = checkbox
            layout.addWidget(checkbox)
        w.setLayout(layout)
        return w

    def value(self):
        """ Returns a dictionary containing the statuses for the checkboxes.
        """

        return {k: self._optionwidgets[k].isChecked() for k in self.key}

    def set_value(self, value_to_set):
        if type(value_to_set) is not dict:
            raise TypeError('Input is not a dictionary.')

        for v in value_to_set.values():
            if type(v) is not bool:
                raise TypeError('Input dict values are not booleans.')

        for k, v in value_to_set.items():
            self._optionwidgets[k].setChecked(v)

    def init(self):
        if not self.__remember:
            self.set_value(self.__default_value)


class DropDownField(BaseField):
    """A field representing a choice amongst a number of options.
    """

    def __init__(self, key, label, options, default=None, remember=True):
        """
        """

        self.__options = options

        super().__init__(key, label, default, remember)

    def value(self):
        return self.widget.currentText()

    def set_value(self, value):
        if value is not None:
            if value not in self.__options:
                raise ValueError('Value not in options.')
            self.widget.setCurrentText(value)

    def _widget(self):
        widget = QComboBox()
        widget.addItems(self.__options)
        widget.setEditable(False)
        return widget
