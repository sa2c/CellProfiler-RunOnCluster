import unittest
from unittest.mock import MagicMock as MM
from unittest.mock import patch
from PySide2.QtCore import QTimer
from PySide2.QtTest import QTest
import rynner
from rynner.create_view import *
from tests.qtest_helpers import *

# TODO: for CheckBoxesField, only __init__, set_value and value methods have been tested,
#       and it has been tested that 'key','label' and 'widget' are public members.
#       All the rest must be tested.


class ConcreteField(BaseField):
    def __init__(self, key, label, default=None, remember=True):
        super().__init__(key, label, default=default, remember=remember)
        self.__value = default

    def _widget(self):
        return QWidget()

    def value(self):
        return self.__value

    def set_value(self, value):
        self.__value = value


class TestBaseField(unittest.TestCase):
    def setUp(self):
        pass

    def instance(self, **kwargs):
        self.input = ConcreteField('key', 'Label', **kwargs)

    def test_instance(self):
        self.instance()

    def test_show_qwidget(self):
        input = ConcreteField('key', 'label')
        input.init()

    def test_has_adds_widgets_as_child(self):
        input = ConcreteField('key', 'label')

        self.assertIsInstance(input.widget, QWidget)

    def test_sets_default_text_in_text_edit(self):
        input = ConcreteField('key', 'label', default='default string')
        self.assertEqual(input.value(), 'default string')

    def test_label_contains_text(self):
        input = ConcreteField('key', 'My label')

        self.assertEqual(input.label.text(), "My label")
        self.assertIsInstance(input.label, QLabel)

    def test_uses_default_as_initial(self):
        input = ConcreteField('key', 'My label', default="default value")

        input.init()

        self.assertEqual(input.value(), "default value")

    def test_stores_key(self):
        mock_key = MM()
        input = ConcreteField(mock_key, 'label')

        self.assertEqual(input.key, mock_key)

    def test_cli_asks_for_input(self):
        input = ConcreteField('key', 'Test Label')

        input_data = "Test Input Data"

        with patch.object(
                rynner.create_view, "input", create=True,
                return_value=input_data):
            value = input.cli()
            self.assertEqual(value, input_data)

    @patch('rynner.create_view.input')
    def test_cli_correct_label(self, mock_input):
        input = ConcreteField('key', 'Test Label')

        value = input.cli()
        mock_input.assert_called_once_with('Test Label')

    def test_default_BaseField_is_valid(self):
        self.instance()
        self.assertTrue(self.input.valid())


class RunCreateViewTestInput(QTestCase):
    def setUp(self):
        self.children = [
            TextField('key1', 'My label 1', default="My Value 1"),
            TextField('key2', 'My label 2', default="My Value 2"),
            TextField('key3', 'My label 3', default="My Value 3")
        ]

    def children_widgets(self):
        return [child.widget for child in self.children]

    def instance(self):
        self.run_create_view = RunCreateView(self.children)

    def run_create_view_widget(self):
        return self.run_create_view.layout().itemAt(0).widget()

    def test_instance(self):
        self.instance()

    def test_create_dialog(self):
        self.instance()
        self.assertIsInstance(self.run_create_view, QDialog)

    def test_widgets_added_as_children_of_dialog(self):
        self.instance()

        widget_children = self.run_create_view_widget().children()
        for child in self.children:
            self.assertIn(child.widget, widget_children)

    def test_all_keys_must_be_unique(self):

        first = self.children[0]

        self.children.append(TextField(first.key, first.label))

        with self.assertRaises(DuplicateKeyException) as context:
            self.instance()

        self.assertIn(first.key, str(context.exception))

    def test_widgets_added_to_layout(self):
        self.instance()

        layout = self.run_create_view_widget().layout()
        count = layout.count()
        widgets = (layout.itemAt(idx).widget() for idx in range(count))

        for index, child in enumerate(self.children):
            # Type and content of QLabel
            qlabel = next(widgets)
            self.assertEqual(type(qlabel), QLabel)
            self.assertEqual(qlabel.text(), f"My label {index + 1}")

            # Type and content of QLineEdit
            qfield = next(widgets)
            self.assertEqual(type(qfield), QLineEdit)
            self.assertEqual(qfield.text(), f"My Value {index + 1}")

    def test_data_returns_data_from_children(self):
        self.children = [
            TextField('key1', 'Label', default='default1'),
            TextField('key2', 'Label', default='default2')
        ]
        self.instance()
        self.assertEqual(self.run_create_view.data(), {
            'key1': 'default1',
            'key2': 'default2'
        })

    def test_valid_returns_empty_if_no_invalid_children(self):
        self.instance()
        invalid = self.run_create_view.invalid()
        self.assertEqual(len(invalid), 0)

    def test_valid_true_if_children_valid(self):

        self.children[0].valid = lambda: False

        self.instance()

        self.assertEqual(self.run_create_view.invalid(), [self.children[0]])

    def test_creates_and_shows_dialog(self):
        self.instance()
        self.assertNotQVisible(self.children_widgets())

        def callback():
            # widgets visible on show
            self.assertQVisible(self.children_widgets())
            assert self.run_create_view.isVisible()

            # widgets visible on show
            self.run_create_view.accept()

            # dialog invisible after accept
            assert not self.run_create_view.isVisible()

        QTimer.singleShot(10, callback)

        self.run_create_view.show()

    def test_resets_children(self):
        input1 = TextField("key1", "label", default="default", remember=False)
        input2 = TextField("key2", "label", default="default", remember=True)

        self.children = [input1, input2]
        self.instance()

        # type into fields
        for child in self.children:
            QTest.keyClicks(child.widget, " some text")

        ok_button = find_QPushButton(self.run_create_view, 'ok')
        button_callback(method=self.run_create_view.show, button=ok_button)

        values = self.run_create_view.data()

        self.assertEqual(values, {
            'key1': 'default',
            'key2': 'default some text'
        })


class TestRunCreateViewTestDialog(QTestCase):
    def setUp(self):
        self.children = [
            TextField('key1', 'My label 1', default="My Value 1"),
            TextField('key2', 'My label 2', default="My Value 2"),
            TextField('key3', 'My label 3', default="My Value 3")
        ]
        self.widgets = (c.widget for c in self.children)

    def instance(self, **kwargs):
        self.dialog = RunCreateView(self.children, **kwargs)

    def instance_items(self):
        layout = self.dialog.layout()
        return [layout.itemAt(i).widget() for i in range(layout.count())]

    def test_instance(self):
        self.instance()

    def test_default_title(self):
        self.instance()
        self.assertEqual(self.dialog.windowTitle(), "Set up run")

    def test_title(self):
        self.instance(title="title")
        self.assertEqual(self.dialog.windowTitle(), "title")

    def test_widget_in_layout(self):
        self.instance()
        items = self.instance_items()

        self.assertNotQVisible(self.widgets)

        self.dialog.show()

        self.assertQVisible((c.widget for c in self.children))

    def test_buttons_in_layout(self):
        self.instance()
        type_items = list(map(type, self.instance_items()))

        self.assertIn(QDialogButtonBox, type_items)

    def test_shows_dialog_with_title(self):
        widgets = [child.widget for child in self.children]
        self.instance(title="MY WINDOW TITLE")
        self.assertNotQVisible(widgets)
        self.dialog.show()
        self.assertQVisible(widgets)
        self.assertEqual(self.dialog.windowTitle(), "MY WINDOW TITLE")

    def test_shows_dialog_twice(self):
        widgets = [child.widget for child in self.children]
        self.instance()

        # Show once
        self.assertNotQVisible(widgets)
        self.dialog.show()
        self.assertQVisible(widgets)
        self.dialog.close()
        self.assertNotQVisible(widgets)

        # Show again
        self.assertNotQVisible(widgets)
        self.dialog.show()
        self.assertQVisible(widgets)
        self.dialog.close()
        self.assertNotQVisible(widgets)


class TestTextField(unittest.TestCase):
    def setUp(self):
        pass

    def instance(self, **kwargs):
        self.input = TextField('key', 'Label', **kwargs)

    def type_text(self, text):
        QTest.keyClicks(self.input.widget, text)

    def test_can_type_text(self):
        self.instance()
        self.type_text("some input")

    def test_value_method_return_text(self):
        self.instance()
        self.type_text("some input")
        self.assertEqual(self.input.value(), "some input")

    def test_reset_leaves_value_by_default(self):
        input = TextField('key', 'My label')
        input.init()

        QTest.keyClicks(input.widget, "My Input Text")

        self.assertEqual(input.value(), "My Input Text")

        input.init()

        self.assertEqual(input.value(), "My Input Text")

    def test_no_reset_as_default(self):
        input = TextField('key', 'My label', default="default value")

        QTest.keyClicks(input.widget, " and some more text")

        value = input.value()
        self.assertNotEqual(input.value(), "default value")

        input.init()

        # input value remains the same on calls to init
        self.assertEqual(input.value(), value)

    def test_resets_if_reset_true(self):
        input = TextField(
            'key', 'My label', default="default value", remember=False)

        QTest.keyClicks(input.widget, " and some more text")

        self.assertNotEqual(input.value(), "default value")

        input.init()

        self.assertEqual(input.value(), "default value")


class TestNumericField(unittest.TestCase):
    def setUp(self):
        pass

    def instance(self, **kwargs):
        self.input = NumericField('key', 'Label', **kwargs)

    def test_instance(self):
        self.instance()

    def test_widget_should_return_text_field(self):
        self.instance()
        self.assertIsInstance(self.input.widget, QLineEdit)


class TestCheckBoxesField(unittest.TestCase):
    def setUp(self):
        pass

    def test_instance_pass1(self):
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']

        CheckBoxesField(keys=keys, labels=labs)

    def test_instance_fail1(self):
        '''
        Labels and keys must have same length.
        '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2']

        with self.assertRaises(ValueError) as cm:
            CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(str(cm.exception), 'len(labels)[2] != len(keys)[3]')

    def test_instance_fail2(self):
        '''
        Keys must be strings.
        '''
        keys = [(1, 2, 3), 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']

        with self.assertRaises(TypeError) as cm:
            CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(str(cm.exception), 'keys are not strings.')

    def test_instance_fail3(self):
        '''
        labels must be strings.
        '''
        keys = ['key1', 'key2', 'key3']
        labs = [(1, 2, 3), 'lab2', 'lab3']

        with self.assertRaises(TypeError) as cm:
            CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(str(cm.exception), 'labels are not strings.')

    def test_instance_fail4(self):
        '''
        keys must be lists
        '''
        keys = 'key1'
        labs = ['lab1']
        with self.assertRaises(TypeError) as cm:
            CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(str(cm.exception), '"keys" is not a list.')

    def test_instance_fail5(self):
        '''
        labels must be lists
        '''
        keys = ['key1']
        labs = 'lab1'
        with self.assertRaises(TypeError) as cm:
            CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(str(cm.exception), '"labels" is not a list.')

    def test_instance_fail6(self):
        '''
        if defaults is specified, it must be of the same length of
        keys and labels.
        '''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        defaults = [True]
        with self.assertRaises(ValueError) as cm:
            CheckBoxesField(keys=keys, labels=labs, defaults=defaults)

        self.assertEqual(str(cm.exception), 'len(defaults)[1] != len(keys)[2]')

    def test_instance_fail6(self):
        '''
        if defaults is specified, it must be composed of booleans.
        '''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        defaults = ['True', True]
        with self.assertRaises(TypeError) as cm:
            CheckBoxesField(keys=keys, labels=labs, defaults=defaults)

        self.assertEqual(str(cm.exception), 'defaults are not booleans.')

    def test_instance_pass2(self):
        '''
        The widget must be created with the right title.
        '''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        title = 'A title'
        cb = CheckBoxesField(keys=keys, labels=labs, title=title)
        self.assertEqual(cb.widget.title(), title)

    def test_instance_pass3a(self):
        '''
        the widget children must have the correct labels.
        '''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(len(cb._optionwidgets), len(labs))

    def test_instance_pass3b(self):
        '''
        the widget children must have the correct labels.
        '''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        for label, key in zip(labs, keys):
            optionWidget = cb._optionwidgets[key]
            self.assertEqual(optionWidget.text(), label)

    def test_instance_pass4a(self):
        '''
        The widget children must be in the added to the layout.
        '''

        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        widgets_layout = [
            cb._layout.itemAt(i).widget() for i in range(cb._layout.count())
        ]
        self.assertEqual(len(widgets_layout), len(keys))

    def test_instance_pass4b(self):
        '''
        The widget children must be in the added to the layout.
        '''

        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        widgets_layout = [
            cb._layout.itemAt(i).widget() for i in range(cb._layout.count())
        ]

        for label, key, optionWidget2 in zip(labs, keys, widgets_layout):
            optionWidget = cb._optionwidgets[key]
            self.assertIs(optionWidget, optionWidget2)

    def test_instance_5(self):
        '''The widget must have the layout as layout.'''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        self.assertIs(cb.widget.layout(), cb._layout)

    def test_value_1(self):
        '''Value() must return a dictionary'''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        self.assertIsInstance(cb.value(), dict)

    def test_value_2(self):
        '''Value() must return a dictionary, and keys must be equal to the keys
        passed to __init__()'''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(set(cb.value().keys()), set(keys))

    def test_value_3(self):
        '''Value() must return a dictionary, of booleans.'''
        keys = ['key1', 'key2']
        labs = ['lab1', 'lab2']
        cb = CheckBoxesField(keys=keys, labels=labs)

        for k, v in cb.value().items():
            self.assertIsInstance(v, bool)

    def test_value_4(self):
        '''Value() must return the default values just after initialisation.'''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        defaults = [True, False, True]
        cb = CheckBoxesField(keys=keys, labels=labs, defaults=defaults)

        expectedValues = dict(zip(keys, defaults))

        self.assertEqual(expectedValues, cb.value())

    def test_set_value_fail1(self):
        '''set_value() must complain if a dict is not passed as input.'''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        cb = CheckBoxesField(keys=keys, labels=labs)

        valuesToSet = labs  # just not a dict

        with self.assertRaises(TypeError) as cm:
            cb.set_value(valuesToSet)

        self.assertEqual(str(cm.exception), 'Input is not a dictionary.')

    def test_set_value_fail2(self):
        '''set_value() must complain if in the dict passed as input values are not booleans.'''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        cb = CheckBoxesField(keys=keys, labels=labs)

        valuesToSet = dict(zip(keys, labs))  # just not bools as dict

        with self.assertRaises(TypeError) as cm:
            cb.set_value(valuesToSet)

        self.assertEqual(
            str(cm.exception), 'Input dict values are not booleans.')

    def test_set_value_1(self):
        '''value() must return values set with set_values().'''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        vals = [True, False, True]
        cb = CheckBoxesField(keys=keys, labels=labs)

        valuesToSet = dict(zip(keys, vals))

        cb.set_value(valuesToSet)

        self.assertEqual(valuesToSet, cb.value())

    def test_getkey(self):
        '''
        field 'key' must be part of the run_create_view.
        '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        cb = CheckBoxesField(keys=keys, labels=labs)

        self.assertEqual(keys, cb.key)

    def test_getlabel(self):
        '''
        field 'label' must be part of the run_create_view.
        '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        title = 'a title'
        cb = CheckBoxesField(keys=keys, labels=labs, title=title)

        self.assertEqual(labs, cb.label)

    def test_getwidget(self):
        '''
        field 'widget' must be part of the run_create_view.
        '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        title = 'a title'
        cb = CheckBoxesField(keys=keys, labels=labs, title=title)

        thewidget = cb.widget

    def test_remember_1(self):
        '''constructor should accept 'remember' '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        title = 'a title'
        cb = CheckBoxesField(
            keys=keys, labels=labs, title=title, remember=True)

    def test_init(self):
        '''Ojbect should have init() method'''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        defaults = [True, False, True]
        cb = CheckBoxesField(
            keys=keys, labels=labs, defaults=defaults, remember=False)

        vals = [False, True, True]

        valuesToSet = dict(zip(keys, vals))

        cb.set_value(valuesToSet)
        cb.init()

    def test_remember_2(self):
        ''' If remember is set to false, init() should restore the value to the default '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        defaults = [True, False, True]
        cb = CheckBoxesField(
            keys=keys, labels=labs, defaults=defaults, remember=False)

        vals = [False, True, True]

        valuesToSet = dict(zip(keys, vals))

        cb.set_value(valuesToSet)
        cb.init()

        expectedValues = dict(zip(keys, defaults))

        self.assertEqual(expectedValues, cb.value())

    def test_remember_2(self):
        ''' If remember is set to true, init() should not change the value '''
        keys = ['key1', 'key2', 'key3']
        labs = ['lab1', 'lab2', 'lab3']
        defaults = [True, False, True]
        cb = CheckBoxesField(
            keys=keys, labels=labs, defaults=defaults, remember=True)

        vals = [False, True, True]

        valuesToSet = dict(zip(keys, vals))

        cb.set_value(valuesToSet)
        expectedValues = cb.value()
        cb.init()
        self.assertEqual(expectedValues, cb.value())


class TestDropDownField(unittest.TestCase):
    def test_instance1(self):
        ''' Basic instantiation test'''
        options = ['1', '2', '3']
        dd = DropDownField('testOption', 'testLabel', options=options)

    def test_instance2(self):
        ''' instantiation with default (default in the option list)'''
        options = ['1', '2', '3']
        default = '2'
        dd = DropDownField(
            'testOption', 'testLabel', options=options, default=default)
        self.assertEqual(dd.value(), default)

    def test_setvalue_fail1(self):
        ''' setting value (value NOT in the option list)'''
        options = ['1', '2', '3']
        valtoset = '4'
        dd = DropDownField('testOption', 'testLabel', options=options)

        with self.assertRaises(ValueError) as cm:
            dd.set_value(valtoset)

        self.assertEqual(str(cm.exception), 'Value not in options.')

    def test_setgetvalue(self):
        ''' setting value (value NOT in the option list)'''
        options = ['1', '2', '3']
        valtoset = '3'
        dd = DropDownField('testOption', 'testLabel', options=options)

        dd.set_value(valtoset)

        self.assertEqual(dd.value(), valtoset)
