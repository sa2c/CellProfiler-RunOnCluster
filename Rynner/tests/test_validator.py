import unittest
import rynner
from rynner.validator import *


class TestIntValidator(unittest.TestCase):
    def setUp(self):
        pass

    def test_interval_pass(self):
        validator = IntValidator(min=6, max=12)
        goodvalue = 7
        self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_no_boundaries_int(self):
        validator = IntValidator()
        goodvalue = 16
        self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_no_boundaries_str(self):
        validator = IntValidator()
        badvalue = '5'
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
        self.assertEqual(str(cm.exception), f'not Int')

    def test_min_int(self):
        validator = IntValidator(min=15)
        badvalue = 10
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
        self.assertEqual(str(cm.exception), f'not Int in range 15 to +infty')

    def test_min_int_pass(self):
        validator = IntValidator(min=-2)
        goodvalue = 13
        self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_min_str(self):
        validator = IntValidator(min=20)
        badvalue = '5'
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
        self.assertEqual(str(cm.exception), f'not Int')

    def test_max_int(self):
        validator = IntValidator(max=92)
        badvalue = 93
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
            self.assertEqual(
                str(cm.exception), f'not Int in range -infty to 92')

    def test_max_int_pass(self):
        validator = IntValidator(max=32)
        goodvalue = 25
        self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_max_str(self):
        validator = IntValidator(max=6)
        badvalue = '5'
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
        self.assertEqual(str(cm.exception), f'not Int')

    def test_interval_fail(self):
        with self.assertRaises(RynnerValidatorException) as cm:
            valid = IntValidator(min=12, max=-6)

        self.assertEqual(str(cm.exception), f'range [12,-6] is empty')

    def test_max_str(self):
        validator = IntValidator(min=6, max=120)
        badvalue = '5'
        with self.assertRaises(RynnerValidatorException) as cm:
            validator.validate(badvalue)
        self.assertEqual(str(cm.exception), f'not Int')


class TestTimeHMSStringValidator(unittest.TestCase):
    def test_int_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = [1, ('b', 1), 1.50]
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(1)

            self.assertEqual(str(cm.exception), f'Not a string')

    def test_ms_ok(self):
        validator = TimeHMSStringValidator()
        goodvalues = ['00:12:50', '12:32:50', '80:32:50']
        for goodvalue in goodvalues:
            self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_random_string_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = [
            'a string',
            '20h 3m 50s',
        ]
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)

            self.assertEqual(str(cm.exception), f'Not a valid duration')

    def test_malformed_durations_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = [':10:20', '::', '40::', '10:-5:3', 'a:a:a']
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)

            self.assertEqual(str(cm.exception), f'Not a valid duration')

    def test_spaces_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = [
            ' 10:20:33', '23 :15:32', '23: 12:00', '10:50 :12', '85:10: 56',
            '12:20:15 '
        ]
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)
            self.assertEqual(str(cm.exception), f'Not a valid duration')

    def test_excess_seconds_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = ['10:30:80', '56:10:60', '122:00:61']
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)

            self.assertEqual(
                str(cm.exception), f'Seconds should be between 0 and 59')

    def test_excess_minutes_fail(self):
        validator = TimeHMSStringValidator()
        badvalues = ['10:70:20', '56:60:15', '122:90:00']
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)

            self.assertEqual(
                str(cm.exception), f'Minutes should be between 0 and 59')


class TestSimplieTypeValidator(unittest.TestCase):
    def test_bool_fail(self):
        validator = SimpleTypeValidator(bool)
        badvalues = [1, 'asdfa', (True, 'p')]
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)
            self.assertEqual(str(cm.exception), f'Input value is not bool')

    def test_bool_pass(self):
        validator = SimpleTypeValidator(bool)
        goodvalues = [True, False]
        for goodvalue in goodvalues:
            self.assertEqual(validator.validate(goodvalue), goodvalue)

    def test_str_fail(self):
        validator = SimpleTypeValidator(str)
        badvalues = [1, True, (True, 'p')]
        for badvalue in badvalues:
            with self.assertRaises(RynnerValidatorException) as cm:
                validator.validate(badvalue)
            self.assertEqual(str(cm.exception), f'Input value is not str')

    def test_str_pass(self):
        validator = SimpleTypeValidator(str)
        goodvalues = ['True', 'another string']
        for goodvalue in goodvalues:
            self.assertEqual(validator.validate(goodvalue), goodvalue)
