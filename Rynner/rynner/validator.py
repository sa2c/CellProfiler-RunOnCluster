from abc import ABC, abstractmethod
import re


class RynnerValidatorException(Exception):
    pass


class Validator(ABC):
    @abstractmethod
    def validate():
        """
        Returns the value passed when it is ok, raises RynnerValidatorException
        otherwise.
        """
        pass


class IntValidator(Validator):
    """
    Checks that input to validate method is an integer.
    If min or max are specified, it also checks against these constraints.
    """

    def __init__(self, min=None, max=None):
        self.min = min
        self.max = max
        if min is not None and max is not None and min > max:
            raise RynnerValidatorException(
                f'range [{min},{max}] is empty'.format(min=min, max=max))

    def validate(self, value):
        if type(value) is not int:
            raise RynnerValidatorException(f'not Int')

        if self.min is not None and value < self.min:
            raise RynnerValidatorException(
                'not Int in range {} to +infty'.format(self.min))

        if self.max is not None and value > self.max:
            raise RynnerValidatorException(
                'not Int in range -infty to {}'.format(self.max))

        return value


class TimeHMSStringValidator(Validator):
    """
    Checks that input to validate method is a valid H:M:S time duration.
    M and S must be integers between 0 and 59, while H must be an integer
    greater or equal than 0.
    """

    def validate(self, value):
        if type(value) is not str:
            raise RynnerValidatorException(f'Not a string')

        regexp = re.compile('^\d+:\d+:\d+$')

        if regexp.match(value) is None:
            raise RynnerValidatorException(f'Not a valid duration')

        H, M, S = value.split(':')
        S = int(S)
        if S < 0 or S > 59:
            raise RynnerValidatorException(
                f'Seconds should be between 0 and 59')

        M = int(M)
        if M < 0 or M > 59:
            raise RynnerValidatorException(
                f'Minutes should be between 0 and 59')

        return value


class SimpleTypeValidator(Validator):
    """
    Only checks that the input to validate is of the type passed to the
    constructor.
    """

    def __init__(self, t):
        self.mytype = t

    def validate(self, value):
        if type(value) is not self.mytype:
            raise RynnerValidatorException(
                f'Input value is not {self.mytype.__name__}')
        return value


validators = {
    'account': SimpleTypeValidator(str),
    'memory_per_task_MB': IntValidator(min=0),
    'name': SimpleTypeValidator(str),
    'ngpus': IntValidator(min=0),
    'ntasks': IntValidator(min=1),
    'ntasks_per_node': IntValidator(min=1),
    'output_file': SimpleTypeValidator(str),
    'oversubscribe': SimpleTypeValidator(bool),
    'queue': SimpleTypeValidator(str),
    'runtime_HMS': TimeHMSStringValidator(),
}
