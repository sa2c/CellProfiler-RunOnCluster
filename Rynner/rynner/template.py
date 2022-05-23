from string import Formatter


class TemplateArgumentException(Exception):
    pass


class Template:
    @classmethod
    def from_file(self, path):
        with open(path, 'r') as f:
            content = f.read()
        return Template(content)

    def __init__(self, template_string):
        self.template_string = template_string

    def format(self, args):
        try:
            argset = set(args.keys())
        except:
            raise TemplateArgumentException(
                'invalid type of template arguments')

        if argset == self.keys():
            return self.template_string.format(**args)
        else:
            raise TemplateArgumentException(
                f'template arguments do not match {self.keys()} != {argset}')

    def keys(self):
        return {i[1] for i in Formatter().parse(self.template_string)}
