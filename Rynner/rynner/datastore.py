import os
import yaml


class Datastore:
    """ A class to manage job data storage on the cluster.
    """

    store_name = 'datastore.yaml'

    def __init__(self, connection):
        self.connection = connection

    def write(self, basedir, data):
        path = os.path.join(basedir, self.store_name)
        content = yaml.dump(data)
        self.connection.put_file_content(content, path)

    def read(self, basedir):
        # get datastore
        path = os.path.join(basedir, self.store_name)
        content = self.connection.get_file_content(path)
        data = yaml.load(content)

        # if datastore doesn't have qid yet then add it
        # and write it back
        if not hasattr(data, 'qid'):
            qid_path = os.path.join(basedir, 'qid')

            content = self.connection.get_file_content(
                qid_path).decode().split()

            if len(content) > 0:
                content = content[0]

            data['qid'] = content

            self.write(basedir, data)

        return data

    def read_multiple(self, basedict):
        """Accepts a dict where the values are base directories
        of a run, and replaces the value with the content of
        the datastore for that run, leaving the keys untouched.
        """

        return {key: self.read(dir_) for key, dir_ in basedict.items()}

    def all_job_ids(self, basedir):
        return self.connection.list_dir(basedir)
