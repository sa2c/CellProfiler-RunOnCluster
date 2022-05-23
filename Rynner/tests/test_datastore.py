import paramiko
import os
import yaml
from logging import Logger
from rynner.datastore import Datastore
from rynner.host import Connection
from tests.host_env import homedir, test_host, test_user


class TestDatastore:
    def test_store_and_retrieve(self):
        ssh = paramiko.SSHClient()
        connection = Connection(
            Logger('name'),
            test_host,
            user=test_user,
            rsa_file=f'{homedir}/.ssh/id_rsa')

        # info to store
        info = {'a': 1, 'b': 'two', 3: 'c'}
        info = {'info': info, 'info': [{'beta': info, 'alpha': [info, info]}]}

        datastore = Datastore(connection)
        datastore.write('my_plugin/some-id', info)
        info_stored = datastore.read('my_plugin/some-id')
        assert info_stored == info
