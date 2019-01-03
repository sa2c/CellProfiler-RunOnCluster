# -*- coding: future_fstrings -*-
"""
Creates an instance of Rynner to be shared between the
clusterview and runnoncluster plugins. Implements safeguards
for file handling and ssh execptions.
"""

import os, shutil
import wx
from future import *

from rynner.rynner import Rynner
from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException, FileCopyException
import tempfile


def _create_rynner(username):
    ''' Create an instance of Rynner connected to the cluster
    '''
    if username is None:
        dialog = wx.TextEntryDialog(None, "Cluster Username", 'Username','',style=wx.TextEntryDialogStyle)
        result = dialog.ShowModal()
        if result == wx.ID_OK:
            username = dialog.GetValue()
        else:
            return None
        dialog.Destroy()

    dialog = wx.PasswordEntryDialog(None, "Cluster Password", 'Password','',style=wx.TextEntryDialogStyle)
    result = dialog.ShowModal()
    if result == wx.ID_OK:
        password = dialog.GetValue()
    else:
        return None
    dialog.Destroy()

    tmpdir = tempfile.mkdtemp()
    try:
        provider = SlurmProvider(
            'compute',
            channel=SSHChannel(
                hostname='sunbird.swansea.ac.uk',
                username=username,
                password=password,
                script_dir='rynner',
            ),
            script_dir=tmpdir,
            nodes_per_block=1,
            tasks_per_node=1,
            walltime="00:00:10",
            init_blocks=1,
            max_blocks=1,
            launcher = SimpleLauncher(),
        )
        return Rynner(provider)
    except SSHException:
        return None

cprynner = None
def CPRynner(username):
    ''' Return a shared instance of Rynner
    '''
    global cprynner
    if cprynner is None:
        cprynner = _create_rynner(username)
    return cprynner
    
