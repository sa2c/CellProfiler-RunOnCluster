# -*- coding: future_fstrings -*-
"""
Creates an instance of Rynner to be shared between the
clusterview and runnoncluster plugins.
"""

import wx
from future import *

from rynner.rynner import Rynner
from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException, FileCopyException
import tempfile


class LoginDialog(wx.Dialog):
    """
    A dialog window asking for a username and a password
    """
 
    def __init__(self, username = ''):
        """Constructor"""
        super(LoginDialog, self).__init__(None, title="Login", size = (250,180))
        self.panel = wx.Panel(self)

        # username field
        username_sizer = wx.BoxSizer(wx.HORIZONTAL)
        username_label = wx.StaticText(self.panel, label="Username:")
        username_sizer.Add(username_label, 0, wx.ALL|wx.CENTER, 5)
        self.username = wx.TextCtrl(self.panel, value = username)
        username_sizer.Add(self.username, 0, wx.ALL, 5)
 
        # password field
        password_sizer = wx.BoxSizer(wx.HORIZONTAL)
        password_label = wx.StaticText(self.panel, label="Password: ")
        password_sizer.Add(password_label, 0, wx.ALL|wx.CENTER, 5)
        self.password = wx.TextCtrl(self.panel, style=wx.TE_PASSWORD|wx.TE_PROCESS_ENTER)
        password_sizer.Add(self.password, 0, wx.ALL, 5)
 
        # The Login button (overwrite self.btn from wx.Dialog)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.btn = wx.Button(self.panel, wx.ID_OK, label="Login", size=(60, 30))
        button_sizer.Add(self.btn, 0, wx.ALL , 5)
        
        # Bind password enter press to the button
        button_event = wx.PyCommandEvent(wx.EVT_BUTTON.typeId,self.btn.GetId())
        self.password.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )

        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(username_sizer, 0, wx.ALL, 5)
        main_sizer.Add(password_sizer, 0, wx.ALL, 5)
        main_sizer.Add(button_sizer, 0, wx.ALL | wx.ALIGN_CENTER, 5)
 
        self.panel.SetSizer(main_sizer)


def _get_username_and_password():
    cnfg = wx.Config('CPRynner')
    if cnfg.Exists('username'):
        username = cnfg.Read('username')
    else:
        username = ''

    dialog = LoginDialog( username )
    result = dialog.ShowModal()
    if result == wx.ID_OK:
        username = dialog.username.GetValue()
        password = dialog.password.GetValue()

        cnfg.Write('username', username)

        return [username, password]
    else:
        return [None,None]
    dialog.Destroy()

def _create_rynner():
    ''' Create an instance of Rynner connected to the cluster
    '''
    username, password = _get_username_and_password()
    while username is None:
        wx.MessageBox(
            'Unable to log in. Check your username and password.',
            'Info', wx.OK | wx.ICON_INFORMATION
        )
        username, password = _get_username_and_password()

    tmpdir = tempfile.mkdtemp()
    
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
        walltime="00:01:00",
        init_blocks=1,
        max_blocks=1,
        launcher = SimpleLauncher(),
    )
    return Rynner(provider)

cprynner = None
def CPRynner():
    ''' Return a shared instance of Rynner
    '''
    global cprynner
    while cprynner is None:
        try:
            cprynner = _create_rynner()
            if cprynner is None:
                wx.MessageBox(
                    'Unable to log in. Check your username and password. If the problem persists, contact your HPC system administrator.',
                    'Info', wx.OK | wx.ICON_INFORMATION
                )
                username, password = _get_username_and_password()
        except SSHException:
            wx.MessageBox(
                'Unable to contact the cluster. The cluster may be offline or you may have a problem with your internet connection.',
                'Info', wx.OK | wx.ICON_INFORMATION
            )
            return None

    return cprynner
    
