"""
Creates an instance of Rynner to be shared between the
clusterview and runnoncluster plugins.
"""

from future import *

# Libsubmit creates a .script file in the working directory.
# To avoid clutter, we run in a temp directory
import tempfile, os
workdir = tempfile.mkdtemp()
os.chdir(workdir)

import wx
from rynner.rynner import Rynner
from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException


# Default cluster specific settings
max_tasks = 40


class clusterSettingDialog(wx.Dialog):
    """
    A dialog window for setting cluster parameters
    """

    def __init__(self, cluster_address, tasks_per_node, work_dir, setup_script ):
        """Constructor"""
        super(clusterSettingDialog, self).__init__(None, title="Login", size = (300,360))

        self.panel = wx.Panel(self)

        # cluster_address field
        cluster_address_sizer = wx.BoxSizer(wx.HORIZONTAL)
        cluster_address_label = wx.StaticText(self.panel, label="Cluster Address:")
        cluster_address_sizer.Add(cluster_address_label, 0, wx.ALL|wx.CENTER, 5)
        self.cluster_address = wx.TextCtrl(self.panel, value = cluster_address, size=(160, -1))
        cluster_address_sizer.Add(self.cluster_address, 0, wx.ALL, 5)

        # tasks_per_node field
        tasks_per_node_sizer = wx.BoxSizer(wx.HORIZONTAL)
        tasks_per_node_label = wx.StaticText(self.panel, label="Tasks Per Node:")
        tasks_per_node_sizer.Add(tasks_per_node_label, 0, wx.ALL|wx.CENTER, 5)
        self.tasks_per_node = wx.SpinCtrl(self.panel, value = str(tasks_per_node), size=(160, -1))
        tasks_per_node_sizer.Add(self.tasks_per_node, 0, wx.ALL, 5)

        # work_dir field
        work_dir_sizer = wx.BoxSizer(wx.HORIZONTAL)
        work_dir_label = wx.StaticText(self.panel, label="Working Directory:")
        work_dir_sizer.Add(work_dir_label, 0, wx.ALL|wx.CENTER, 5)
        self.work_dir = wx.TextCtrl(self.panel, value = work_dir, size=(160, -1))
        work_dir_sizer.Add(self.work_dir, 0, wx.ALL, 5)

        # setup_script field
        setup_script_sizer = wx.BoxSizer(wx.HORIZONTAL)
        setup_script_label = wx.StaticText(self.panel, label="Setup Script:")
        setup_script_sizer.Add(setup_script_label, 0, wx.ALL|wx.CENTER, 5)
        self.setup_script = wx.TextCtrl(self.panel, value = setup_script, size=(160, -1))
        setup_script_sizer.Add(self.setup_script, 0, wx.ALL, 5)

        # The Ok and Cancel button
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.ok_button = wx.Button(self.panel, wx.ID_OK, label="Ok", size=(60, 30))
        button_sizer.Add(self.ok_button, 0, wx.ALL , 5)

        self.cancel_btn = wx.Button(self.panel, wx.ID_CANCEL, label="Cancel", size=(60, 30))
        button_sizer.Add(self.cancel_btn, 0, wx.ALL , 5)
        
        # Bind enter press to the Ok button
        button_event = wx.PyCommandEvent(wx.EVT_BUTTON.typeId,self.ok_button.GetId())
        self.cluster_address.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )
        self.tasks_per_node.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )
        self.work_dir.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )
        self.setup_script.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )

        # Build the layout
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(cluster_address_sizer, 0, wx.ALL, 5)
        main_sizer.Add(tasks_per_node_sizer, 0, wx.ALL, 5)
        main_sizer.Add(work_dir_sizer, 0, wx.ALL, 5)
        main_sizer.Add(setup_script_sizer, 0, wx.ALL, 5)
        main_sizer.Add(button_sizer, 0, wx.ALL | wx.ALIGN_CENTER, 5)
 
        self.panel.SetSizer(main_sizer)



class LoginDialog(wx.Dialog):
    """
    A dialog window asking for a username and a password
    """
 
    def __init__(self, username = ''):
        """Constructor"""
        super(LoginDialog, self).__init__(None, title="Login", size = (300,180))

        self.panel = wx.Panel(self)

        # username field
        username_sizer = wx.BoxSizer(wx.HORIZONTAL)
        username_label = wx.StaticText(self.panel, label="Username:")
        username_sizer.Add(username_label, 0, wx.ALL|wx.CENTER, 5)
        self.username = wx.TextCtrl(self.panel, value = username, size=(160, -1))
        username_sizer.Add(self.username, 0, wx.ALL, 5)
 
        # password field
        password_sizer = wx.BoxSizer(wx.HORIZONTAL)
        password_label = wx.StaticText(self.panel, label="Password: ")
        password_sizer.Add(password_label, 0, wx.ALL|wx.CENTER, 5)
        self.password = wx.TextCtrl(self.panel, size=(160, -1), style=wx.TE_PASSWORD|wx.TE_PROCESS_ENTER)
        password_sizer.Add(self.password, 0, wx.ALL, 5)

 
        # The login and cancel button 
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.ok_button = wx.Button(self.panel, wx.ID_OK, label="Login", size=(60, 30))
        button_sizer.Add(self.ok_button, 0, wx.ALL , 5)

        self.cancel_btn = wx.Button(self.panel, wx.ID_CANCEL, label="Cancel", size=(60, 30))
        button_sizer.Add(self.cancel_btn, 0, wx.ALL , 5)

        self.settings_button = wx.Button(self.panel, wx.ID_PREFERENCES, label="Settings", size=(60, 30))
        button_sizer.Add(self.settings_button, 0, wx.ALL , 5)

        # Bind enter press to the Login button
        button_event = wx.PyCommandEvent(wx.EVT_BUTTON.typeId,self.ok_button.GetId())
        self.username.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )
        self.password.Bind( wx.EVT_TEXT_ENTER, lambda e: wx.PostEvent(self, button_event) )

        # Bind 
        self.settings_button.Bind(wx.EVT_BUTTON, self.settings )

        # Build the layout
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(username_sizer, 0, wx.ALL, 5)
        main_sizer.Add(password_sizer, 0, wx.ALL, 5)
        main_sizer.Add(button_sizer, 0, wx.ALL | wx.ALIGN_CENTER, 5)
 
        self.panel.SetSizer(main_sizer)

    def settings(self, button_event):
        update_cluster_parameters()


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


def _cluster_parameters():
    cnfg = wx.Config('CPRynner')
    if cnfg.Exists('cluster_address'):
        cluster_address = cnfg.Read('cluster_address')
    else:
        cluster_address = 'sunbird.swansea.ac.uk'

    if cnfg.Exists('tasks_per_node'):
        tasks_per_node = cnfg.Read('tasks_per_node')
    else:
        tasks_per_node = '40'
    
    if cnfg.Exists('work_dir'):
        work_dir = cnfg.Read('work_dir')
    else:
        work_dir = 'sunbird.swansea.ac.uk'

    if cnfg.Exists('setup_script'):
        setup_script = cnfg.Read('setup_script')
    else:
        setup_script = 'sunbird.swansea.ac.uk'

    return cluster_address, tasks_per_node, work_dir, setup_script


def update_cluster_parameters():
    cluster_address, tasks_per_node, work_dir, setup_script = _cluster_parameters()
    dialog = clusterSettingDialog( cluster_address, tasks_per_node, work_dir, setup_script )
    result = dialog.ShowModal()
    if result == wx.ID_OK:
        cluster_address = dialog.cluster_address.GetValue()
        tasks_per_node = dialog.tasks_per_node.GetValue()

        cnfg = wx.Config('CPRynner')
        cnfg.Write('cluster_address', cluster_address)
        cnfg.Write('tasks_per_node', tasks_per_node)
        cnfg.Write('work_dir', work_dir)
        cnfg.Write('setup_script', setup_script)

    dialog.Destroy()


def _create_rynner():
    ''' Create an instance of Rynner connected to the cluster
    '''
    hostname, tasks_per_node, work_dir, setup_script = _cluster_parameters()
    username, password = _get_username_and_password()
    if username is not None:

        tmpdir = tempfile.mkdtemp()

        path = '/scratch/'+username+'/CellProfiler/'
    
        provider = SlurmProvider(
            'compute',
            channel=SSHChannel(
                hostname=hostname,
                username=username,
                password=password,
                script_dir=path,
            ),
            script_dir=tmpdir,
            nodes_per_block=1,
            tasks_per_node=int(tasks_per_node),
            walltime="01:00:00", # Overwritten in runoncluster.py
            init_blocks=1,
            max_blocks=1,
            launcher = SimpleLauncher(),
        )
        return Rynner(provider, path)
    else:
        return None

cprynner = None
def CPRynner():
    ''' Return a shared instance of Rynner
    '''
    global cprynner
    if cprynner is None:
        try:
            cprynner = _create_rynner()
        except SSHException:
            wx.MessageBox(
                'Unable to contact the cluster. The cluster may be offline or you may have a problem with your internet connection.',
                'Info', wx.OK | wx.ICON_INFORMATION
            )
            return None

    return cprynner

def logout():
    ''' Logout and scrap the rynner object
    '''
    global cprynner
    if cprynner is not None:
        CPRynner().provider.channel.close()
        cprynner = None
    
