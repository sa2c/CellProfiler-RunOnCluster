# coding=utf-8

"""
ClusterView
=============



|

============ ============ ===============
Supports 2D? Supports 3D? Respects masks?
============ ============ ===============
YES          YES          NO
============ ============ ===============


"""

import six
import logging

logger = logging.getLogger(__package__)

import numpy as np
import os, shutil
import time

import cellprofiler.module as cpm
import cellprofiler.measurement as cpmeas
import cellprofiler.setting as cps
import cellprofiler.preferences as cpprefs

from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException, FileCopyException
import tempfile

import CPRynner.CPRynner as CPRynner

import wx

    
class ClusterviewFrame(wx.Frame):

    def __init__(self, parent, title):
        super(ClusterviewFrame, self).__init__(parent, title=title, size = (400,400))
        self.update_time = datetime.datetime.now()
        self.update()
        self.InitUI()
        self.Centre()

    def InitUI(self):

        self.panel = wx.lib.scrolledpanel.ScrolledPanel(self)

        self.panel.SetBackgroundColour('#ededed')
        self.vbox = wx.BoxSizer(wx.VERTICAL)

        self.build_view(self.vbox)

        self.panel.SetSizer(self.vbox)
        self.panel.SetAutoLayout(1)
        self.panel.SetupScrolling(scroll_x=False, scroll_y=True)

    def build_view(self, vbox):

        font = wx.SystemSettings.GetFont(wx.SYS_SYSTEM_FONT)
        font.SetPointSize(9)

        vbox.Add((-1, 5))

        hbox = wx.BoxSizer(wx.HORIZONTAL)

        btn = wx.Button(self.panel, label='Update', size=(90, 30))
        hbox.Add(btn, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        btn.Bind(wx.EVT_BUTTON, self.on_update_click )

        update_time_text = wx.StaticText(self.panel, label="")
        update_time_text.SetFont(font)
        self.set_timer(update_time_text)
        
        hbox.Add(update_time_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        vbox.Add(hbox, 0, wx.EXPAND, 10)

        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add((0,0), 1, wx.ALIGN_CENTER_VERTICAL)
        
        btn = wx.Button(self.panel, label='Logout', size=(90, 30))
        btn.Bind(wx.EVT_BUTTON, self.on_logout_click )
        hbox.Add(btn, 0, wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, 8)

        vbox.Add(hbox, 0, wx.EXPAND, 10)

        vbox.Add((-1, 5))

        line = wx.StaticLine(self.panel)
        vbox.Add(line, 0, wx.EXPAND, 10)

        self.run_displays = []
        for run in sorted(self.runs, key=lambda k: k['upload_time'], reverse = True):
            hbox1 = wx.BoxSizer(wx.HORIZONTAL)
            st = wx.StaticText(self.panel, label=run.job_name+":")
            st.SetFont(font)
            hbox1.Add(st, flag=wx.RIGHT, border=8)
            vbox.Add(hbox1, flag=wx.EXPAND|wx.LEFT|wx.RIGHT|wx.TOP, border=10)

            vbox.Add((-1, 5))

            hbox2 = wx.BoxSizer(wx.HORIZONTAL)
            st2 = wx.StaticText(self.panel, label=run.status+" since "+str(datetime.datetime.fromtimestamp(int(run['status_time']))))
            st2.SetFont(font)
            hbox2.Add(st2)
            vbox.Add(hbox2, flag=wx.LEFT | wx.TOP, border=10)

            vbox.Add((-1, 5))

            hbox3 = wx.BoxSizer(wx.HORIZONTAL)
            btn = wx.Button(self.panel, label='Download Results', size=(110, 30))
            btn.Bind(wx.EVT_BUTTON, lambda e, r=run: self.on_download_click( e, r ) )
            hbox3.Add(btn)
            vbox.Add(hbox3, flag=wx.ALIGN_RIGHT|wx.RIGHT, border=10)

    def set_timer(self, element):
        self.timer = wx.Timer(self)
        def update_st(event):
            element.SetLabel("Last updated: "+timeago.format(self.update_time, locale='en_GB'))
        def close(event):
            self.timer.Stop()
            self.Destroy()
        self.timer.Start(1000)
        self.Bind(wx.EVT_TIMER, update_st, self.timer)
        wx.EVT_CLOSE(self, close)

    def on_download_click(self, event, run):
        self.download(run)

    def on_update_click( self, event ):
        self.update()
        self.vbox.Clear(True)
        self.build_view(self.vbox)
        self.vbox.Layout() 
        self.FitInside()

    def on_logout_click( self, event ):
        CPRynner.logout()
        self.runs = []
        self.vbox.Clear(True)
        self.build_view(self.vbox)
        self.vbox.Layout() 
        self.FitInside()

    def update( self ):
        rynner = CPRynner.CPRynner()
        if rynner is not None:
            self.runs = [ r for r in rynner.get_runs() if 'upload_time' in r ]
            rynner.update(self.runs)
        else:
            self.runs = []

    def download( self, run ):
        default_target = cpprefs.get_default_output_directory()
        dialog = wx.DirDialog (None, "Choose an output directory", default_target,
                    wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        try:
            if dialog.ShowModal() == wx.ID_CANCEL:
                return
            target_directory = dialog.GetPath()
        except Exception:
            wx.LogError('Failed to open directory!')
            raise
        finally:
            dialog.Destroy()

        tmpdir = tempfile.mkdtemp()
        # Define the job to run
        run.downloads = [ [d[0], tmpdir] for d in run.downloads ]

        rynner = CPRynner.CPRynner()
        if rynner is not None:
            rynner.start_download(run)
            dialog = wx.GenericProgressDialog("downloading","Downloading files")
            maximum = dialog.GetRange()
            while run['download_status'] < 1:
                value = min( maximum, int(maximum*run['download_status']) )
                dialog.Update(value)
                time.sleep(0.04)
            dialog.Destroy()
            
        for runfolder, localdir in run.downloads:
            self.handle_result_file( 
                os.path.join(localdir, runfolder, 'results'),
                target_directory
            )
    
    def handle_result_file( self, filename, target_directory ):
        if os.path.isdir(filename):
            for f in os.listdir(filename):
                self.handle_result_file( os.path.join(filename, f), target_directory)
        else:
            try:
                name = os.path.basename(filename)
                target_file = os.path.join(target_directory, name)
                if not os.path.isfile(target_file):
                    shutil.move( filename, target_directory )
                elif filename.endswith('.csv'):
                    self.handle_csv( filename, target_file )
                else:
                    stripped_name, suffix = os.path.splitext(name)
                    n=2
                    new_name = stripped_name + '_' +str(n)+suffix
                    while os.path.isfile(new_name):
                        n += 1
                        new_name = stripped_name + '_' +str(n)+suffix
                    shutil.move( filename,  os.path.join(target_directory, new_name))
            except Exception as e:
                wx.MessageBox(
                    "Failed to move a file to the destination",
                    caption="File error",
                    style=wx.OK | wx.ICON_INFORMATION)

    def handle_csv( self, source, destination ):
        ''' Write the data rows of a csv file into an existing csv file.
            Fix image numbering before writing '''
        # First check if the file contains the image number
        outfile = open(destination,"rb")
        header = outfile.next()
        has_image_num = False
        for index, cell in enumerate(header.split(',')):
            if cell == 'ImageNumber':
                image_num_cell = index
                has_image_num = True
        
        # If the image number is included, find the largest value
        if has_image_num:
            last_image_num = 1
            for row in outfile:
                image_num = int(row.split(',')[image_num_cell])
                last_image_num = max(image_num, last_image_num)
        outfile.close()

        # Read the source file and write row by row to the destination
        infile = open(source, 'rb')
        infile.next()
        outfile = open(destination,"ab")
        for row in infile:
            # If the image number is included, correct the number
            if has_image_num:
                cells = row.split(',')
                local_num = int(cells[image_num_cell])
                cells[image_num_cell] = str(image_num+local_num)
                row = ','.join(cells)
            outfile.write(row)
        outfile.close()
        infile.close()
                

class clusterView(cpm.Module):
    module_name = "ClusterView"
    category = "Data Tools"
    variable_revision_number = 1

    @classmethod
    def is_input_module(cls):
        ''' This is a rather horrible hack...
            Prevents CellProfiler from listing this in the add module window.
        '''
        return True

    def create_settings(self):
        self.pipelineinfo = cps.HTMLText( 
            "",
            "Use the 'Data Tools' menu to open the Cluster View",
            size=(2,2)
        )

    def settings(self):
        return [self.pipelineinfo]

    def post_pipeline_load(self, pipeline):
        '''Fixup any measurement names that might have been ambiguously loaded

        pipeline - for access to other module's measurements
        '''
        pass

    def visible_settings(self):
        return [self.pipelineinfo]

    def run(self):
        pass

    def run_as_data_tool(self):
        frame = ClusterviewFrame(wx.GetApp().frame, 'Cluster View')
        frame.Show()
        pass

    def display(self, workspace, figure):
        pass

    def validate_module(self, pipeline):
        '''Do further validation on this module's settings

        pipeline - this module's pipeline

        Check to make sure the output measurements aren't duplicated
        by prior modules.
        '''
        pass

    def upgrade_settings(self, setting_values, variable_revision_number,
                         module_name, from_matlab):
        return setting_values, variable_revision_number, from_matlab
    
    def volumetric(self):
        return True
