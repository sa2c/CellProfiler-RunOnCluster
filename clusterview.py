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

import logging

logger = logging.getLogger(__package__)

import numpy as np
import six
import os, shutil
import csv

import cellprofiler.module as cpm
import cellprofiler.measurement as cpmeas
import cellprofiler.setting as cps
import cellprofiler.preferences as cpprefs
from cellprofiler.setting import YES, NO
from cellprofiler.measurement import R_PARENT, R_CHILD

from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException, FileCopyException
import tempfile

from CPRynner.CPRynner import CPRynner

import wx

    
class ClusterviewFrame(wx.Frame):

    def __init__(self, parent, title):
        super(ClusterviewFrame, self).__init__(parent, title=title, size = (250,400))

        self.rynner = None
        self.check_cluster()
        self.InitUI()
        self.Centre()

    def InitUI(self):

        self.panel = wx.lib.scrolledpanel.ScrolledPanel(self, size = (250,400))

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
        hbox.Add(btn)
        vbox.Add(hbox, flag=wx.ALIGN_LEFT|wx.LEFT, border=10)
        btn.Bind(wx.EVT_BUTTON, self.on_update_click )

        vbox.Add((-1, 5))

        line = wx.StaticLine(self.panel)
        vbox.Add(line, flag=wx.EXPAND|wx.BOTTOM, border=10)

        self.run_displays = []
        for run in sorted(self.runs, key=lambda k: k['upload_time'], reverse = True):
            hbox1 = wx.BoxSizer(wx.HORIZONTAL)
            st = wx.StaticText(self.panel, label=run.job_name+":")
            st.SetFont(font)
            hbox1.Add(st, flag=wx.RIGHT, border=8)
            vbox.Add(hbox1, flag=wx.EXPAND|wx.LEFT|wx.RIGHT|wx.TOP, border=10)

            vbox.Add((-1, 5))

            hbox2 = wx.BoxSizer(wx.HORIZONTAL)
            st2 = wx.StaticText(self.panel, label=run.status)
            st2.SetFont(font)
            hbox2.Add(st2)
            vbox.Add(hbox2, flag=wx.LEFT | wx.TOP, border=10)

            vbox.Add((-1, 5))

            hbox3 = wx.BoxSizer(wx.HORIZONTAL)
            btn = wx.Button(self.panel, label='Download Results', size=(110, 30))
            btn.Bind(wx.EVT_BUTTON, lambda e, r=run: self.on_download_click( e, r ) )
            hbox3.Add(btn)
            vbox.Add(hbox3, flag=wx.ALIGN_RIGHT|wx.RIGHT, border=10)

    def on_download_click(self, event, run):
        self.download(run)

    def on_update_click( self, event ):
        self.update()
        self.vbox.Clear(True)
        self.build_view(self.vbox)
        self.vbox.Layout() 
        self.FitInside()

    def get_runs( self ):
        try:
            return self.rynner.get_runs()
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def update( self ):
        if self.rynner is None:
            self.rynner = CPRynner()
        self.runs = [ r for r in self.get_runs() if 'upload_time' in r ]
        self.rynner.update(self.runs)

    def check_cluster( self ):
        '''Get all runs from the cluster and list in the UI'''
        if self.rynner is None:
            self.rynner = CPRynner()
        self.update()

    def download( self, run ):
        if self.rynner is None:
            self.rynner = CPRynner()

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

        try:
            tmpdir = tempfile.mkdtemp()
            # Define the job to run
            run.downloads = [ [d[0], tmpdir] for d in run.downloads ]
            self.rynner.download(run)
            
        except FileCopyException as exception:
            self.rynner = None
            raise exception
        
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
                    shutil.move( filename,  )
            except Exception as e:
                print(e)
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
        outfile = open(destination,"a")
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
    variable_revision_number = 2

    def create_settings(self):
        pass   

    def settings(self):
        result = []

        return result

    def post_pipeline_load(self, pipeline):
        '''Fixup any measurement names that might have been ambiguously loaded

        pipeline - for access to other module's measurements
        '''
        pass

    def visible_settings(self):
        result = []

        return result

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
