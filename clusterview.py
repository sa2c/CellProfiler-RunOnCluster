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
import timeago, datetime

import CPRynner.CPRynner as CPRynner

import wx


class YesToAllMessageDialog(wx.Dialog):
    def __init__(self, parent, message, title):
        super(YesToAllMessageDialog, self).__init__(parent, title=title, size = (310,160) )

        self.panel = wx.Panel(self)
        
        text_sizer = wx.BoxSizer(wx.HORIZONTAL)
        stmessage = wx.StaticText(self.panel, 11, message)
        stmessage.Wrap(300)
        text_sizer.Add(stmessage, 0, wx.ALL , 5)

        button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.yes_btn = wx.Button(self.panel, wx.ID_YES, label="Yes", size=(60, 30))
        button_sizer.Add(self.yes_btn, 0, wx.ALL , 5)
        self.no_btn = wx.Button(self.panel, wx.ID_NO, label="No", size=(60, 30))
        button_sizer.Add(self.no_btn, 0, wx.ALL , 5)
        self.yestoall_btn = wx.Button(self.panel, wx.ID_YESTOALL, label="Yes to All", size=(60, 30))
        button_sizer.Add(self.yestoall_btn, 0, wx.ALL , 5)

        self.yes_btn.Bind(wx.EVT_BUTTON, self.on_yes)
        self.no_btn.Bind(wx.EVT_BUTTON, self.on_no)
        self.yestoall_btn.Bind(wx.EVT_BUTTON, self.on_yes_to_all)

        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(text_sizer, 0, wx.ALL, 5)
        main_sizer.Add(button_sizer, 0, wx.ALL | wx.ALIGN_CENTER, 5)
        self.panel.SetSizer(main_sizer)
        self.panel.Fit()
    
    def on_yes(self, event):
        self.SetReturnCode(wx.ID_YES)
        self.Destroy()

    def on_no(self, event):
        self.SetReturnCode(wx.ID_NO)
        self.Destroy()

    def on_yes_to_all(self, event):
        self.SetReturnCode(wx.ID_YESTOALL)
        self.Destroy()


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
            if hasattr(run, 'downloaded') and run.downloaded:
                label = 'Download Again'
            else:
                label = 'Download Results'
            btn = wx.Button(self.panel, label=label, size=(110, 30))
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
            for run in self.runs:
                run['status_time'] = rynner.read_time(run)
            self.update_time = datetime.datetime.now()
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
            
        self.csv_dict = {}
        self.yes_to_all_clicked = False
        has_been_downloaded = hasattr(run, 'downloaded') and run.downloaded
        for runfolder, localdir in run.downloads:
            self.handle_result_file( 
                os.path.join(localdir, runfolder, 'results'),
                target_directory,
                has_been_downloaded
            )

        # Set a flag marking the run downloaded
        run['downloaded'] = True
        rynner.save_run_config( run )

        self.update()

    def rename_file(self, name):
        '''
        Add a number at the end of a filename to create a unique new name
        '''
        stripped_name, suffix = os.path.splitext(name)
        n=2
        new_name = stripped_name + '_' +str(n)+suffix
        while os.path.isfile(new_name):
            n += 1
            new_name = stripped_name + '_' +str(n)+suffix
        return new_name
    
    def handle_result_file( self, filename, target_directory, has_been_downloaded ):
        if os.path.isdir(filename):
            for f in os.listdir(filename):
                self.handle_result_file( os.path.join(filename, f), target_directory, has_been_downloaded)
        else:
            name = os.path.basename(filename)
            target_file = os.path.join(target_directory, name)
            try:
                if not os.path.isfile(target_file):
                    # File does not exist
                    shutil.move( filename, target_directory )
                    if filename.endswith('.csv'):
                        self.csv_dict[name] = name
                elif name.endswith('.csv'):
                    # File exists and is csv
                    append = False
                    if name not in self.csv_dict:
                        if not self.yes_to_all_clicked:
                            if has_been_downloaded:
                                dialog = YesToAllMessageDialog(self, 'The file '+name+' already exists. Append to the existing file? This file has already been downloaded and appending may result in dublication of data.', 'Append to File')
                            else:
                                dialog = YesToAllMessageDialog(self, 'The file '+name+' already exists. Append to the existing file?', 'Append to File')
                            answer = dialog.ShowModal()
                            if answer == wx.ID_YES:
                                append = True
                            if answer == wx.ID_YESTOALL:
                                append = True
                                self.yes_to_all_clicked = True
                        else:
                            append = True
    
                        if append:
                            self.csv_dict[name] = name
                            self.handle_csv( filename, os.path.join(target_directory, name) )
                        else:
                            self.csv_dict[name] = self.rename_file(name)
                            shutil.move( filename, os.path.join(target_directory, self.csv_dict[name]))
                    else:
                        self.handle_csv( filename, os.path.join(target_directory, self.csv_dict[name]))
                else:
                    # File exists, use a new name
                    new_name = self.rename_file(name)
                    shutil.move( filename, os.path.join(target_directory, new_name))
            except Exception as e:
                print(e)
                wx.MessageBox(
                    "Failed to move a file to the destination",
                    caption="File error",
                    style=wx.OK | wx.ICON_INFORMATION)
                raise(e)

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
