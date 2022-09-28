"""
ClusterView
=============

**Clusterview** displays information about queued and completed runs
started using Rynner on the cluster and allow downloading result files.
Expect the folder structure created by the RunOnCluster plugin.

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
import os, time, shutil
import tempfile
import timeago, datetime
import wx

import cellprofiler_core.module as cpm
import cellprofiler_core.setting as cps
import cellprofiler_core.preferences as cpprefs

import CPRynner.CPRynner as CPRynner


class YesToAllMessageDialog(wx.Dialog):
    """
    A message dialog with "yes", "no" and "yes to all" buttons, returning
    wx.ID_YES, wx.ID_NO and wx.ID_YESTOALL respectively
    """
    def __init__(self, parent, message, title):
        super(YesToAllMessageDialog, self).__init__(parent, title=title, size = (310,210) )
        self.panel = wx.Panel(self)

        # First the message text        
        text_sizer = wx.BoxSizer(wx.HORIZONTAL)
        stmessage = wx.StaticText(self.panel, 11, message, size=(310,110))
        stmessage.Wrap(300)
        text_sizer.Add(stmessage, 0, wx.ALL , 5)

        # Three buttons with the appropriate labels
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.yes_btn = wx.Button(self.panel, wx.ID_YES, label="Yes", size=(60, 30))
        button_sizer.Add(self.yes_btn, 0, wx.ALL , 5)
        self.no_btn = wx.Button(self.panel, wx.ID_NO, label="No", size=(60, 30))
        button_sizer.Add(self.no_btn, 0, wx.ALL , 5)
        self.yestoall_btn = wx.Button(self.panel, wx.ID_YESTOALL, label="Yes to All", size=(90, 30))
        button_sizer.Add(self.yestoall_btn, 0, wx.ALL , 5)

        # Bind the buttons to functions
        self.yes_btn.Bind(wx.EVT_BUTTON, self.on_yes)
        self.no_btn.Bind(wx.EVT_BUTTON, self.on_no)
        self.yestoall_btn.Bind(wx.EVT_BUTTON, self.on_yes_to_all)

        # Create a primary sizer and add the text and button sizers
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(text_sizer, 0, wx.ALL, 5)
        main_sizer.Add(button_sizer, 0, wx.ALL | wx.ALIGN_CENTER, 5)
        self.panel.SetSizer(main_sizer)
        self.panel.Fit()
    
    def on_yes(self, event):
        # On 'yes' button click return wx.ID_YES
        self.EndModal(wx.ID_YES)
        self.Destroy()

    def on_no(self, event):
        # On 'no' button click return wx.ID_NO
        self.EndModal(wx.ID_NO)
        self.Destroy()

    def on_yes_to_all(self, event):
        # On 'yes to all' button click return wx.ID_YESTOALL
        self.EndModal(wx.ID_YESTOALL)
        self.Destroy()


class ClusterviewFrame(wx.Frame):
    """
    A frame containing information on queued and accomplished runs,
    update and logout buttons and a download button for each run
    """

    def __init__(self, parent, title):
        # First update runs, then create the window
        super(ClusterviewFrame, self).__init__(parent, title=title, size = (400,400))
        self.update_time = datetime.datetime.now()
        self.update()
        self.InitUI()
        self.Centre()

    def InitUI(self):
        # The containers in the window are organised here
        self.panel = wx.lib.scrolledpanel.ScrolledPanel(self)
        self.panel.SetBackgroundColour('#ededed')
        self.vbox = wx.BoxSizer(wx.VERTICAL)

        self.build_view(self.vbox)

        self.panel.SetSizer(self.vbox)
        self.panel.SetAutoLayout(1)
        self.panel.SetupScrolling(scroll_x=False, scroll_y=True)

    def update( self ):
        """
        Update the run list
        """
        rynner = CPRynner.CPRynner()
        if rynner is not None:
            self.runs = [ r for r in rynner.get_runs() if 'upload_time' in r ]
            rynner.update(self.runs)
            for run in self.runs:
                run['status_time'] = rynner.read_time(run)
            self.update_time = datetime.datetime.now()
        else:
            self.runs = []

    def build_view(self, vbox):
        # Build the contents for the window

        font = wx.SystemSettings.GetFont(wx.SYS_SYSTEM_FONT)
        font.SetPointSize(9)

        # Margin
        vbox.Add((-1, 5))

        # The update button and info
        btn = wx.Button(self.panel, label='Update', size=(90, 30))
        btn.Bind(wx.EVT_BUTTON, self.on_update_click )

        update_time_text = wx.StaticText(self.panel, label="")
        update_time_text.SetFont(font)
        # Set a timer to update the time since update -text
        self.set_timer(update_time_text)

        # Add the button and text to a sizer
        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add(btn, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        hbox.Add(update_time_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        vbox.Add(hbox, 0, wx.EXPAND, 10)

        # The logout and settings buttons in a separate sizer
        logout_btn = wx.Button(self.panel, label='Logout', size=(90, 30))
        logout_btn.Bind(wx.EVT_BUTTON, self.on_logout_click )
        settings_btn = wx.Button(self.panel, label='Cluster Settings', size=(90, 30))
        settings_btn.Bind(wx.EVT_BUTTON, 
        self.on_cluster_settings_click)
        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add((0,0), 1, wx.ALIGN_CENTER_VERTICAL)
        hbox.Add(logout_btn, 0, wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, 8)
        hbox.Add(settings_btn, 0, wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, 8)
        vbox.Add(hbox, 0, wx.EXPAND, 10)

        # Margin and a separator
        vbox.Add((-1, 5))
        line = wx.StaticLine(self.panel)
        vbox.Add(line, 0, wx.EXPAND, 10)

        # Add a display for all runs in history
        self.run_displays = []
        for run in sorted(self.runs, key=lambda k: k['upload_time'], reverse = True):
            # Run name
            st = wx.StaticText(self.panel, label=run.job_name+":")
            st.SetFont(font)
            hbox1 = wx.BoxSizer(wx.HORIZONTAL)
            hbox1.Add(st, flag=wx.RIGHT, border=8)
            vbox.Add(hbox1, flag=wx.EXPAND|wx.LEFT|wx.RIGHT|wx.TOP)
            
            # The state of the run
            hbox2 = wx.BoxSizer(wx.HORIZONTAL)
            time_since = int(datetime.datetime.fromtimestamp(int(run['status_time']))) #Error 1
            st2 = wx.StaticText( self.panel,
                label=run.status+" since " + time_since
            )
            hbox2.Add(st2)
            vbox.Add(hbox2, flag=wx.LEFT | wx.TOP, border=10)

            if run.status == 'PENDING':
                starttime = run['starttime']
                hbox3 = wx.BoxSizer(wx.HORIZONTAL)
                st3 = wx.StaticText( self.panel,
                    label="Estimated start time " + starttime
                )
                hbox3.Add(st3)
                vbox.Add(hbox3, flag=wx.LEFT | wx.TOP, border=10)

            # Padding
            vbox.Add((-1, 5))

            # The download button
            if run.status == 'COMPLETED':
                if hasattr(run, 'downloaded') and run.downloaded:
                    label = 'Download Again'
                else:
                    label = 'Download Results'
                btn = wx.Button(self.panel, label=label, size=(130, 40))
                btn.Bind(wx.EVT_BUTTON, lambda e, r=run: self.on_download_click( e, r ) )
                hbox3 = wx.BoxSizer(wx.HORIZONTAL)
                hbox3.Add(btn)
                vbox.Add(hbox3, flag=wx.ALIGN_RIGHT|wx.RIGHT, border=10)

    def set_timer(self, element):
        """
        Set a timer to update the time since last update
        """
        def update_st(event):
            element.SetLabel("Last updated: "+timeago.format(self.update_time, locale='en_GB'))
        def close(event):
            self.timer.Stop()
            self.Destroy()
        self.timer = wx.Timer(self)
        self.timer.Start(1000)
        self.Bind(wx.EVT_TIMER, update_st, self.timer)
        wx.EVT_CLOSE(self, close)

    def on_download_click(self, event, run):
        self.download(run)

    def on_update_click( self, event ):
        """
        Update runs and rebuild the layout
        """
        self.update()
        self.draw()

    def on_logout_click( self, event ):
        CPRynner.logout()
        self.runs = []

    def on_cluster_settings_click(self, event):
        cluster_address_orig = CPRynner.cluster_url()
        CPRynner.update_cluster_parameters()
        cluster_address_new = CPRynner.cluster_url()

        if cluster_address_orig != cluster_address_new:
            CPRynner.logout()
        self.runs = []
        self.update()
        self.draw()

    def draw(self):
        self.vbox.Clear(True)
        self.build_view(self.vbox)
        self.vbox.Layout()
        self.FitInside()

    def download( self, run ):
        """
        Ask for a destination folder, download files in the results
        folders and move to the destination
        """
        target_directory = self.ask_for_output_dir()
        if not target_directory:
            return False
            
        # Download into a temporary directory
        tmpdir = tempfile.mkdtemp()
        self.download_to_tempdir(run, tmpdir)
        
        # Move the files to the selected folder, handling file names and csv files
        self.download_file_handling_setup()
        has_been_downloaded = hasattr(run, 'downloaded') and run.downloaded
        for runfolder, localdir in run.downloads:
            self.handle_result_file( 
                os.path.join(localdir, runfolder, 'results'),
                target_directory,
                has_been_downloaded
            )

        # Set a flag marking the run downloaded
        run['downloaded'] = True
        CPRynner.CPRynner().save_run_config( run )

        self.update()
        self.draw()

    def ask_for_output_dir(self):
        """
        Ask for a destination for the downloaded files
        """
        default_target = cpprefs.get_default_output_directory()
        dialog = wx.DirDialog (None, "Choose an output directory", default_target,
                    wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        try:
            if dialog.ShowModal() == wx.ID_CANCEL:
                return False
            target_directory = dialog.GetPath()
        except Exception:
            wx.LogError('Failed to open directory!')
            raise
        finally:
            dialog.Destroy()
        return target_directory

    def download_to_tempdir(self, run, tmpdir):
        """
        Actually download the files from the cluster into tmpdir,
        showing a progress dialog
        """
        run.downloads = [ [d[0], tmpdir] for d in run.downloads ]
        CPRynner.CPRynner().start_download(run)
        dialog = wx.GenericProgressDialog("Downloading","Downloading files")
        maximum = dialog.GetRange()
        while run['download_status'] < 1:
            value = min( maximum, int(maximum*run['download_status']) )
            dialog.Update(value)
            time.sleep(0.04)
        dialog.Destroy()

    def download_file_handling_setup(self):
        self.csv_dict = {}
        self.yes_to_all_clicked = False

    def rename_file(self, name):
        """
        Add a number at the end of a filename to create a unique new name
        """
        stripped_name, suffix = os.path.splitext(name)
        n=2
        new_name = stripped_name + '_' +str(n)+suffix
        while os.path.isfile(new_name):
            n += 1
            new_name = stripped_name + '_' +str(n)+suffix
        return new_name
    
    def handle_result_file( self, filename, target_directory, has_been_downloaded ):
        """
        Recursively check result files and move to the target directory. Handle conflicting file names
        and csv files

        Each run will create the same set of csv files to contain the measurement info. These need to be
        combined into one and the image numbers need to be fixed. We will ask how the files should be handled
        once for each file name and remember the answer in self.csv_dict
        """
        if os.path.isdir(filename):
            # Recursively walk directories
            for f in os.listdir(filename):
                self.handle_result_file( os.path.join(filename, f), target_directory, has_been_downloaded)
        else:
            # Handle an actual file
            name = os.path.basename(filename)
            target_file = os.path.join(target_directory, name)
            try:
                if not os.path.isfile(target_file):
                    # No file name conflict, just move
                    shutil.move( filename, target_directory )
                    if filename.endswith('.csv'):
                        # File is .csv, we need to remember this one has been handled already
                        self.csv_dict[name] = name
                elif name.endswith('.csv'):
                    # File exists and is csv. Ask the user whether to append or to create a new file
                    if name not in self.csv_dict:
                        append = self.ask_csv_append(name, has_been_downloaded)
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

    def ask_csv_append(self, name, has_been_downloaded):
        if self.yes_to_all_clicked:
            return True

        message = 'The file '+name+' already exists. Append to the existing file?'
        if has_been_downloaded:
            message +=  ' This file has already been downloaded and appending may result in dublication of data.'
            dialog = YesToAllMessageDialog(self, message, 'Append to File')
        else:
            dialog = YesToAllMessageDialog(self, message, 'Append to File')
        answer = dialog.ShowModal()

        if answer == wx.ID_NO:
            return False
        if answer == wx.ID_YESTOALL:
            self.yes_to_all_clicked = True
        return True
        

    def handle_csv( self, source, destination ):
        """
        Write the data rows of a csv file into an existing csv file.
        Fix image numbering before writing 
        """

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

    def __init__(self):
        super().__init__()

    def create_settings(self):
        
        self.run_names = ["None"]

        doc_ = "Bring up old ClusterView frame."
        self.frame_button = cps.do_something.DoSomething(
            "", "Cluster View", self.run_as_data_tool, doc = doc_)

        doc_ = "Select a CellProfiler job that has been run on the cluster."
        self.choose_run = cps.choice.Choice(
            "Select CellProfiler cluster run.", choices=self.run_names, value="None", doc = doc_)

        doc_ = "Update the module's internal list of CellProfiler cluster runs."
        self.update_button = cps.do_something.DoSomething(
            "Update job list.", "Update", self.update_module, doc = doc_) 
        
        doc_ = "View the status of the currently selected cluster job. Opens new window."
        self.status_button = cps.do_something.DoSomething(
            "View job status.", "Status", self.run_status_window, doc = doc_)

        doc_ = "Update the cluster login settings."
        self.settings_button = cps.do_something.DoSomething(
            "Change cluster settings.", "Settings", self.on_settings_click, doc = doc_)

        doc_ = "Log out from the cluster. Log back in by updating cluster settings."
        self.logout_button = cps.do_something.DoSomething(
            "Logout from cluster.", "Logout", self.on_logout_click, doc = doc_)

    def settings(self):
        result = [
            self.frame_button,
            self.choose_run,
            self.update_button,
            self.settings_button,
            self.logout_button,
            self.status_button
        ]
        return result

    def post_pipeline_load(self, pipeline):
        """
        Fixup any measurement names that might have been ambiguously loaded

        pipeline - for access to other module's measurements
        """
        pass

    def visible_settings(self):
        result = [
            self.frame_button,
            self.choose_run,
            self.update_button,
            self.settings_button,
            self.logout_button
        ]
        if self.choose_run.value != "None":
            result += [self.status_button]
        return result

    def run(self):
        pass

    def run_as_data_tool(self):
        frame = ClusterviewFrame(wx.GetApp().frame, 'Cluster View')
        frame.Show()
        pass

    def update_module(self):
        """
        Update the run list
        """
        rynner = CPRynner.CPRynner()
        if rynner is not None:
            self.runs = [ r for r in rynner.get_runs() if 'upload_time' in r ]
            rynner.update(self.runs)
            for run in self.runs:
                run['status_time'] = rynner.read_time(run)
            self.update_time = datetime.datetime.now()
        else:
            self.runs = []
        self.run_names = []
        for r in self.runs:
            self.run_names[r] = self.runs.job_name[r]
        self.run_names += ["None"]

    def on_logout_click( self ):
        CPRynner.logout()
        self.runs = []
        self.run_names = ["None"]

    def on_settings_click(self):
        cluster_address_orig = CPRynner.cluster_url()
        CPRynner.update_cluster_parameters()
        cluster_address_new = CPRynner.cluster_url()

        if cluster_address_orig != cluster_address_new:
            CPRynner.logout()
        self.runs = []
        self.update_module()

    def run_status_window(self):
        run = self.runs[self.choose_run.index(self.choose_run.value)]
        frame = RunStatusFrame(wx.GetApp().frame,self.choose_run.value,run)
        frame.Show()
        pass

    def validate_module(self, pipeline):
        """
        Do further validation on this module's settings

        pipeline - this module's pipeline

        Check to make sure the output measurements aren't duplicated
        by prior modules.
        """
        pass

    # def upgrade_settings(self, setting_values, variable_revision_number,
    #                      module_name, from_matlab):
    #     return setting_values, variable_revision_number, from_matlab
    
    def volumetric(self):
        return True

class RunStatusFrame(wx.Frame):
    """
    A pop-up window that displays the state of the chosen run and offers a download button if completed.
    """
    def __init__(self, parent, title, run):
        super(RunStatusFrame, self).__init__(parent, title = title, size = (400,200))
        self.run = run
        self.run_complete = False
        self.download_num = 0
        self.InitUI()
        self.Centre()

    def InitUI(self):
        font = wx.SystemSettings.GetFont(wx.SYS_SYSTEM_FONT)
        font.SetPointSize(9)

        self.panel = wx.Panel(self)
        self.panel.SetBackgroundColour('#ededed')

        run_text = wx.StaticText(self.panel, label=("Run name: "+self.run.job_name))
        run_text.SetFont(font)
        self.status_text = wx.StaticText(self.panel, label="Run status:   PENDING")
        self.status_text.SetFont(font)           

        # Create a download button                
        self.download_number_text = wx.StaticText(self.panel, label="Times downloaded:   0")
        self.download_number_text.SetFont(font)
        self.download_avail_text = wx.StaticText(self.panel, label="Download available?:   False")
        self.download_avail_text.SetFont(font)
        self.download_button = wx.Button(self.panel, label='Download', size=(90,30))
        self.download_button.Bind(wx.EVT_BUTTON, self.on_download_click)
        self.download_button.SetFont(font)
        self.download_button.Disable()
        
        # Update text and button:         
        update_button = wx.Button(self.panel, label='Update', size=(90, 30))
        update_button.Bind(wx.EVT_BUTTON, self.on_update_click)
        update_button.SetFont(font)
        self.update_time = datetime.datetime.now()
        update_text = wx.StaticText(self.panel, label="")
        update_text.SetFont(font)
        self.set_timer(update_text)
        
        self.vbox = wx.BoxSizer(wx.VERTICAL)        
        self.vbox.Add((-1, 5))
        # Horizontal line for separation
        vline = wx.StaticLine(self.panel, style=wx.LI_VERTICAL)
        run_hbox = wx.BoxSizer(wx.HORIZONTAL)
        run_hbox.Add(run_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        run_hbox.Add(vline, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        run_hbox.Add(self.status_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        # Add the run name to the box sizer.
        self.vbox.Add(run_hbox, 0, wx.EXPAND, 10)
        # Vertical line for spacing
        self.vbox.Add((-1, 5))
        hline = wx.StaticLine(self.panel)
        self.vbox.Add(hline, 0, wx.EXPAND, 10)    
        # Incorporate the update button and text into a single box sizer
        update_hbox = wx.BoxSizer(wx.HORIZONTAL)
        update_hbox.Add(update_button, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)  
        update_hbox.Add(update_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)              
        # Add the update box sizer to the main box sizer 
        self.vbox.Add((-1, 5))
        self.vbox.Add(update_hbox, 0, wx.EXPAND, 10)
        # Another vertical line for spacing
        hline = wx.StaticLine(self.panel)
        self.vbox.Add(hline, 0, wx.EXPAND, 10)
        # Want some vertical alignment, so putting in another vbox
        download_vbox = wx.BoxSizer(wx.VERTICAL)
        download_hbox = wx.BoxSizer(wx.HORIZONTAL)
        download_hbox.Add(self.download_button, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        download_hbox.Add(self.download_avail_text, 0, wx.LEFT|wx.ALIGN_CENTER_VERTICAL, 8)
        download_vbox.Add((-1,5))
        download_vbox.Add(download_hbox, 0, wx.EXPAND, 10)
        hline = wx.StaticLine(self.panel)
        download_vbox.Add((-1,5))
        download_vbox.Add(hline, 0, wx.EXPAND, 10)
        download_vbox.Add(self.download_number_text, 0, wx.EXPAND, 10)

        self.vbox.Add(download_vbox, 0, wx.EXPAND, 10)

        self.panel.SetSizer(self.vbox)
        self.panel.SetAutoLayout(1)

    def on_update_click(self, event):
        self.update_time = datetime.datetime.now()
        self.status_text.SetLabel("Run status:  COMPLETE")
        self.run_complete = True
        self.download_avail_text.SetLabel("Download available?:   "+str(self.run_complete))
        self.download_button.Enable()

    def on_download_click(self, event):
        if self.run_complete == True:
            self.download_num += 1
            self.download_number_text.SetLabel("Times downloaded:   "+str(self.download_num))

    def set_timer(self, element):
        """
        Set a timer to update the time since last update
        """
        def update_st(event):
            element.SetLabel("Last updated: "+timeago.format(self.update_time, locale='en_GB'))
        def close(event):
            self.timer.Stop()
            self.Destroy()
        self.timer = wx.Timer(self)
        self.timer.Start(1000)
        self.Bind(wx.EVT_TIMER, update_st, self.timer)
        wx.EVT_CLOSE(self, close)
