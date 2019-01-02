# -*- coding: future_fstrings -*-

"""
RunOnCluster
================

**RunOnCluster** submits the pipeline and images to run on
an HPC cluster.

This module submits the pipeline to run on an HPC cluster. It
should be placed at the end of an image processing pipeline.

|

============ ============ ===============
Supports 2D? Supports 3D? Respects masks?
============ ============ ===============
YES          YES          NO
============ ============ ===============
"""



import logging
logger = logging.getLogger(__name__)
import numpy as np
import os
import time
import re
import sys
import zlib
import wx
from future import *

import cellprofiler
import cellprofiler.image as cpi
import cellprofiler.module as cpm
import cellprofiler.measurement as cpmeas
import cellprofiler.pipeline as cpp
import cellprofiler.setting as cps
from cellprofiler.setting import YES, NO
import cellprofiler.preferences as cpprefs
import cellprofiler.workspace as cpw

from cellprofiler.measurement import F_BATCH_DATA, F_BATCH_DATA_H5

from rynner.rynner import Rynner
from libsubmit import SSHChannel
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.launchers.launchers import SimpleLauncher
from libsubmit.channels.errors import SSHException, FileCopyException
import tempfile

'''# of settings aside from the mappings'''
S_FIXED_COUNT = 8
'''# of settings per mapping'''
S_PER_MAPPING = 2


class CPRynner(Rynner):
    
    def __init__( self, username = None ):
        # Create a connection
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
            super(CPRynner, self).__init__(provider)
        except SSHException:
            return None
    

            


class RunOnCluster(cpm.Module):
    #
    # How it works:
    #
    # 
    module_name = "RunOnCluster"
    category = 'Other'
    variable_revision_number = 8
    runs = []

    rynner = None

    def create_rynner( self ):
        self.rynner = CPRynner( self.username.value )

    def upload( self, run ):
        try:
            self.rynner.upload(run)
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def download( self, run ):
        try:
            self.rynner.download(run)
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def submit( self, run ):
        try:
            self.rynner.submit(run)
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def get_runs( self ):
        try:
            return self.rynner.get_runs()
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def update( self, runs ):
        try:
            self.rynner.update(runs)
        except FileCopyException as exception:
            self.rynner = None
            raise exception

    def volumetric(self):
        return True

    def create_settings(self):
        '''Create the module settings and name the module'''
        self.username = cps.Text( 
            "Username",
            "",
            doc = "Enter your SCW username",
        )

        self.show_advanced_setting = cps.Binary(
            "Advanced Settings", 
            True, 
            doc="""Show advanced settings.""")

        self.batch_mode = cps.Binary("Hidden: in batch mode", False)
        self.revision = cps.Integer("Hidden: revision number", 0)

    def settings(self):
        result = [
            self.username,
            self.batch_mode,
            self.revision,
        ]
        return result

    def prepare_settings(self, setting_values):
        pass

    def visible_settings(self):
        result = [
            self.username,
        ]
        return result

    def help_settings(self):
        help_settings = [
            self.username,
            self.show_advanced_setting,
        ]

        return help_settings

    def prepare_run(self, workspace):
        '''Invoke the image_set_list pickling mechanism and save the pipeline'''

        pipeline = workspace.pipeline

        if pipeline.test_mode:
            return True
        if self.batch_mode.value:
            return True
        else:
            if self.rynner is None:
                self.create_rynner()

            # save the pipeline
            path = self.save_pipeline(workspace)

            # Create the run data structure
            file_list = pipeline.file_list
            file_list = [name.replace('file:///','') for name in file_list]
            # Add destination folder for the image files
            uploads = [[name, 'images'] for name in file_list]
            uploads +=  [[path,'.']]

            # Define the job to run
            run = self.rynner.create_run( 
                script = 'module load java; mkdir results; cellprofiler -c -p Batch_data.h5 -i images/ -o results 2> results/cellprofiler_output;',
                uploads = uploads,
                downloads =  [['results',cpprefs.get_default_output_directory()]],
            )

            # Copy the pipeline and images accross
            self.upload(run)

            # Submit the run
            self.submit(run)

            # Store submission data
            self.runs += [run]

            wx.MessageBox(
                "RunOnCluster submitted the run to the cluster",
                caption="RunOnCluster: Batch job submitted",
                style=wx.OK | wx.ICON_INFORMATION)
            return False

    def run(self, workspace):
        # The submission happens in prepare run.
        pass

    def validate_module(self, pipeline):
        '''Make sure the module settings are valid'''
        # This must be the last module in the pipeline
        if id(self) != id(pipeline.modules()[-1]):
            raise cps.ValidationError("The RunOnCluster module must be "
                                      "the last in the pipeline.",
                                      self.wants_default_output_directory)

    def validate_module_warnings(self, pipeline):
        '''Warn user re: Test mode '''
        if pipeline.test_mode:
            raise cps.ValidationError("RunOnCluster will not produce output in Test Mode",
                                      self.wants_default_output_directory)

    def alter_path(self, path, **varargs):
        if path == cpprefs.get_default_output_directory():
            path = 'results'
        else:
            path = os.path.join('results', os.path.basename(path))
        path = path.replace('\\', '/')
        return path

    def save_pipeline(self, workspace, outf=None):
        '''Save the pipeline in Batch_data.h5

        Save the pickled image_set_list state in a setting and put this
        module in batch mode.

        if outf is not None, it is used as a file object destination.
        '''

        if outf is None:
            path = cpprefs.get_default_output_directory()
            h5_path = os.path.join(path, F_BATCH_DATA_H5)
        else:
            h5_path = outf

        image_set_list = workspace.image_set_list
        pipeline = workspace.pipeline
        m = cpmeas.Measurements(copy=workspace.measurements,
                                filename=h5_path)
        
        try:
            assert isinstance(pipeline, cpp.Pipeline)
            assert isinstance(m, cpmeas.Measurements)

            orig_pipeline = pipeline
            pipeline = pipeline.copy()
            # this use of workspace.frame is okay, since we're called from
            # prepare_run which happens in the main wx thread.
            target_workspace = cpw.Workspace(pipeline, None, None, None,
                                             m, image_set_list,
                                             workspace.frame)
            # Assuming all results go to the same place, output folder can be set
            # in the script
            pipeline.prepare_to_create_batch(target_workspace, self.alter_path)
            self_copy = pipeline.module(self.module_num)
            self_copy.revision.value = int(re.sub(r"\.|rc\d{1}", "", cellprofiler.__version__))
            self_copy.batch_mode.value = True
            pipeline.write_pipeline_measurement(m)
            orig_pipeline.write_pipeline_measurement(m, user_pipeline=True)

            return h5_path
        finally:
            m.close()

    def upgrade_settings(self, setting_values, variable_revision_number,
                         module_name, from_matlab):
        # The first version of this module was created for CellProfiler
        # version 8. 

        if from_matlab and variable_revision_number == 8:
            # There is no matlab implementation
            raise NotImplementedError("Attempting to import RunOnCluster from Matlab.")
            
        if (not from_matlab) and variable_revision_number == 8:
            pass

        if variable_revision_number < 8:
             # There are no older implementations
             raise NotImplementedError("Importing unkown version of RunOnCluster.")

        return setting_values, variable_revision_number, from_matlab
