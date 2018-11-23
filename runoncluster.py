# coding=utf-8

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

# Questions:
# BATCH_PROCESSING_HELP_REF
# F_BATCH_DATA, F_BATCH_DATA_H5

# Cluster settings need to be added and saved as preferences
#  (not per pipeline)
# The output files for each module should be mapped to a cluster directory
#  and copied to the correct place a the end
# For now asume all results go to the same place

from cellprofiler.gui.help.content import BATCH_PROCESSING_HELP_REF

import logging
logger = logging.getLogger(__name__)
import numpy as np
import os
import re
import sys
import zlib

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

'''# of settings aside from the mappings'''
S_FIXED_COUNT = 8
'''# of settings per mapping'''
S_PER_MAPPING = 2


class RunOnCluster(cpm.Module):
    #
    # How it works:
    #
    # 
    module_name = "RunOnCluster"
    category = 'Other'
    variable_revision_number = 8

    def volumetric(self):
        return True

    #
    def create_settings(self):
        '''Create the module settings and name the module'''
        self.show_advanced_setting = cps.Binary(
                "Advanced Settings", True, doc="""Show advanced settings.""")

        self.batch_mode = cps.Binary("Hidden: in batch mode", False)

        self.default_image_directory = cps.Setting("Hidden: default input folder at time of run",
                cpprefs.get_default_image_directory())
        self.revision = cps.Integer("Hidden: revision number", 0)

    def settings(self):
        result = [self.show_advanced_setting,self.batch_mode,
                  self.default_image_directory, self.revision]
        return result

    def prepare_settings(self, setting_values):
        pass

    def visible_settings(self):
        result = [self.show_advanced_setting]
        return result

    def help_settings(self):
        help_settings = [
            self.wants_default_output_directory,
            self.custom_output_directory]

        return help_settings

    def prepare_run(self, workspace):
        '''Invoke the image_set_list pickling mechanism and save the pipeline'''

        pipeline = workspace.pipeline
        image_set_list = workspace.image_set_list

        if pipeline.test_mode:
            return True
        if self.batch_mode.value:
            return True
        else:
            # save the pipeline
            print( pipeline, image_set_list, workspace )
            path = self.save_pipeline(workspace)
            # Create the run data structure
            # Copy the pipeline and images accross
            # Submit the run
            # Store submission data
            if not cpprefs.get_headless():
                import wx
                wx.MessageBox(
                    "RunOnCluster submitted the run to the cluster",
                    caption="RunOnCluster: Batch job submitted",
                    style=wx.OK | wx.ICON_INFORMATION)
            return False

    def run(self, workspace):
        # The submission happens in prepare run.
        # If running in batch mode, do nothing here
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
            #pipeline.prepare_to_create_batch(target_workspace, self.alter_path)
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
