
"""
RunOnCluster
================

**RunOnCluster** submits the pipeline and images to run on
an HPC cluster.

The plugin uses the Rynner library, which in turn uses libsubmit,
to copy the input files and the pipeline to the cluster. The image
files are divided into separate run folders for each core, which
will then be processed independently. The download method in the
ClusterView plugin automatically combines these back into a single
result folder.

Should be placed at the end of the image processing pipeline.

|

============ ============ ===============
Supports 2D? Supports 3D? Respects masks?
============ ============ ===============
YES          YES          NO
============ ============ ===============
"""

import os, time, re
from future import *
import logging
logger = logging.getLogger(__name__)
import numpy as np
import wx

import cellprofiler
import cellprofiler.module as cpm
import cellprofiler.measurement as cpmeas
import cellprofiler.pipeline as cpp
import cellprofiler.setting as cps
import cellprofiler.preferences as cpprefs
import cellprofiler.workspace as cpw

from cellprofiler.measurement import F_BATCH_DATA_H5

from CPRynner.CPRynner import CPRynner
from CPRynner.CPRynner import max_tasks as CPRynner_max_tasks


class RunOnCluster(cpm.Module):
    #
    # How it works:
    #
    # 
    module_name = "RunOnCluster"
    category = 'Other'
    variable_revision_number = 8
    runs = []

    def is_create_batch_module(self):
        return True

    def upload( self, run, dialog = None ):
        rynner = CPRynner()

        if dialog == None:
            dialog = wx.GenericProgressDialog("Uploading","Uploading files")
            destroy_dialog = True
        else:
            destroy_dialog = False

        if rynner is not None:
            rynner.start_upload(run)
            maximum = dialog.GetRange()
            while run['upload_status'] < 1:
                value = min( maximum, int(maximum*run['upload_status']) )
                dialog.Update(value)
            dialog.Update(maximum-1)
            if destroy_dialog:
                dialog.Destroy()

    def volumetric(self):
        return True

    def create_settings(self):
        '''Create the module settings and name the module'''
        self.runname = cps.Text( 
            "Run Name",
            "Run_name",
            doc = "Enter a recognizable identifier for the run (spaces will be replaced by undescores)",
        )
        self.n_images_per_measurement = cellprofiler.setting.Integer(
            "Number of images per measurement",
            1,
            minval=1,
            doc = "The number of image files in each measurement that must be present for the pipeline to run correctly. This is usually the number of image types in the NamesAndTypes module."
        )
        self.type_first = cellprofiler.setting.Binary(
            text="Image type first",
            value=True,
            doc= "Wether the images are ordered by image type first. If not, ordering by measurement first is assumed."
        )
        self.max_walltime = cellprofiler.setting.Integer(
            "Maximum Runtime (hours)",
            24,
            doc = "The maximum time for reserving a node on the cluster. Should be higher than the actual runtime, or the run may not compelte. Runs with lower values will pass the queue more quickly."
        )
        self.batch_mode = cps.Binary("Hidden: in batch mode", False)
        self.revision = cps.Integer("Hidden: revision number", 0)

    def settings(self):
        result = [
            self.runname,
            self.n_images_per_measurement,
            self.type_first,
            self.max_walltime,
            self.batch_mode,
            self.revision,
        ]
        return result

    def prepare_settings(self, setting_values):
        pass

    def visible_settings(self):
        result = [
            self.runname,
            self.n_images_per_measurement,
            self.type_first,
            self.max_walltime,
        ]
        return result

    def help_settings(self):
        help_settings = [
            self.runname,
            self.n_images_per_measurement,
            self.type_first,
            self.max_walltime,
        ]

        return help_settings

    def group_images( self, list, n_measurements, measurements_per_run, groups_first = True ):
        ''' Divides a list of images into numbered groups and returns a list enumerated by the group numbers '''
        if groups_first:
            images_per_run = len(list)/n_measurements * measurements_per_run
            return [(int(i/images_per_run), name) for i, name in enumerate(list)]
        else :
            return [(int((i%n_measurements)/measurements_per_run), name) for i, name in enumerate(list)]

    def prepare_run(self, workspace):
        '''Invoke the image_set_list pickling mechanism and save the pipeline'''

        pipeline = workspace.pipeline

        if pipeline.test_mode:
            return True
        if self.batch_mode.value:
            return True
        else:
            rynner = CPRynner()
            if rynner is not None:

                # Set walltime
                rynner.provider.walltime = str(self.max_walltime.value)+":00:00"

                # save the pipeline
                path = self.save_pipeline(workspace)

                # Create the run data structure
                file_list = pipeline.file_list
                file_list = [name.replace('file:///','') for name in file_list]
                file_list = [name.replace('file:','') for name in file_list]
                file_list = [name.replace('%20',' ') for name in file_list]

                if len(file_list) == 0:
                    wx.MessageBox(
                    "No images found. Did you remember to add them to the Images module?",
                    caption="No images",
                    style=wx.OK | wx.ICON_INFORMATION)
                    return False

                # Divide measurements to up to 40 runs
                n_measurements = int(len(file_list)/self.n_images_per_measurement.value)
                measurements_per_run = int(n_measurements/CPRynner_max_tasks) + 1
                grouped_images = self.group_images( file_list, n_measurements, measurements_per_run, self.type_first.value)
                n_image_groups = max(zip(*grouped_images)[0]) + 1

                # Add image files to uploads
                uploads = [[name, 'run{}/images'.format(g)] for g,name in grouped_images]

                # Also add the pipeline
                uploads +=  [[path,'.']]

                # The runs are downloaded in their separate folders. They can be processed later
                output_dir = cpprefs.get_default_output_directory()
                downloads = [['run{}'.format(g),output_dir] for g in range(n_image_groups)]

                # Create run scripts and add to uploads
                for g in range(n_image_groups):
                    runscript_name = 'cellprofiler_run{}'.format(g)
                    local_script_path = os.path.join(rynner.provider.script_dir, runscript_name)

                    with open(local_script_path, "w") as file:
                        n_measurements = len([ i for i in grouped_images if i[0]==g ]) / self.n_images_per_measurement.value
                        file.write("cellprofiler -c -p ../Batch_data.h5 -o results -i images -f 1 -l {} 2>>../cellprofiler_output".format(n_measurements))

                    uploads += [[local_script_path,"run{}".format(g)]]

                # Define the job to run
                run = rynner.create_run( 
                    jobname = self.runname.value.replace(' ','_'),
                    script = 'source /apps/local/life-sciences/CellProfiler/bin/activate; module load java; printf %s\\\\n {{0..{}}} | xargs -P 40 -n 1 -IX bash -c "cd runX ; ./cellprofiler_runX; ";'.format(n_image_groups-1),
                    uploads = uploads,
                    downloads =  downloads,
                )

                # Copy the pipeline and images accross
                dialog = wx.GenericProgressDialog("Uploading","Uploading files",style=wx.PD_APP_MODAL)
                try:
                    self.upload(run, dialog)

                    # Submit the run
                    dialog.Update( dialog.GetRange()-1, "Submitting" )
                    CPRynner().submit(run)

                    # Store submission data
                    self.runs += [run]

                    dialog.Destroy()
                    wx.MessageBox(
                    "RunOnCluster submitted the run to the cluster",
                    caption="RunOnCluster: Batch job submitted",
                    style=wx.OK | wx.ICON_INFORMATION)
                except Exception as e:
                    dialog.Destroy()
                    raise e


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
