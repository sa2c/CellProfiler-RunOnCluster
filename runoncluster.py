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

#import sys
#sys.path.append('C:\\Users\\tianyi.pan\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages')

import cellprofiler_core
import cellprofiler_core.module as cpm
import cellprofiler_core.measurement as cpmeas
import cellprofiler_core.pipeline as cpp
import cellprofiler_core.setting as cps
import cellprofiler_core.preferences as cpprefs
import cellprofiler_core.workspace as cpw

from cellprofiler_core.constants.measurement import F_BATCH_DATA_H5

from CPRynner.CPRynner import CPRynner
from CPRynner.CPRynner import update_cluster_parameters
from CPRynner.CPRynner import cluster_tasks_per_node
from CPRynner.CPRynner import cluster_setup_script
from CPRynner.CPRynner import cluster_max_runtime

class RunOnCluster(cpm.Module):
    module_name = "RunOnCluster"
    category = 'Other'
    variable_revision_number = 9

    def __init__(self):
        super().__init__()

    def update_settings(self, setting: list):
        pass

    @staticmethod
    def is_create_batch_module():
        return True

    @staticmethod
    def upload(run, dialog=None):
        rynner = CPRynner()

        if dialog is None:
            dialog = wx.GenericProgressDialog("Uploading", "Uploading files")
        destroy_dialog = True if dialog is None else False

        if rynner is not None:
            rynner.start_upload(run)
            maximum = dialog.GetRange()
            while run['upload_status'] < 1:
                value = min(maximum, int(maximum * run['upload_status']))
                dialog.Update(value)
            dialog.Update(maximum - 1)
            if destroy_dialog:
                dialog.Destroy()

    def volumetric(self):
        return True

    def create_settings(self):
        """Create the module settings and name the module"""

        doc_ = (f"Enter a recognizable identifier for the run "
                f"(spaces will be replaced by undescores)")
        self.runname = cps.text.Text("Run Name", "Run_name", doc=doc_)

        doc_ = (f"The number of image files in each measurement that must be "
                f"present for the pipeline to run correctly. This is usually "
                f"the number of image types in the NamesAndTypes module.")
        self.n_images_per_measurement = cps.text.Integer(
            "Number of images per measurement", 1, minval=1, doc=doc_)

        doc_ = (f"Wether the images are ordered by image type first. "
                f"If not, ordering by measurement first is assumed.")
        self.type_first = cps.Binary(text="Image type first", value=True, doc=doc_)

        doc_ = (f"Set to Yes if the the images are included as a single image "
                f"archive, such as an Ism file.")
        self.is_archive = cps.Binary(text="Is image archive", value=False, doc=doc_)

        doc_ = "The number of measurements in the archive file."
        self.measurements_in_archive = cps.text.Integer(
            "Number of measurements in the archive", 1, minval=1, doc=doc_)

        doc_ = (f"The maximum time for reserving a node  on the cluster. Should"
                f" be higher than the actual runtime, or the run may not "
                f"complete. Runs with lower values will pass the queue "
                f"more quickly.")
        self.max_walltime = cps.text.Integer("Maximum Runtime (hours)", 24, doc=doc_)

        doc_ = (f"Enter a project code of an Supercomputing Wales project you "
                f"wish to run under. This can  be left empty if you have only "
                f"one project.")
        self.account = cps.text.Text("Project Code", "", doc=doc_)

        doc_ = "Change cluster and edit cluster settings."
        self.cluster_settings_button = cps.do_something.DoSomething(
            "", "Cluster Settings", update_cluster_parameters, doc=doc_)

        self.batch_mode = cps.Binary("Hidden: in batch mode", False)
        self.revision = cps.text.Integer("Hidden: revision number", 0)

    def settings(self):
        result = [
            self.runname,
            self.is_archive,
            self.n_images_per_measurement,
            self.type_first,
            self.measurements_in_archive,
            self.max_walltime,
            self.account,
            self.batch_mode,
            self.revision,
        ]
        return result

    def prepare_settings(self, setting_values):
        pass

    def visible_settings(self):
        result = [
            self.runname,
            self.is_archive,
        ]

        if self.is_archive.value:
            result += [self.measurements_in_archive]
        else:
            result += [
                self.n_images_per_measurement,
                self.type_first,
            ]

        result += [
            self.max_walltime,
            self.account,
            self.cluster_settings_button,
        ]
        return result

    def help_settings(self):
        help_settings = [
            self.runname,
            self.n_images_per_measurement,
            self.type_first,
            self.is_archive,
            self.measurements_in_archive,
            self.max_walltime,
            self.account,
        ]

        return help_settings

    @staticmethod
    def group_images(list_, n_measurements, measurements_per_run,
                     groups_first=True):
        """Divides a list of images into numbered groups
        and returns a list enumerated by the group numbers """

        if groups_first:
            images_per_run = len(list_) / n_measurements * measurements_per_run
            return [(int(i / images_per_run), name) for i, name in
                    enumerate(list_)]
        else:
            return [(int((i % n_measurements) / measurements_per_run), name) for
                    i, name in enumerate(list_)]

    def prepare_run(self, workspace):
        """Invoke the image_set_list pickling mechanism and save the pipeline"""

        pipeline = workspace.pipeline

        if pipeline.test_mode:
            return True
        if self.batch_mode.value:
            return True
        else:
            rynner = CPRynner()
            if rynner is not None:
                # Get parameters
                max_tasks = int(cluster_tasks_per_node())
                setup_script = cluster_setup_script()

                # Set walltime
                rynner.provider.walltime = str(
                    self.max_walltime.value) + ":00:00"

                # save the pipeline
                path = self.save_pipeline(workspace)

                # Create the run data structure
                file_list = pipeline.file_list
                file_list = [name.replace('file:///', '') for name in file_list]
                file_list = [name.replace('file:', '') for name in file_list]
                file_list = [name.replace('%20', ' ') for name in file_list]

                if len(file_list) == 0:
                    wx.MessageBox(
                        (f"No images found. Did you remember to add them to the"
                         f" Images module?"),
                        caption="No images", style=wx.OK | wx.ICON_INFORMATION)
                    return False

                # Divide measurements to runs
                # according to the number of cores on a node
                n_images = len(file_list)

                if not self.is_archive.value:
                    n_measurements = int(
                        n_images / self.n_images_per_measurement.value)
                    measurements_per_run = int(n_measurements / max_tasks) + 1

                    grouped_images = self.group_images(file_list,
                                                       n_measurements,
                                                       measurements_per_run,
                                                       self.type_first.value)
                    n_image_groups = max(zip(*grouped_images)[0]) + 1

                    # Add image files to uploads
                    uploads = [[name, f"run{g}/images"] for g, name in
                               grouped_images]

                else:
                    if n_images > 1:
                        wx.MessageBox(
                            "Include only one image archive per run.",
                            caption="Image error",
                            style=wx.OK | wx.ICON_INFORMATION)
                        return False

                    uploads = [[file_list[0], 'images']]

                    n_measurements = self.measurements_in_archive.value
                    n_image_groups = max_tasks

                # Also add the pipeline
                uploads += [[path, '.']]

                # The runs are downloaded in their separate folders.
                # They can be processed later
                output_dir = cpprefs.get_default_output_directory()
                downloads = [[f"run{g}", output_dir] for g in
                             range(n_image_groups)]

                # Create run scripts and add to uploads
                for g in range(n_image_groups):
                    runscript_name = f"cellprofiler_run{g}"
                    local_script_path = os.path.join(rynner.provider.script_dir,
                                                     runscript_name)

                    if not self.is_archive.value:
                        n_measurements = len([i for i in grouped_images if i[
                            0] == g]) / self.n_images_per_measurement.value

                        script = (f"cellprofiler -c -p ../Batch_data.h5 -o " 
                                  f"results -i images -f 1 -l {n_measurements}" 
                                  f" 2>>../cellprofiler_output; rm -r images")

                    else:
                        n_images_per_group = int(n_measurements / max_tasks)
                        n_additional_images = int(n_measurements % max_tasks)

                        if g < n_additional_images:
                            first = (n_images_per_group + 1) * g
                            last = (n_images_per_group + 1) * (g + 1)
                        else:
                            first = n_images_per_group * g + n_additional_images
                            last = n_images_per_group * (
                                    g + 1) + n_additional_images

                        script = (f"mkdir images; cp ../images/* images; "
                                  f"cellprofiler -c -p ../Batch_data.h5 -o "
                                  f"results -i images -f {first} -l {last} 2>>"
                                  f"../cellprofiler_output; rm -r images")

                    with open(local_script_path, "w") as file:
                        file.write(script)

                    uploads += [[local_script_path, f"run{g}"]]

                # Define the job to run
                script = (f"{setup_script}; printf %s\\\\n "
                          f"{{0..{n_image_groups - 1}}} | xargs -P 40 -n 1 -IX "
                          f"bash -c \"cd runX ; ./cellprofiler_runX; \";")

                script = script.replace('\r\n', '\n')
                script = script.replace(';;', ';')
                print(script)
                run = rynner.create_run(
                    jobname=self.runname.value.replace(' ', '_'),
                    script=script, uploads=uploads, downloads=downloads)

                run['account'] = self.account.value

                # Copy the pipeline and images accross
                dialog = wx.GenericProgressDialog("Uploading",
                                                  "Uploading files",
                                                  style=wx.PD_APP_MODAL)
                try:
                    self.upload(run, dialog)

                    # Submit the run
                    dialog.Update(dialog.GetRange() - 1, "Submitting")
                    success = CPRynner().submit(run)
                    dialog.Destroy()

                    if success:
                        wx.MessageBox(
                            "RunOnCluster submitted the run to the cluster",
                            caption="RunOnCluster: Batch job submitted",
                            style=wx.OK | wx.ICON_INFORMATION)
                    else:
                        wx.MessageBox(
                            "RunOnCluster failed to submit the run",
                            caption="RunOnCluster: Failure",
                            style=wx.OK | wx.ICON_INFORMATION)
                except Exception as e:
                    dialog.Destroy()
                    raise e

            return False

    def run(self, workspace):
        # The submission happens in prepare run.
        pass

    def validate_module(self, pipeline):
        """Make sure the module settings are valid"""
        # This must be the last module in the pipeline
        if id(self) != id(pipeline.modules()[-1]):
            raise cps.ValidationError((f"The RunOnCluster module must be the last "
                                   f"in the pipeline."), self.runname)

        max_runtime = int(cluster_max_runtime())
        if self.max_walltime.value >= max_runtime:
            raise cps.ValidationError(
                f"The maximum runtime must be less than {max_runtime} hours.",
                self.max_walltime)

    def validate_module_warnings(self, pipeline):
        """Warn user re: Test mode """
        if pipeline.test_mode:
            raise cps.ValidationError(
                "RunOnCluster will not produce output in Test Mode",
                self.runname)

    @staticmethod
    def alter_path(path):
        if path == cpprefs.get_default_output_directory():
            path = 'results'
        else:
            path = os.path.join('results', os.path.basename(path))
        path = path.replace('\\', '/')
        return path

    def save_pipeline(self, workspace, outf=None):
        """Save the pipeline in Batch_data.h5

        Save the pickled image_set_list state in a setting and put this
        module in batch mode.

        if outf is not None, it is used as a file object destination.
        """

        if outf is None:
            path = cpprefs.get_default_output_directory()
            h5_path = os.path.join(path, F_BATCH_DATA_H5)
        else:
            h5_path = outf

        image_set_list = workspace.image_set_list
        pipeline = workspace.pipeline
        m = cpmeas.Measurements(copy=workspace.measurements, filename=h5_path)

        try:
            assert isinstance(pipeline, cpp.Pipeline)
            assert isinstance(m, cpmeas.Measurements)

            orig_pipeline = pipeline
            pipeline = pipeline.copy()
            # this use of workspace.frame is okay, since we're called from
            # prepare_run which happens in the main wx thread.
            target_workspace = cpw.Workspace(pipeline, None, None, None, m,
                                         image_set_list,
                                         workspace.frame)
            # Assuming all results go to the same place,
            # output folder can be set in the script
            pipeline.prepare_to_create_batch(target_workspace, self.alter_path)
            self_copy = pipeline.module(self.module_num)
            self_copy.revision.value = int(
                re.sub(r"\.|rc\d{1}", "", cellprofiler_core.__version__))
            self_copy.batch_mode.value = True
            pipeline.write_pipeline_measurement(m)
            orig_pipeline.write_pipeline_measurement(m, user_pipeline=True)

            return h5_path
        finally:
            m.close()
