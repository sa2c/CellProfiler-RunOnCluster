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

from CPRynner.CPRynner import cluster_max_runtime
from CPRynner.CPRynner import cluster_run_command
from CPRynner.CPRynner import cluster_setup_script
from CPRynner.CPRynner import cluster_tasks_per_node
from CPRynner.CPRynner import update_cluster_parameters
from CPRynner.CPRynner import CPRynner
from cellprofiler_core.preferences import get_plugin_directory
from cellprofiler_core.preferences import DEFAULT_OUTPUT_SUBFOLDER_NAME
from cellprofiler_core.preferences import DEFAULT_OUTPUT_FOLDER_NAME
from cellprofiler_core.preferences import DEFAULT_INPUT_SUBFOLDER_NAME
from cellprofiler_core.preferences import DEFAULT_INPUT_FOLDER_NAME
from cellprofiler_core.preferences import ABSOLUTE_FOLDER_NAME
from cellprofiler_core.setting.text import Directory
from cellprofiler_core.constants.measurement import F_BATCH_DATA_H5
from cellprofiler_core.pipeline import Pipeline
from cellprofiler_core.workspace import Workspace
from cellprofiler_core.measurement import Measurements
from cellprofiler_core.preferences import get_default_output_directory
from cellprofiler_core.setting.do_something import DoSomething
from cellprofiler_core.setting.text import Integer, Text
from cellprofiler_core.setting import Binary, ValidationError
from cellprofiler_core.module import Module
import cellprofiler_core
import os
import re
import wx
import sys
import logging
logger = logging.getLogger(__name__)
sys.path.append(get_plugin_directory())


class RunOnCluster(Module):
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

        doc_ = (f"Enter a recognisable identifier for the run "
                f"(spaces will be replaced by undescores)")
        self.runname = Text("Run Name", "Run_name", doc=doc_)

        doc_ = (f"The number of image files in each measurement that must be "
                f"present for the pipeline to run correctly. This is usually "
                f"the number of image types in the NamesAndTypes module.")
        self.n_images_per_measurement = Integer(
            "Number of images per measurement", 1, minval=1, doc=doc_)

        doc_ = (f"Wether the images are ordered by image type first. "
                f"If not, ordering by measurement first is assumed.")
        self.type_first = Binary(text="Image type first", value=True, doc=doc_)

        doc_ = (f"Set to Yes if the the images are included as a single image "
                f"archive, such as an Ism file.")
        self.is_archive = Binary(
            text="Is image archive", value=False, doc=doc_)

        doc_ = "The number of measurements in the archive file."
        self.measurements_in_archive = Integer(
            "Number of measurements in the archive", 1, minval=1, doc=doc_)

        doc_ = (f"The maximum time for reserving a node on the cluster. Should"
                f" be higher than the actual runtime, or the run may not "
                f"complete. Runs with lower values will pass the queue "
                f"more quickly.")
        self.max_walltime = Integer("Maximum Runtime (hours)", 24, doc=doc_)

        doc_ = (f"Enter a project code of an Supercomputing Wales project you "
                f"wish to run under. This can be left empty if you have only "
                f"one project.")
        self.account = Text("Project Code", "", doc=doc_)

        doc_ = (f"Select the partition you wish to run your job on. This may "
                f"be useful if you have a private partition you wish to utilise. "
                f"Defaults to 'compute' partition.")
        self.partition = Text("Partition", "compute", doc=doc_)

        doc_ = (f"Choose where local copies of remote scripts and batch data are"
                f" saved.")
        self.script_directory = Directory("Local script directory",
                                          dir_choices=[DEFAULT_OUTPUT_FOLDER_NAME,
                                                       DEFAULT_INPUT_FOLDER_NAME,
                                                       ABSOLUTE_FOLDER_NAME,
                                                       DEFAULT_OUTPUT_SUBFOLDER_NAME,
                                                       DEFAULT_INPUT_SUBFOLDER_NAME,],
                                          value=DEFAULT_OUTPUT_SUBFOLDER_NAME, doc=doc_)

        doc_ = "Change cluster and edit cluster settings."
        self.cluster_settings_button = DoSomething(
            "", "Cluster Settings", update_cluster_parameters, doc=doc_)

        self.batch_mode = Binary("Hidden: in batch mode", False)
        self.revision = Integer("Hidden: revision number", 0)

    def settings(self):
        result = [
            self.runname,
            self.is_archive,
            self.n_images_per_measurement,
            self.type_first,
            self.measurements_in_archive,
            self.max_walltime,
            self.account,
            self.partition,
            self.script_directory,
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
            self.partition,
            self.script_directory,
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
            self.partition,
            self.script_directory
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

    def assign_image_groups(self, file_list):
        max_tasks = int(cluster_tasks_per_node())
        n_images = len(file_list)
        if not self.is_archive.value:
            n_measurements = int(
                n_images / self.n_images_per_measurement.value)
            measurements_per_run = int(n_measurements / max_tasks) + 1
            grouped_images = self.group_images(file_list,
                                               n_measurements,
                                               measurements_per_run,
                                               self.type_first.value)
            image_group_list = list(zip(*grouped_images))
            n_image_groups = max(image_group_list[0]) + 1
            # Add image files to uploads
            uploads = [[name, f"run{g}/images"] for g, name in
                       grouped_images]
        else:
            if n_images > 1:
                wx.MessageBox("Include only one image archive per run.",
                              caption="Image error",
                              style=wx.OK | wx.ICON_INFORMATION)
                return (None, None, None)
            uploads = [[file_list[0], 'images']]
            n_measurements = self.measurements_in_archive.value
            n_image_groups = max_tasks
        return (uploads, grouped_images, n_image_groups)

    @staticmethod
    def sanitise_scripts(script):
        # Split by semicolons then check for entries that are just spaces
        split_script = script.split(";")
        for i in range(len(split_script)):
            ss = split_script[i]
            ss = ss.lstrip(" ").rstrip(" ")
            if len(ss) > 0 and i > 0:
                ss = " " + ss
            split_script[i] = ss
        split_script = [ss for ss in split_script if len(ss) > 0]
        end_script = ";".join(split_script).rstrip(" ").lstrip(" ")
        if end_script[-1] != ";":
            end_script = end_script+";"
        end_script = end_script.replace("\r\n", "\n")
        end_script = end_script.lstrip(";")  # Remove leading semicolons
        return end_script

    def create_run_scripts(self, workspace, rynner, uploads, n_image_groups, grouped_images):
        max_tasks = int(cluster_tasks_per_node())
        run_command = cluster_run_command()
        for g in range(n_image_groups):
            runscript_name = f"cellprofiler_run{g}"
            local_script_path = os.path.join(
                rynner.provider.script_dir, runscript_name)
            if not self.is_archive.value:
                n_measurements = int(len([i for i in grouped_images if i[
                    0] == g]) / self.n_images_per_measurement.value)
                script = (f"{run_command} -p Batch_data.h5 -o "
                          f"results -i images -f 1 -l {n_measurements}"
                          f" 2>>../cellprofiler_output")
                script = self.sanitise_scripts(script)
            else:
                n_images_per_group = int(n_measurements / max_tasks)
                n_additional_images = int(n_measurements % max_tasks)
                if g < n_additional_images:
                    first = int((n_images_per_group + 1) * g)
                    last = int((n_images_per_group + 1) * (g + 1))
                else:
                    first = int(n_images_per_group * g + n_additional_images)
                    last = int(n_images_per_group *
                               (g + 1) + n_additional_images)
                    script = (f"{run_command} -p Batch_data.h5 -o "
                              f"results -i images -f {first} -l {last} 2>>"
                              f"../cellprofiler_output;")
                    script = self.sanitise_scripts(script)
            with open(local_script_path, "w") as file:
                file.write(script)
                uploads += [[local_script_path, f"run{g}"]]
            # save the pipeline on a per-node basis in directories labelled by job and subjob
            batch_subdir = os.path.join(
                self.runname.value.replace(' ', '_'), f"run{g}")
            batch_dir = os.path.join(rynner.provider.script_dir, batch_subdir)
            if not os.path.exists(batch_dir):
                os.makedirs(batch_dir)
            path = self.save_remote_pipeline(
                workspace, os.path.join(batch_dir, F_BATCH_DATA_H5))
            # Add the pipeline
            uploads += [[path, f"run{g}"]]
        return uploads

    def create_job_script(self, n_image_groups):
        setup_script = cluster_setup_script()
        script = (f"{setup_script} printf %s\\\\n "
                  f"{{0..{n_image_groups - 1}}} | xargs -P 40 -n 1 -IX "
                  f"bash -c \"cd runX ; ./cellprofiler_runX; \";")
        script = self.sanitise_scripts(script)
        return script

    def prepare_run(self, workspace):
        """Invoke the image_set_list pickling mechanism and save the pipeline"""
        pipeline = workspace.pipeline
        if pipeline.test_mode:
            return True
        if self.batch_mode.value:
            return True
        else:
            rynner = CPRynner()
            # Change default script directory to one set in script_directory setting
            rynner.provider.script_dir = self.script_directory.get_absolute_path()
            if rynner is not None:
                # Set walltime
                rynner.provider.walltime = str(
                    self.max_walltime.value) + ":00:00"

                # Create the run data structure
                file_list = pipeline.file_list
                file_list = [name.replace('file:///', '')
                             for name in file_list]
                file_list = [name.replace('file:', '') for name in file_list]
                file_list = [name.replace('%20', ' ') for name in file_list]

                if len(file_list) == 0:
                    wx.MessageBox(
                        (f"No images found. Did you remember to add them to the"
                         f" Images module?"),
                        caption="No images", style=wx.OK | wx.ICON_INFORMATION)
                    return False

                # Divide measurements to runs
                # according to the number of cores on a node in assign_image_groups
                uploads, grouped_images, n_image_groups = self.assign_image_groups(
                    file_list)
                if (uploads is None) or (grouped_images is None) or (n_image_groups is None):
                    return False

                # The runs are downloaded in their separate folders.
                # They can be processed later
                output_dir = get_default_output_directory()
                downloads = [[f"run{g}", output_dir] for g in
                             range(n_image_groups)]

                # Create run scripts and add to uploads with create_run_scripts
                uploads = self.create_run_scripts(
                    workspace, rynner, uploads, n_image_groups, grouped_images)

                # Define the job to run in create_job_script
                script = self.create_job_script(n_image_groups)

                run = rynner.create_run(
                    jobname=self.runname.value.replace(' ', '_'),
                    script=script, uploads=uploads, downloads=downloads)

                # Add account and partition information to run.
                run['account'] = self.account.value
                run['partition'] = self.partition.value

                # Copy the pipeline and images accross
                dialog = wx.GenericProgressDialog("Uploading",
                                                  "Uploading files",
                                                  style=wx.PD_APP_MODAL)

                try:

                    self.upload(run, dialog)
                    # Submit the run
                    dialog.Update(dialog.GetRange() - 1, "Submitting")
                    success = rynner.submit(run)
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

    def validate_module_warnings(self, pipeline):
        """Warn user re: Test mode """
        if pipeline.test_mode:
            raise ValidationError(
                "RunOnCluster will not produce output in Test Mode",
                self.runname)

    @staticmethod
    def alter_path(path):
        if path == get_default_output_directory():
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
            path = get_default_output_directory()
            h5_path = os.path.join(path, F_BATCH_DATA_H5)
        else:
            h5_path = outf

        image_set_list = workspace.image_set_list
        pipeline = workspace.pipeline
        m = Measurements(copy=workspace.measurements, filename=h5_path)

        try:
            assert isinstance(pipeline, Pipeline)
            assert isinstance(m, Measurements)

            orig_pipeline = pipeline
            pipeline = pipeline.copy()
            # this use of workspace.frame is okay, since we're called from
            # prepare_run which happens in the main wx thread.
            target_workspace = Workspace(pipeline, None, None, None, m,
                                         image_set_list,
                                         workspace.frame)
            # Assuming all results go to the same place,
            # output folder can be set in the script

            self_copy = pipeline.module(self.module_num)
            self_copy.revision.value = int(
                re.sub(r"\.|rc\d{1}", "", cellprofiler_core.__version__))

            self_copy.batch_mode.value = True
            # Pipeline is readied for saving at this point
            pipeline.prepare_to_create_batch(target_workspace, self.alter_path)

            pipeline.write_pipeline_measurement(m)
            orig_pipeline.write_pipeline_measurement(m, user_pipeline=True)

            return h5_path
        finally:
            m.close()

    def save_remote_pipeline(self, workspace, outf=None):

        if outf is None:
            path = get_default_output_directory()
            h5_path = os.path.join(path, F_BATCH_DATA_H5)
        else:
            h5_path = outf

        image_set_list = workspace.image_set_list
        pipeline = workspace.pipeline
        m = Measurements(copy=workspace.measurements, filename=h5_path)

        try:
            assert isinstance(pipeline, Pipeline)
            assert isinstance(m, Measurements)

            orig_pipeline = pipeline
            pipeline = pipeline.copy()
            # this use of workspace.frame is okay, since we're called from
            # prepare_run which happens in the main wx thread.
            target_workspace = Workspace(pipeline, None, None, None, m,
                                         image_set_list,
                                         workspace.frame)
            # Assuming all results go to the same place,
            # output folder can be set in the script

            self_copy = pipeline.module(self.module_num)
            self_copy.revision.value = int(
                re.sub(r"\.|rc\d{1}", "", cellprofiler_core.__version__))

        # Trim RunOnCluster and ClusterView modules from submitted pipeline
            for module in reversed(pipeline.modules()):
                if module.module_name == "RunOnCluster" or module.module_name == "ClusterView":
                    pipeline.remove_module(module.module_num)

            self_copy.batch_mode.value = True

            pipeline.prepare_to_create_batch(target_workspace, self.alter_path)

            pipeline.write_pipeline_measurement(m)
            orig_pipeline.write_pipeline_measurement(m, user_pipeline=True)

            return h5_path
        finally:
            m.close()
