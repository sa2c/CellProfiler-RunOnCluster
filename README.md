# CellProfiler-RunOnCluster 
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3275888.svg)](https://doi.org/10.5281/zenodo.3275888)

A CellProfiler module for submitting batch jobs to a cluster running Slurm. Currently testing both plugins with [CellProfiler 4.2.1 and CellProfiler 4.2.2rc](https://cellprofiler.org/releases) on Windows 10/11.

## Installation
### Windows
Note as our plugin requires additional libraries which aren’t packaged with CellProfiler 4.2.2rc, you’ll need to build CellProfiler from source rather than using a pre-packaged version. ([Reference](https://cellprofiler-manual.s3.amazonaws.com/CellProfiler-4.0.6/help/other_plugins.html?highlight=plugins))

### Source Installation
When running on Linux or developing your own plugins, the most straight forward method is to copy the plugin sources to your Cellprofiler plugins directory. It is recommended to use the default ``CellProfiler\plugins`` folder as your plugin directory, but you can still have the plugins in other locations. Follow the instructions for installing CellProfiler on the [Wiki](https://github.com/CellProfiler/CellProfiler/wiki). Choose you operating system on the right side panel. Once you have installed CellProfiler, set the plugins directory in Cellprofiler preferences, then save and close CellProfiler. 

Some useful guidlines for developing CellProfiler plugins:

[Orientation to CellProfiler code](https://github.com/CellProfiler/CellProfiler/wiki/Orientation-to-CellProfiler-code)

[CellProfiler 3 to 4 migration guide](https://github.com/CellProfiler/CellProfiler-plugins/wiki/CellProfiler-3-to-4-migration-guide)

### Previous Plugins
[Previous plugins](https://github.com/sa2c/CellProfiler-RunOnCluster/archive/master.zip) DON'T FULLY WORK with the precompiled CellProfile executable (version 3.1.8). They have default settings for the SCW Sunbird cluster, which can be found in [releases](https://github.com/sa2c/CellProfiler-RunOnCluster/releases/download/v1.0/CellProfiler.exe). These executables require that a Java Runtime Environment is installed. You can get one for example form [Java](https://www.java.com/en/download/).

### Latest Plugins
Please download the [latest plugins](https://github.com/sa2c/CellProfiler-RunOnCluster/archive/CellProfiler4.zip) from this branch and move the files to your plugins directory. In the plugins directory, install the [Rynner](https://github.com/sa2c/Rynner) module and additional requirements for the plugins:

```
pip install git+https://github.com/sa2c/Rynner.git
python -m pip install -r requirements.txt
```

If `pip install` does not install the `Rynner` library properly (i.e. there is no ``Rynner`` folder inside ``site-packages``.). You should copy the `Rynner` folder in this repository and paste it inside ``side-packages``.

## Usage
### Submitting josb to cluster

Once you have tested your pipeline on your local machine, add all images to be processed into the Images plugin in the usual way. Add the `RunOnCluster` module in the `Other` category to the end of the pipeline.

The module has four settings:
 * Run Name: An identifier that allows you to recognise the pipeline and image batch.
 * Number of images per measurement: If several image files are required for a single measurement, adjust this to the number of images required.
 * Image type first: Select `Yes` if the image type appears before the measurement number in the image file name. Select `No` if the measurement number appears before the image type.
 * Maximum Runtime (hours): The amount of time to reserve a node for on the cluster. The actual runtime can be lower, but not larger than this. If the run takes longer than the time given, it will be terminated before completion. Must be less than 72.
 * Project Code: Specify your Supercomputing Wales project code, for example, scw1234.
 * Partition: Select the partition you wish to run your job on. (Defaults to `compute` partition)
 * Local script directory: Choose where local copies of remote scripts and batch data are saved.

Submit the pipeline by pressing `Analyze Images`. The plugin will copy the image files and the pipeline to the cluster and add the process to the queue.

### Checking job status

In CellProfiler 3, you can open the `ClusterView` module in the `Data Tools` menu, however, in CellPfoiler 4 the `Data Tools` menu has been merged into the Module list. Therefore, you will need to add the `ClusterView` plugin into the pipeline, then you will see a list of all runs submitted to the cluster. Under the run name the module will display `PENDING` for runs in queue or currently running and `COMPLETED` for runs that have stopped running. Click `Update` in the upper left corner to refresh the status of the runs. Use the `Download Results` button to download and inspect the results.

If you have already downloaded the results, the button label will change to `Download Again`.
