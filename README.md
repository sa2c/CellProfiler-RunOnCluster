# CellProfiler-RunOnCluster 
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3275888.svg)](https://doi.org/10.5281/zenodo.3275888)

A CellProfiler module for submitting batch jobs to a cluster running Slurm.

## Installation
### Windows
Currently testing [CellProfiler 4.2.1](https://cellprofiler.org/releases) on Windows 10 with Python 3.8.

Previously, a precompiled executable with default settings for the Sunbird cluster in Swansea was provided in [releases](https://github.com/sa2c/CellProfiler-RunOnCluster/releases/download/v1.0/CellProfiler.exe). These executables require that a Java Runtime Environment is installed. You can get one for example form [Java](https://www.java.com/en/download/). 

### Source Installation
When running on Linux or developing your own plugins, the most straight forward method is to copy the plugin sources to your Cellprofiler plugins directory. Follow the instructions for installing CellProfiler on the [Wiki](https://github.com/CellProfiler/CellProfiler/wiki). Choose you operating system on the right side panel. Once you have installed CellProfiler, set the plugins directory in Cellprofiler preferences. 

### Loading Plugins
Download [Plugins](https://github.com/sa2c/CellProfiler-RunOnCluster/archive/project_update.zip) and move the files to your plugins directory. Please note the plugins are being modified for CellProfiler 4.

In the plugins directory, install the additional requirements for the plugins:
```
pip install git+https://github.com/sa2c/Rynner.git
```

Then,
```
python -m pip install -r requirements.txt
```

## Usage
### Submitting to Cluster
Once you have tested your pipeline on your local machine, add all images to be processed into the Images plugin in the usual way. Add the `RunOnCluster` module in the `Other` category to the end of the pipeline.

The module has four settings:
 * Run Name: An identifier that allows you to recognize the pipeline and image batch.
 * Number of images per measurement: If several image files are required for a single measurement, adjust this to the number of images required.
 * Image type first: Select `Yes` if the image type appears before the measurement number in the image file name. Select `No` if the measurement number appears before the image type.
 * Maximum Runtime (hours): The amount of time to reserve a node for on the cluster. The actual runtime can be lower, but not larger than this. If the run takes longer than the time given, it will be terminated before completion. Must be less than 72.

Submit the pipeline by pressing `Analyze Images`. The plugin will copy the image files and the pipeline to the cluster and add the process to the queue.

### Checking Run Status

Open the ClusterView module in the Data Tools menu. You will see a list of all runs submitted to the cluster. Under the run name the module will display `PENDING` for runs in queue or currently running and `COMPLETED` for runs that have stopped running. Click `Update` in the upper left corner to refresh the status of the runs. Use the `Download Results` button to download and inspect the results.

If you have already downloaded the results, the button label will change to `Download Again`.
