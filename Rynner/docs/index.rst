.. Rynner documentation master file, created by
   sphinx-quickstart on Thu Aug 30 17:45:08 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Rynner's documentation!
==================================

`Rynner` is a tool that eases launching and managing jobs on a HPC cluster. It
consist of a simple framework to create graphical user interfaces tailored to a
specific HPC application, connecting them with a job scheduling system like `SLURM`
and `PBS` on a remote system, and relieve the user from having to cope
with the difficulties of a HPC environment - as if they were running the HPC
software they need on their own PC.

It consist of a set of Python classes that a developer can use to manage input
from the user, file transfers, job submission and monitoring, and it is flexible 
enough to allow the developer to add any operation on the local machine.

It works through `SSH` and `SFTP`, using the library `paramiko`. 


Usage
-----


Notes
+++++





.. toctree::
   :maxdepth: 2

   create_view
   datastore
   host_patterns
   host
   index_view
   logs
   main
   pattern_parser
   plugin
   run
   template
   validator
 



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
