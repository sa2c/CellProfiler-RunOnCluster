# -*- coding: future_fstrings -*-
import uuid
import os
import pickle
from box import Box
from stat import S_ISDIR
import tempfile
import time

from future import *
from pathlib import Path, PurePosixPath

class Rynner(object):

    StatusPending = 'PENDING'
    StatusRunning = 'RUNNING'
    StatusCancelled = 'CANCELLED'
    StatusCompleted = 'COMPLETED'

    StatesPreComplete = [StatusPending, StatusRunning]
    StatesPostComplete = [StatusCompleted, StatusCancelled]

    def __init__(self, provider):
        self.provider = provider

    def create_run(self,
                   script,
                   uploads=None,
                   downloads=None,
                   namespace=None,
                   jobname='rynner'):
        if not uploads:
            uploads = []

        if not downloads:
            downloads = []

        uid = str(uuid.uuid1())

        run = Box({
            'id': uid,
            'job_name': jobname,
            'remote_dir': self._remote_dir(namespace, uid),
            'uploads': uploads,
            'downloads': downloads,
            'script': script,
            'status': Rynner.StatusPending
        })

        return run

    def _remote_dir(self, namespace, uid):
        path = PurePosixPath( 'rynner' )
        if namespace:
            path = path.joinpath( namespace, uid )
        else:
            path = path.joinpath( uid )

        return path

    def _parse_path(self, file_transfer):
        if type(file_transfer) == str:
            src = file_transfer
            dest, _ = os.path.split(file_transfer)

        elif len(file_transfer):
            src, dest = file_transfer
        else:
            raise InvalidContextOption(
                'invalid format for uploads options: {uploads}')

        if dest == '':
            dest = '.'

        return src, dest

    def upload(self, run):
        '''
        Uploads files using provider channel.
        '''

        uploads = run['uploads']

        for upload in uploads:
            src, dest = self._parse_path(upload)
            print(f'Uploading {src} to {dest}')
            dest = run.remote_dir.joinpath( dest ).as_posix()

            self.provider.channel.push_directory(src, dest)
        
        run['upload_time'] = time.time()
        self.save_run_config( run )

    def download(self, run):
        '''
        Download files using provider channel.
        '''

        downloads = run['downloads']

        for download in downloads:
            src, dest = self._parse_path(download)
            src = run.remote_dir.joinpath( src ).as_posix()

            print(f'Downloading {src} to {dest}')

            # Libsubmit will refuse to overwrite an existing file.
            # We want overwriting as default behaviour and remove
            # the file here
            dest_file = os.path.join(dest, os.path.basename(src))
            if os.path.isfile(dest_file):
                os.remove(dest_file)

            self.provider.channel.pull_directory(src, dest)

    def submit(self, run):
        # copy execution script to remote

        runscript_name = f'rynner_exec_{run.job_name}'
        script_dir = self.provider.script_dir
        local_script_path = os.path.join(self.provider.script_dir, runscript_name)

        with open(local_script_path, "w") as file:
            file.write(run['script'])

        print(local_script_path, run.remote_dir.as_posix())
        self.provider.channel.push_file(local_script_path, run.remote_dir.as_posix())

        # record submission times on remote

        self._record_time('submit', run, execute=True)

        # submit run

        submit_script = f'cd {run.remote_dir.as_posix()}; \
{self._record_time("start", run)}; \
./{runscript_name}; \
{self._record_time("end", run)}'

        print('Submitting run')
        run['qid'] = self.provider.submit(submit_script, 1)
        print(f'Run submitted with queue id {run["qid"]}')
        run['status'] = Rynner.StatusPending

        self.save_run_config( run )

    def cancel(self, run):
        self._record_time('cancel', run)
        run['qid'] = self.provider.cancel(run['script'], 1)
        run['status'] = Rynner.StatusCancelled
        self.save_run_config( run )

    def _record_time(self, label, run, execute=False):
        times_file = run.remote_dir.joinpath('rynner.times').as_posix()
        remote_cmd = f'echo "{label}: $(date +%s)" >> {times_file}'
        if execute:
            self.provider.channel.execute_wait(remote_cmd)

        return remote_cmd

    def _finished_since_last_update(self, runs):

        qids = [r['qid'] for r in runs]
        status = self.provider.status(qids)

        needs_update = [
            run['status'] == Rynner.StatusRunning
            and status[index] == Rynner.StatusRunning
            for index, run in enumerate(runs)
        ]

        return needs_update, status

    def update(self, runs):
        '''
        Performs an in-place update of run information. Only information in info and status keys are changed. Status is changed to reflect the current state of the job. Info is updated with the output of self.provider.info every time the job state has changed. The method returns True when any run has been changes, and false otherwise.
        '''

        changed = False

        # find current status of all runs

        qids = [r['qid'] for r in runs]
        status = self.provider.status(qids)

        # find runs which have finished since last update

        needs_update = []

        for index, run in enumerate(runs):
            old_status = run['status']
            new_status = status[index]
            if new_status != old_status:
                # finished since last check
                needs_update.append(run)
                changed = True

        # update status of all runs

        for index, run in enumerate(runs):
            run['status'] = status[index]

        # get info on remaining runs (not implemented)
        qids = [run['qid'] for run in needs_update]

        return changed

    def save_run_config(self, run):
        '''
        Saves the run configuration on the cluster
        '''

        filename = f'rynner_data_{run.job_name}.pkl'
        filepath = run.remote_dir.joinpath( filename ).as_posix()

        sftp_client = self.provider.channel.sftp_client
        with sftp_client.open(filepath, "wb") as f:
            pickle.dump( run, f )

        return filepath

    def get_runs(self, namespace = ''):
        '''
        Read pickled run files from the cluster
        '''

        sftp_client = self.provider.channel.sftp_client
        basedir = self._remote_dir(namespace=namespace, uid='')

        runs = []
        try:
            rynner_folder_exists = S_ISDIR( sftp_client.stat(basedir.as_posix()).st_mode )
        except:
            rynner_folder_exists = False

        if rynner_folder_exists:
            dir_list = sftp_client.listdir(path=basedir.as_posix())

            for dirname in dir_list:
                subdir = basedir.joinpath(dirname)

                if S_ISDIR( sftp_client.stat(subdir.as_posix()).st_mode ):
                    file_list = sftp_client.listdir(path=subdir.as_posix())

                    for filename in file_list:
                        if 'rynner_data_' in filename and '.pkl' in filename:
                            remote_path = subdir.joinpath(filename).as_posix()
                            try:
                                with sftp_client.open(remote_path, 'rb') as f:
                                    run = pickle.load( f )
                                self.provider._test_add_resource( run['qid'] )
                                runs += [ run ]
                            except:
                                pass
                            
        return runs

