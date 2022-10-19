from datetime import datetime
import uuid
import os
import pickle
from stat import S_ISDIR
import time

from pathlib import PurePosixPath
from box import Box  # this warning is a PyCharm bug
import threading


class Rynner:

    StatusPending = 'PENDING'
    StatusRunning = 'RUNNING'
    StatusCancelled = 'CANCELLED'
    StatusCompleted = 'COMPLETED'

    StatesPreComplete = [StatusPending, StatusRunning]
    StatesPostComplete = [StatusCompleted, StatusCancelled]

    def __init__(self, provider, path='rynner'):
        self.provider = provider
        self.path = path

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

        # uid = str(uuid.uuid1())
        date = datetime.now().strftime("%d_%b_%y-%X")
        uid = str(jobname)+"_"+str(date)

        run = Box({
            'id': uid,
            'job_name': jobname,
            'remote_dir': self._remote_dir(namespace, uid),
            'uploads': uploads,
            'downloads': downloads,
            'script': script,
            'status': Rynner.StatusPending,
            'account': '',
            'partition': ''
        })

        return run

    def _remote_dir(self, namespace, uid):
        p_ = PurePosixPath(self.path)
        path = p_.joinpath(namespace, uid) if namespace else p_.joinpath(uid)
        return path

    @staticmethod
    def _parse_path(file_transfer):
        if type(file_transfer) == str:
            src = file_transfer
            dest, _ = os.path.split(file_transfer)
        elif len(file_transfer):
            src, dest = file_transfer
        else:
            raise InvalidContextOption('invalid format for uploads options: '
                                       '{uploads}')  # FIXME need a parameter

        if dest == '':
            dest = '.'

        return src, dest

    def list_local_files(self, run, local_source, remote_dir):
        """Build a list of files a remote folder
        """
        expanded_uploads = []

        try:
            if os.path.isdir(local_source):
                _, directory_name = os.path.split(local_source)
                dest = remote_dir + '/' + directory_name  # FIXME os independent
                
                file_list = os.listdir(directory_name)
                for filename in file_list:
                    src = os.path.join(local_source, filename)
                    expanded_uploads += self.list_remote_files(run, src, dest)
            else:
                expanded_uploads = [[local_source, remote_dir]]
        except Exception as e:
            print(e)
            print("No such file")
        return expanded_uploads
    
    def start_upload(self, run):
        """Spawn a thread to upload the files in the upload list.
        Update a report of the current state of the process.
        """
        uploads = []  # todo consolidation
        for upload in run['uploads']:
            uploads += self.list_local_files(run, upload[0], upload[1])

        def upload_thread(run_):  # fixme remove this nest?, verify name changes
            """The function executed by the download thread
            """
            run_copy = run_.copy()
            for i, upload_ in enumerate(uploads):
                run_['upload_status'] = (float(i)/len(uploads))
                run_copy['uploads'] = [upload_]
                self.upload(run_copy)
                
            run_['upload_status'] = 1.0
            return
        
        run['upload_status'] = 0
        thread = threading.Thread(target=upload_thread, args=(run,))
        thread.start()

    def upload(self, run):
        """Uploads files using provider channel.
        """

        uploads = run['uploads']

        for upload in uploads:
            src, dest = self._parse_path(upload)
            dest = run.remote_dir.joinpath(dest).as_posix()
            print(f"Source location: {src}")
            print(f"Remote destination: {dest}")
            self.provider.channel.push_file(src, dest)

        run['upload_time'] = time.time()

    def list_remote_files(self, run, remote_source, local_dir):
        """Build a list of files a remote folder
        """
        expanded_downloads = []
        sftp_client = self.provider.channel.sftp_client
        src = run.remote_dir.joinpath(remote_source).as_posix()

        try:
            if S_ISDIR(sftp_client.stat(src).st_mode):
                _, directory_name = os.path.split(src)
                dest = os.path.join(local_dir, directory_name)
                
                file_list = sftp_client.listdir(path=src)
                for filename in file_list:
                    src = remote_source + '/' + filename
                    # fixme os indepedent path
                    expanded_downloads += self.list_remote_files(run, src, dest)
            else:
                expanded_downloads = [[remote_source, local_dir]]
        except Exception as e:
            print(e)
            print(src)
            print("No such file")
        return expanded_downloads
    
    def start_download(self, run):  # fixme download and upload are identical?
        """Spawn a thread to download the files in the download list.
        Update a report of the current state of the process.
        """
        downloads = []
        for download in run['downloads']:
            downloads += self.list_remote_files(run, download[0], download[1])

        def download_thread(run_):
            """The function executed by the download thread
            """
            run_copy = run_.copy()
            for i, download_ in enumerate(downloads):
                run_['download_status'] = (float(i)/len(downloads))
                run_copy['downloads'] = [download_]
                self.download(run_copy)
                
            run_['download_status'] = 1.0
            return
        
        run['download_status'] = 0
        thread = threading.Thread(target=download_thread, args=(run,))
        thread.start()

    def download(self, run):  # fixme same for download/upload
        """Download files using provider channel.
        """

        downloads = run['downloads']

        for download in downloads:
            src, dest = self._parse_path(download)
            src = run.remote_dir.joinpath(src).as_posix()

            # Libsubmit will refuse to overwrite an existing file.
            # We want overwriting as default behaviour and remove
            # the file here*
            dest_file = os.path.join(dest, os.path.basename(src))
            if os.path.isfile(dest_file):
                os.remove(dest_file)

            self.provider.channel.pull_directory(src, dest)

    def submit(self, run):
        # copy execution script to remote

        runscript_name = f'rynner_exec_{run.job_name}'

        local_script_path = os.path.join(self.provider.script_dir,
                                         runscript_name)
        print(f"Writing script into: {local_script_path}")
        with open(local_script_path, "wb") as file:
            file.write(run['script'].replace('\r\n','\n').encode())

        self.provider.channel.push_file(local_script_path,
                                        run.remote_dir.as_posix())

        # record submission times on remote

        self._record_time('submit', run, execute=True)

        # Add SCW account to script through overrides property of provider
        self.provider.overrides = f"#SBATCH --account={run.account}"

        # Add partition to script via provider.partition
        self.provider.partition = run.partition
        # submit run

        submit_script = (f'{self._record_time("start", run)}; '
                         f'cd {run.remote_dir.as_posix()}; '
                         f'./{runscript_name}; '
                         f'cd ; '
                         f'{self._record_time("end", run)}')  # todo verify this

        submit_script.replace('\r\n','\n') # Filter out DOS linebreaks
        submit_script.replace('%TEST4%','STEP4')

        qid = self.provider.submit(submit_script, blocksize=1, job_name = run.job_name)
        
        if qid is None:
            # Failed to submit
            return False
        else:
            # Succesfully submitted
            run['qid'] = qid
            run['status'] = Rynner.StatusPending

            self.save_run_config(run)
            return True

    def cancel(self, run):
        self._record_time('cancel', run)
        run['qid'] = self.provider.cancel(run['script'], 1)
        run['status'] = Rynner.StatusCancelled
        self.save_run_config(run)

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

    def read_time(self, run):

        filename = 'rynner.times'
        filepath = run.remote_dir.joinpath(filename).as_posix()

        sftp_client = self.provider.channel.sftp_client
        try:
            with sftp_client.open(filepath, "r") as f:
                lines = f.read().splitlines()
                last_line = lines[-1]
                return last_line.split(' ')[1]
        except Exception as e:
            print(f"Exception happened: {e}")
            return None

    def update(self, runs):
        """Performs an in-place update of run information. Only information in
        info and status keys are changed. Status is changed to reflect the
        current state of the job. Info is updated with the output of
        self.provider.info every time the job state has changed. The method
        returns True when any run has been changes, and false otherwise.
        """

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
        qids = [run['qid'] for run in needs_update]  # todo finish?

        return changed

    def save_run_config(self, run):
        """Saves the run configuration on the cluster
        """

        filename = f'rynner_data_{run.job_name}.pkl'
        filepath = run.remote_dir.joinpath(filename).as_posix()

        sftp_client = self.provider.channel.sftp_client
        with sftp_client.open(filepath, "wb") as f:
            # todo do we need binary flag?
            pickle.dump(run, f)

        return filepath

    def get_runs(self, namespace=''):
        """Read pickled run files from the cluster
        """

        sftp_client = self.provider.channel.sftp_client
        basedir = self._remote_dir(namespace=namespace, uid='')

        runs = []
        try:
            rynner_folder_exists = S_ISDIR(sftp_client.stat(
                basedir.as_posix()).st_mode)
        except Exception as e:
            print(f"Failed to check remote folder, reason {e}")
            rynner_folder_exists = False

        if rynner_folder_exists:
            dir_list = sftp_client.listdir(path=basedir.as_posix())

            for dirname in dir_list:
                subdir = basedir.joinpath(dirname)

                if S_ISDIR(sftp_client.stat(subdir.as_posix()).st_mode):
                    file_list = sftp_client.listdir(path=subdir.as_posix())

                    for filename in file_list:
                        if 'rynner_data_' in filename and '.pkl' in filename:
                            remote_path = subdir.joinpath(filename).as_posix()
                            try:
                                with sftp_client.open(remote_path, 'rb') as f:
                                    run = pickle.load(f)
                                self.provider._test_add_resource(run['qid'])
                                runs += [run]
                            except Exception as e:
                                print(e)  # todo finish this
                                pass
                            
        return runs