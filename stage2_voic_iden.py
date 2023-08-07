import os
import threading
import time
from datetime import datetime
from threading import Thread, Semaphore
import queue
import shutil
import numpy as np
import pandas as pd
from speechbrain.pretrained import SpeakerRecognition
import warnings

from typing import List

pd.options.mode.chained_assignment = None


class VoiceIdentification:
    def __init__(self, backend_interface, threshold, device, identification_workers, n_level):
        """Initialization method, selects pretrained model from speechbrain,
        connect to SFTP indicated server. Connects to mysql db and Stores the
        threshold for which the identification should return True.
        Warning are suppressed to handle the fact that pandas officially only supports SQLAlchemy.

        Arguments
        ---------
        backend_interface : DbSFtpInterface
            Communicator class for database and SFTP server
        threshold : float
            Threshold over which a speaker should be considered matched with
            the given subclip.
        device : str
            Device where the identification will be performed [cpu, cuda].
        identification_workers : int
            Number of workers (thread) to perform the identification process.
        n_level : int
            Number o level (batches) to perform the identification on.

        """
        self.sftp_semaphore = None
        self.file_semaphore = None
        self.job_queue = None
        self.local_score = dict()
        self.instance_broken_101 = False

        # Loading pretrained identification model
        self.verification = SpeakerRecognition.from_hparams(source="speechbrain/spkrec-ecapa-voxceleb",
                                                            savedir="pretrained_models/spkrec-ecapa-voxceleb",
                                                            run_opts={'device': device})
        self.identification_workers = identification_workers
        self.n_level = n_level
        self.threshold = threshold
        self.backend_interface = backend_interface

        # Creating temporary dataframe for score calculation
        self.local_analysis_dataframe = pd.DataFrame(columns=['hash', 'speaker_id', 'score'])
        warnings.filterwarnings("ignore")

    def identify_speaker(self, subclip, user, timestamp_at_start):
        """For a given subclip, it evaluates it against a list of
        pre-recorded prioritized (most used, most recent, added by)
        sub-clips and returns the best match.
        It inserts the subclips into SFTP and db

        Arguments
        ---------
        subclip : list
            Subclip tuple containing path on which the identification should be
            done and segment.
        user : int
            User id of the user-recorder
        timestamp_at_start : int
            Timestamp at which the clip recording started.

        Returns
        -------
        success: int
            0 if no problems encountered; error code elsewhere:
                101 - problem during batch work (typically couldn't fetch with SFTP)
        hash : str
            Hash related to the best possible match.
        speaker_id : str
            Speaker id of the speaker related to hash on the best match for the given subclip.
        score : float
            Score from 0 to 1 where 0 means the model is not confident the two subclip
            speaker matches.
        prediction : boolean
            A boolean evaluation on the subclip based on a given threshold.

        """
        # Getting path from tuple
        path = 'tmp_audio_files_save/' + subclip[0] + '.wav'

        # Copying file to prevent thread from locking continuously
        for worker_id in range(self.identification_workers):
            shutil.copy(path, f"{path}{worker_id}")

        # Getting the prioritized list.
        # TODO: GENERATORE - SI LAVORA IN BATCH, IL PRIMO BATCH VIENE CREATO DA 1 O PIU FILE RAPPRESENTANTI TUTTI GLI
        #  UTENTI E SI DA UNO SCORE A OGNI UTENTE, SI RIFORMANO PIU BATCH RESTRINGENDO LE POSSIBILITA
        #  FIN QUANDO SI HA UNO SCORE SODDISFACENTE.
        #  COMPLESSITA N^2. UN FOR DI FOR, IL PRIMO FOR ELABORA TUTTI I BATCH MENTRE IL FOR ANNIDATO ELABORA
        #  LA SINGOLA SUBCLIP. POSSIBILITA DI FARE THREAD ALL INTERNO DI UN BATCH (SI ELABORANO TUTTE LE SUBCLIP
        #  IN PARALLELO E SI ASPETTA LA FINE DI TUTTI I THREAD PER PASSARE AL PROSSIMO BATCH)

        self.sftp_semaphore = Semaphore(1)
        self.file_semaphore = Semaphore(1)

        # Working through batch to decrease the amount of subclips needed
        for batch_number in range(self.n_level):

            # Starting workers and job queue
            self.job_queue = queue.Queue()
            workers = [
                Thread(target=self.batch_worker,
                       args=(self.job_queue, path, self.sftp_semaphore, worker_id))
                for worker_id in range(self.identification_workers)
            ]

            # Getting batch job following algorithm calculation
            registered_speakers_batch = self._get_batch_speaker_priority_on_check_simplified(
                user)

            # Getting batch job following old algorithm calculation -- This function is deprecated but contains code to
            # be transered to the new simplified function.
            """registered_speakers_batch = self._get_batch_speaker_priority_on_check_simplified(
                user, self.local_score)"""
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                f"Current batch len {len(registered_speakers_batch)}")

            # Populating queue with current batch
            for registered_speaker in registered_speakers_batch:
                # print(f"Evaluating subclip {registered_speaker}...")

                # Populating queue
                self.job_queue.put(registered_speaker)

            # Adding a None signal for workers into queue
            for _ in workers:
                self.job_queue.put(None)

            # Starting all workers
            for worker in workers:
                worker.start()

            # Waiting every worker to finish
            for worker in workers:
                worker.join()

        # Check if there was a problem during batch work (typically couldn't fetch from SFTP)
        if self.instance_broken_101:
            return 101

        # Creating a pandas Series with average score for every speaker
        ordered_result_dataframe = self.local_analysis_dataframe.groupby(
            self.local_analysis_dataframe.speaker_id).apply(lambda x: np.mean(x.score))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
              f"Ordered result \n{ordered_result_dataframe}")

        # Ordering speaker from the best match to the worst
        speaker_id_dataframe_best_match = ordered_result_dataframe.sort_values(
            ascending=False
        ).index[0]

        print(self.local_analysis_dataframe)
        ordered_results = sorted(self.local_score.items(), key=lambda item: item[1], reverse=True)

        # Retrieves speaker id from db // deprecated
        """speaker_id = self.get_speaker_from_hash(ordered_results[0][0])"""

        # Insertion into db and SFTP server and deleting from local memory
        self.backend_interface.insert_subclip(subclip, user, int(speaker_id_dataframe_best_match),
                                              ordered_results[0][0],
                                              timestamp_at_start)

        # Temporary dataframe is reset to starting conditions
        self.local_analysis_dataframe = self.local_analysis_dataframe[0:0]

        return 0, ordered_results[0][0], int(speaker_id_dataframe_best_match), float(ordered_results[0][1]), float(
            ordered_results[0][1]) > self.threshold

    def batch_worker(self, q, path, semaphore_sftp, worker_id):
        """Job to be executed in parallel for identification of speaker.

        Arguments
        ---------
        q : Queue
            Object queue which contains the job to be executed. Ends with as much None as there are workers
            to signal a stop (queue end).
        path : str
            Path where the subclip to match is stored.
        semaphore_sftp : threading.Semaphore
            Flag to allow max 1 thread to access the SFTP subroutine.
        worker_id : int
            Integer going from 0 to self.identification_workers. Gives a unique incrementing id to each worker.
        """

        # Getting job from queue.
        registered_speaker = q.get()

        # If EOQ (end of eueu) is hit, worker should be terminated.
        if registered_speaker is None:
            return

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}]"
            f" Pid {threading.get_native_id()}\tanalysing {registered_speaker}...")

        # Retrieving the pre-recorded subclip from the SFTP server (file name = hash).
        with semaphore_sftp:
            stored_subclip = self.get_subclip_from_sftp(registered_speaker[1])
        time.sleep(0.5)

        # Match between given subclip and pre-recorded subclip,
        for i in range(5):
            try:
                # with semaphore_file:
                score, prediction = self.verification.verify_files(f"{path}{worker_id}", stored_subclip)
                break
            except RuntimeError:
                if i < 5:
                    print(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                        f"Error opening {path}{worker_id}, probably file not loaded yet - retrying; thread {threading.get_native_id()}")
                else:
                    print(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                        f"Error opening {path}{worker_id}, probably corrupted file or file not loaded yet - instance broken; "
                        f"thread {threading.get_native_id()}")


        try:
            os.remove(f"{path}{worker_id}")
            new_row = pd.DataFrame(
                {
                    'hash': registered_speaker[1],
                    'speaker_id': registered_speaker[0],
                    'score': float(score)
                },
                index=[0]
            )
            self.local_analysis_dataframe = pd.concat([
                new_row,
                self.local_analysis_dataframe.loc[:]
            ]).reset_index(drop=True)
            self.local_score[registered_speaker[1]] = score
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                f"Pid {threading.get_native_id()}\tanalyzed {registered_speaker}; Score: {score}")
        except RuntimeError:
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                f"Error opening {path}{worker_id}, probably corrupted file or file not loaded yet - instance will break; "
                f"thread {threading.get_native_id()}")
            self.instance_broken_101 = True
            pass

    def get_subclip_from_sftp(self, registered_speaker) -> str:
        """Retrieves pre-recorded sub-clip from SFTP server.

        Arguments
        ---------
        registered_speaker : str
            File name for the sub-clip to retrieve.
            Default format [.wav]

        Returns
        -------
        stored_subclip : str
            Path of locally stored temporary .wav file.

        """
        self.backend_interface.sftp.get(remotepath=registered_speaker + '.wav',
                                        localpath='tmp_audio_files/' + registered_speaker + '.wav')

        return 'tmp_audio_files/' + registered_speaker + '.wav'

    def _get_batch_speaker_priority_on_check_simplified(self, user):

        get_speakers_query = "SELECT * FROM speaker"
        get_subclip_for_speaker = f'SELECT * FROM subclips WHERE speaker = "%d" ORDER BY id desc'
        speakers_dataframe = pd.read_sql(get_speakers_query, self.backend_interface.mysql)
        speakers_id = speakers_dataframe['id'].tolist()
        subclip_hashes = []
        for speaker in speakers_id:
            speaker_subclip_dataframe = pd.read_sql(get_subclip_for_speaker % speaker, self.backend_interface.mysql)
            speakers_subclip_id = speaker_subclip_dataframe['hash'].tolist()[:3]
            speakers_id = speaker_subclip_dataframe['speaker'].tolist()[:3]
            for i in range(0, 5):
                try:
                    subclip_hashes.insert(1, (speakers_id[i], speakers_subclip_id[i]))
                except IndexError:
                    pass
        return subclip_hashes

    def _get_batch_speaker_priority_on_check(self, user, weight=None) -> List[str]:
        """Creates a list of priorities on which the identifier will work first
        weighted on recent usage, who added the speaker first and who calls the speaker
        first.

        TODO: Everything - for now the method returns all the sub-clips presents in the db.

        Arguments
        ---------
        user : int
            Username of the recorder-user.
        Returns
        -------
        registered_speakers : list of str
            List of strings containing name of sub-clips, prioritized, to analyze.

        """

        # Creating empty list where hash values will go
        registered_speakers = []

        # Fetching raw list (every row) in subclips table.
        fetch_raw_list_subclips = "SELECT * FROM subclips ORDER BY id DESC"
        db_dataframe = pd.read_sql(fetch_raw_list_subclips, self.backend_interface.mysql)

        # Getting a weighted list with speakers' subclip hash and a starting weight list if not provided.
        speakers_id = set(db_dataframe['speaker'].tolist())

        # Converting the dataframe to a list of file names (indexed under column `hash` as
        # every uploaded subclip is hashed and saved with hash name) and getting the first 5*weight item in
        # database for every speaker.
        weight_list = WeightListType()
        for i in speakers_id:
            if len(weight) > 0:
                weight_list = {}
            for score in weight:
                fetch_speaker_for_hash_query = f'SELECT * FROM subclips WHERE hash = "{score}"'
                speaker_for_hash = \
                    pd.read_sql(fetch_speaker_for_hash_query, self.backend_interface.mysql)['speaker'].tolist()[0]
                weight_list[speaker_for_hash] = weight[score]

            try:
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}]"
                    f" -- Multiplier for {i} is {float(weight_list[i])}")
                speaker_selected_hash = \
                    db_dataframe.loc[db_dataframe['speaker'] == i].head(int(10 * float(weight_list[i])))[
                        'hash'].tolist()
                registered_speakers = registered_speakers + speaker_selected_hash
            except KeyError:
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}]"
                    f" ! -- Multiplier for {i} is impossible to calculate. Defaulting to 0.5")
                speaker_selected_hash = \
                    db_dataframe.loc[db_dataframe['speaker'] == i].head(int(10 * 0.5))[
                        'hash'].tolist()
                registered_speakers = registered_speakers + speaker_selected_hash
                pass

        return registered_speakers

    def get_speaker_from_hash(self, subclip) -> int:
        """Fetches the speaker id given a subclip name.

        Arguments
        ---------
        subclip : str
            Subclip name in mysql db.

        Returns
        -------
        speaker_id : int
            Speaker id for given subclip name.

        """

        # Creates a dataframe from the query result and gets the first element
        # of speaker columns
        fetch_speaker = f'SELECT speaker FROM subclips WHERE hash = "{subclip}"'
        speaker_id = pd.read_sql(fetch_speaker, self.backend_interface.mysql)['speaker'].tolist()[0]

        return speaker_id


class WeightListType:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getitem__(self, item):
        return 1
