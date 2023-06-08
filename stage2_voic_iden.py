import threading
from datetime import datetime
from threading import Thread, Semaphore
import queue

import pandas as pd
from speechbrain.pretrained import SpeakerRecognition
import warnings

from typing import List


class VoiceIdentification:
    def __init__(self, backend_interface, threshold, device, identification_workers, n_level):
        """Initialization method, selects pretrained model from speechbrain,
        connect to FTP indicated server. Connects to mysql db and Stores the
        threshold for which the identification should return True.
        Warning are suppressed to handle the fact that pandas officially only supports SQLAlchemy.

        Arguments
        ---------
        backend_interface : DbFtpInterface
            Communicator class for database and FTP server
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
        self.ftp_semaphore = None
        self.job_queue = None
        self.local_score = dict()
        self.verification = SpeakerRecognition.from_hparams(source="speechbrain/spkrec-ecapa-voxceleb",
                                                            savedir="pretrained_models/spkrec-ecapa-voxceleb",
                                                            run_opts={'device': device})
        self.identification_workers = identification_workers
        self.n_level = n_level
        self.threshold = threshold
        self.backend_interface = backend_interface
        warnings.filterwarnings("ignore")

    def identify_speaker(self, subclip, user, timestamp_at_start):
        """For a given subclip, it evaluates it against a list of
        pre-recorded prioritized (most used, most recent, added by)
        sub-clips and returns the best match.
        It inserts the subclips into FTP and db

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

        # Getting the prioritized list.
        # TODO: GENERATORE - SI LAVORA IN BATCH, IL PRIMO BATCH VIENE CREATO DA 1 O PIU FILE RAPPRESENTANTI TUTTI GLI
        #  UTENTI E SI DA UNO SCORE A OGNI UTENTE, SI RIFORMANO PIU BATCH RESTRINGENDO LE POSSIBILITA
        #  FIN QUANDO SI HA UNO SCORE SODDISFACENTE.
        #  COMPLESSITA N^2. UN FOR DI FOR, IL PRIMO FOR ELABORA TUTTI I BATCH MENTRE IL FOR ANNIDATO ELABORA
        #  LA SINGOLA SUBCLIP. POSSIBILITA DI FARE THREAD ALL INTERNO DI UN BATCH (SI ELABORANO TUTTE LE SUBCLIP
        #  IN PARALLELO E SI ASPETTA LA FINE DI TUTTI I THREAD PER PASSARE AL PROSSIMO BATCH)

        self.ftp_semaphore = Semaphore(1)

        # Working through batch to decrease the amount of subclips needed
        for batch_number in range(self.n_level):

            # Starting workers and job queue
            self.job_queue = queue.Queue()
            workers = [
                Thread(target=self.batch_worker, args=(self.job_queue, path, self.ftp_semaphore,))
                for _ in range(self.identification_workers)
            ]

            # Getting batch job following algorithm calculation
            registered_speakers_batch = self._get_batch_speaker_priority_on_check(user, self.local_score)

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] Current batch len {len(registered_speakers_batch)}")

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

        # Ordering the results from the best match to the worst.
        ordered_results = sorted(self.local_score.items(), key=lambda item: item[1], reverse=True)

        # Retrieves speaker id from db
        speaker_id = self.get_speaker_from_hash(ordered_results[0][0])

        # Insertion into db and FTP server and deleting from local memory
        self.backend_interface.insert_subclip(subclip, user, speaker_id, ordered_results[0][0], timestamp_at_start)

        return ordered_results[0][0], speaker_id, float(ordered_results[0][1]), float(
            ordered_results[0][1]) > self.threshold

    def batch_worker(self, q, path, semaphore):
        """Job to be executed in parallel for identification of speaker.

        Arguments
        ---------
        q : Queue
            Object queue which contains the job to be executed. Ends with as much None as there are workers
            to signal a stop (queue end).
        path : str
            Path where the subclip to match is stored.
        semaphore : threading.Semaphore
            Flag to allow max 1 thread to access the FTP subroutine.
        """

        # Getting job from queue.
        registered_speaker = q.get()

        # If EOQ (end of eueu) is hit, worker should be terminated.
        if registered_speaker is None:
            return

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] Pid {threading.get_native_id()}\tanalysing {registered_speaker}...")

        # Retrieving the pre-recorded subclip from the FTP server (file name = hash).
        with semaphore:
            stored_subclip = self.get_subclip_from_ftp(registered_speaker)

        # Match between given subclip and pre-recorded subclip
        try:
            score, prediction = self.verification.verify_files(path, stored_subclip)
            self.local_score[registered_speaker] = score
        except RuntimeError:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] Error opening {path}, probably corrupted file; thread {threading.get_native_id()}")
            pass

    def get_subclip_from_ftp(self, registered_speaker) -> str:
        """Retrieves pre-recorded sub-clip from FTP server.

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
        self.backend_interface.ftp.retrbinary("RETR " + registered_speaker + '.wav',
                                              open('tmp_audio_files/' + registered_speaker + '.wav', 'wb').write)

        return 'tmp_audio_files/' + registered_speaker + '.wav'

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
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] -- Multiplier for {i} is {float(weight_list[i])}")
                speaker_selected_hash = \
                    db_dataframe.loc[db_dataframe['speaker'] == i].head(int(10 * float(weight_list[i])))[
                        'hash'].tolist()
                registered_speakers = registered_speakers + speaker_selected_hash
            except KeyError:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] ! -- Multiplier for {i} is impossible to calculate. Defaulting to 0.5")
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
