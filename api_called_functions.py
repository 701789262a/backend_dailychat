import datetime
import yaml
import speechmetrics


class AppFunction:

    def __init__(self, config_file, translator, identificator, mtb):
        """Initializes the object for callable function from app.

        Arguments
        ---------
        config_file : str
            Path to config file where settings are stored.
        """

        self.config = yaml.unsafe_load(open(config_file, 'r').read())
        # self.middle_to_backend = DbFtpInterface()
        # self.middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
        #                                 config['auth']['db']['pass'], config['auth']['db']['port'])
        # self.middle_to_backend.ftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
        #                                  config['auth']['ftp']['pass'], config['auth']['ftp']['port'])
        # self.identificator = VoiceIdentification(self.middle_to_backend, 0.25, config['identification']['device'],
        #                                          config['identification']['identification_workers'],
        #                                          config['identification']['levels'])
        # self.translator = VoiceDiarization(config['diarization']['model'], config['diarization']['device'],
        #                                    config['diarization']['dualgpu']
        #                                    if config['diarization']['device'] == 'cuda' else False)
        self.middle_to_backend = mtb
        self.translator = translator
        self.identificator = identificator

        # Loading model to calculate probability of subclip containing speech
        self.metric = speechmetrics.load('absolute', 20)

    # medium cpu 65s win diar
    # small cpu 31s win diar

    def create_new_speaker(self, sender_user_id, new_speaker_clip, speaker_name):
        """Creates a new speaker from a given clip, storing the speaker in FTP server
        (with genesis clips) and in database

        Arguments
        ---------
        sender_user_id : int
            UserId of the user which recorded the clip
        new_speaker_clip : str
            Path for the clip to hash and store
        speaker_name : str
            Name given to global/[local] speaker by user

        Returns
        -------

        """
        subclip_names = self.translator.clip_transcribe(new_speaker_clip)
        speaker_id, name = self.middle_to_backend.create_speaker(speaker_name)
        for subclip in subclip_names:
            # la subclip con hash subclip[0] viene inserita dall user 1, indentificata da se stessa (subclip[0]) e
            # lo speaker 'e 2
            self.middle_to_backend.insert_subclip(subclip, sender_user_id, speaker_id, subclip[0], timestamp_at_start)

    def manage_regular_job(self, sender_user_id, speaker_clip, timestamp_at_start):
        """Transcribes and identify the speaker. Saves subclips to FTP and stores info on db.

        Arguments
        ---------
        sender_user_id : int
            User id who sent the job request.
        speaker_clip : str
            Path for the .wav clip.
        timestamp_at_start : int
            Timestamp at which the clip recording started.

        Returns
        -------
        analyzed_subclip : dict or int
            Dictionary with subclip diarization and speaker identification or error code if occurred
        """

        # Transcribes(diarization) the clip given and divides it into sublips(segments).
        subclips_name = self.translator.clip_transcribe(speaker_clip)

        # Dict containing subclip hash and speaker.
        analyzed_subclip = {speaker_clip: {'subclips': list()}}

        # Interating thorugh every subclip to identify who's speaking in each subclip.
        for subclip in subclips_name:

            # Generating speech probability for subclip
            scores = self.metric('tmp_audio_files_save/' + subclip[0] + '.wav')
            MOSNet_score = 100 - (scores['mosnet'] * 20)
            SRMR_score = 100 - (scores['srmr'] * 100)

            # Identifying subclip speaker if no_speech_prob is lower than threshold
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
                  f"No speech prob (lower is better):\n"
                  f" * \t\t Whisper included: {subclip[1]['no_speech_prob']}\n"
                  f" * \t\t Speechmetrics MOSNet: {MOSNet_score}\n"
                  f" * \t\t Speechmetrics SRMR: {SRMR_score}")

            avg_speech_score = (subclip[1]['no_speech_prob'] + MOSNet_score + SRMR_score) / 3

            if avg_speech_score < self.config['diarization']['no_speech_prob']:
                identification = self.identificator.identify_speaker(subclip, sender_user_id, timestamp_at_start)

                if identification[0] != 0:
                    return identification[0], ''

                # Appending identification and subclip to dict
                analyzed_subclip[speaker_clip]['subclips'].append([subclip, identification])
        return 0, analyzed_subclip
