from stage2_voic_iden import VoiceIdentification
from stage1_voic_diar import VoiceDiarization
from dbftpinterface import DbFtpInterface
import yaml


class AppFunction:

    def __init__(self, config_file, diarization_device, identification_device):
        """Initializes the object for callable function from app.

        Arguments
        ---------
        config_file : str
            Path to config file where db and ftp and settings are stored.
        diarization_device : str
            Device where diarization will be performed [cpu, cuda].
        identification_device : str
            Device where identification will be performed [cpu, cuda].
        """

        config = yaml.unsafe_load(open(config_file, 'r').read())
        self.middle_to_backend = DbFtpInterface()
        self.middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                                        config['auth']['db']['pass'])
        self.middle_to_backend.ftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                                         config['auth']['ftp']['pass'])
        self.identificator = VoiceIdentification(self.middle_to_backend, 0.25, identification_device,
                                                 config['identification']['identification_workers'],
                                                 config['identification']['levels'])
        self.translator = VoiceDiarization('large-v2', diarization_device)

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
            self.middle_to_backend.insert_subclip(subclip, sender_user_id, speaker_id, subclip[0])

    def manage_regular_job(self, sender_user_id, speaker_clip):
        """Transcribes and identify the speaker. Saves subclips to FTP and stores info on db.

        Arguments
        ---------
        sender_user_id : int or str
            User id who sent the job request.
        speaker_clip : str
            Path for the .wav clip.

        Returns
        -------
        analyzed_subclip : dict
            Dictionary with subclip diarization and speaker identification.
        """

        # Transcribes(diarization) the clip given and divides it into sublips(segments).
        subclips_name = self.translator.clip_transcribe(speaker_clip)

        # Dict containing subclip hash and speaker.
        analyzed_subclip = {speaker_clip:{'subclips': list()}}

        # Interating thorugh every subclip to identify who's speaking in each subclip.
        for subclip in subclips_name:
            # Identifying subclip speaker
            identification = self.identificator.identify_speaker(subclip, sender_user_id)

            # Appending identification and subclip to dict
            analyzed_subclip[speaker_clip]['subclips'].append([subclip, identification])

        return analyzed_subclip
