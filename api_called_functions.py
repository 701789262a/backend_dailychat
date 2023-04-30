from stage2_voic_iden import VoiceIdentification
from stage1_voic_diar import VoiceDiarization
from dbftpinterface import DbFtpInterface
import yaml

config = yaml.load(open('config.yaml','r').read())
middle_to_backend = DbFtpInterface()
middle_to_backend.db_login(config['db']['host'], config['db']['user'], config['db']['pass'])
middle_to_backend.ftp_login(config['ftp']['host'], config['ftp']['user'], config['ftp']['pass'])
identificator = VoiceIdentification(middle_to_backend, 0.25)
translator = VoiceDiarization('small', 'cpu')
#medium cpu 65s win diar
#small cpu 31s win diar

def create_new_speaker(sender_user_id, new_speaker_clip, speaker_name):
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
    subclip_names = translator.clip_transcribe(new_speaker_clip)
    speaker_id, name = middle_to_backend.create_speaker(speaker_name)
    for subclip in subclip_names:
        # la subclip con hash subclip[0] viene inserita dall user 1, indentificata da se stessa (subclip[0]) e
        # lo speaker 'e 2
        middle_to_backend.insert_subclip(subclip, sender_user_id, speaker_id, subclip[0])


def create_job(sender_user_id, speaker_clip):
    subclips_name = translator.clip_transcribe(speaker_clip)
    for subclip in subclips_name:
        print(f"Testing subclip {subclip[0]}")
        print(f"result {identificator.identify_speaker(subclip, sender_user_id)}")
    # sent subclip subclip[0] is identified by speaker result [1]
