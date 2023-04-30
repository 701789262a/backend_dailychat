import datetime
import yaml
from yaml import SafeLoader

from stage2_voic_iden import VoiceIdentification
from stage1_voic_diar import VoiceDiarization
from dbftpinterface import DbFtpInterface

translator = VoiceDiarization('base', 'cpu')
with open ('config.yaml','r') as yy:
    config = yaml.load(yy,Loader=SafeLoader)
print(datetime.datetime.now())
subclips_name = translator.clip_transcribe('gianmarco.wav')
print(datetime.datetime.now())

middle_to_backend = DbFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'], config['auth']['db']['pass'])
middle_to_backend.ftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'], config['auth']['ftp']['pass'])

identificator = VoiceIdentification(middle_to_backend,0.25)

def general_job_from_client():
    for subclip in subclips_name:
        print(f"Testing subclip {subclip[0]}")
        print(f"result {identificator.identify_speaker(subclip, 1)}")
    # sent subclip subclip[0] is identified by speaker result [1]

def create_new_speaker(name):
    id,name=middle_to_backend.create_speaker(name)
    for subclip in subclips_name:
        # la subclip con hash subclip[0] viene inserita dall user 1, indentificata da se stessa (subclip[0]) e
        # lo speaker 'e 2
        middle_to_backend.insert_subclip(subclip,1,id,subclip[0])
print(datetime.datetime.now())
general_job_from_client()
print(datetime.datetime.now())
# print(identificator.identify_speaker('claudio.wav',0))
# exit(0)
