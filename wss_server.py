from datetime import datetime
import hashlib
import threading

import flask
import yaml
from flask import request
from waitress import serve
from dbftpinterface import DbFtpInterface
from mainservice import MainService
from stage1_voic_diar import VoiceDiarization
from stage2_voic_iden import VoiceIdentification

UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
config = yaml.unsafe_load(open("config.yaml", 'r').read())
middle_to_backend = DbFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                           config['auth']['db']['pass'], config['auth']['db']['port'])
middle_to_backend.ftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                            config['auth']['ftp']['pass'], config['auth']['ftp']['port'])
identificator = VoiceIdentification(middle_to_backend, 0.25, config['identification']['device'],
                                    config['identification']['identification_workers'],
                                    config['identification']['levels'])
translator = VoiceDiarization(config['diarization']['model'], config['diarization']['device'],
                              config['diarization']['dualgpu']
                              if config['diarization']['device'] == 'cuda' else False)


@app.route('/', methods=['POST'])
def addspeaker():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Connected...")
    tmp_file_name = str(int(datetime.utcnow().timestamp()))
    with open(f'tmp{tmp_file_name}.wav', 'wb') as f:
        request.files['file'].save(f)

    timestamp_at_start = request.values['timestamp'].split('/')[-1].split('.')[0]
    with open(f'{"tmp" + tmp_file_name + ".wav"}', 'rb') as f:
        file_to_hash_binary = f.read()
        clip_hash = hashlib.sha256(file_to_hash_binary).hexdigest()
        f.close()
        with open(f'{clip_hash}.wav', 'wb') as g:
            g.write(file_to_hash_binary)
            g.close()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Passed to threaded")

    threading.Thread(target=dedicated_thread_connection, args=(clip_hash, timestamp_at_start,)).start()
    return '', 200


@app.route('/', methods=['GET'])
def default():
    page = "<html>" \
           "<body>" \
           "<p>Use POST instead!</p>" \
           "</body>" \
           "</html>"
    return page, 200


def dedicated_thread_connection(clip_hash, timestamp_at_start):
    mainapi = MainService(translator, identificator, middle_to_backend)
    result, time_took, clip_length_seconds = mainapi.main_job(1, clip_hash, timestamp_at_start)
    print(result)
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
        f"Job took {time_took}s; "
        f"Speed factor {time_took / clip_length_seconds} (lower is better)")


if __name__ == "__main__":
    config = yaml.unsafe_load(open("config.yaml", 'r').read())
    # app.run(debug=False, host=config['httpserver']['ip'], port=config['httpserver']['port'], use_reloader=False)
    serve(app, host=config['httpserver']['ip'], port=config['httpserver']['port'])
