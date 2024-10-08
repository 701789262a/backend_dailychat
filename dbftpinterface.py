import json
import os
import ftplib
import threading
import time
from datetime import datetime

import firebase_admin
import mysql.connector
from typing import Tuple

import pandas as pd
from firebase_admin import credentials, firestore


def firebase_datastore_login() -> None:
    cred = credentials.Certificate("firebase_certificate/guardan-audio-79f666b3f835.json")
    firebase_admin.initialize_app(cred)


class DbFtpInterface:
    def __init__(self):
        self.firebase = None
        self.ftp = None
        self.cursor = None
        self.mysql = None

        # Multithread lock
        self._lock = threading.Lock()

    def db_login(self, mysql_server, mysql_user, mysql_password, port) -> None:
        """Logins into database and creates an object to interact with.

        Arguments
        ---------
        mysql_server : str
            IP Address for database.
        mysql_user : str
            Username for database.
        mysql_password : str
            Password for database.
        port : str
            Port for database
        """
        self.mysql = mysql.connector.connect(host=mysql_server, user=mysql_user, password=mysql_password,
                                             database='dba', port=port)
        self.cursor = self.mysql.cursor()

    def ftp_login(self, ftp_server, ftp_user, ftp_password, ftp_port, keepalive=False) -> None:
        """Logins into FTP server and creates an object to interact with.

        Arguments
        ---------
        ftp_server : str
            IP Address for FTP server.
        ftp_user : str
            Username for FTP server.
        ftp_password : str
            Password for FTP server.
        ftp_port : int
            Port for FTP server.
        keepalive : bool
            Flag to control keepalive, if True, a thread is started than keeps the collection alive
        """

        self.ftp = ftplib.FTP()
        self.ftp.connect(host=ftp_server, port=ftp_port)
        self.ftp.login(user=ftp_user, passwd=ftp_password)
        self.ftp.cwd('subclips')
        self.ftp.set_pasv(True)

        # Only for testing, folder with trial subclips
        self.ftp.cwd('trial')

        # Starts keepalive thread
        if keepalive:
            threading.Thread(target=self.keepalive).start()

    def keepalive(self):
        """Keepalive subroutine for FTP and MySQL connection
        """

        while True:
            time.sleep(30)
            self.ftp.sendcmd("NOOP")
            with self._lock:
                self.cursor.execute("SHOW DATABASES;")
                _ = self.cursor.fetchall()

    def create_speaker(self, name, subclip_id = None) -> Tuple:
        """Create a new speaker in [dba.speakers] with given name

        Arguments
        ---------
        name: str
            Chosen name for new speaker.
        subclip_id : str
            Subclip id to change speaker to if API called by selected tile.

        Returns
        -------
        speaker : tuple
            Pair containing (id, name) if query is successful, (None, None) otherwise
        """

        create_user_query = f'INSERT INTO speaker (name) values ("{name}");'
        try:
            self.cursor.execute(create_user_query)
            self.mysql.commit()
            get_create_user_row_query = f'SELECT * FROM speaker WHERE name = "{name}";'
            create_user_query_result = pd.read_sql(get_create_user_row_query, self.mysql)['id'].tolist()[0]

            # If subclip_id is passed, API is called by message tile and that tile's speaker_id must be changed
            if subclip_id is not None:
                change_subclip_speaker = f'UPDATE subclips SET speaker = "{create_user_query_result}" WHERE id = "{subclip_id}"'
                self.cursor.execute(change_subclip_speaker)
                self.mysql.commit()
            return create_user_query_result, name
        except mysql.connector.errors.IntegrityError:
            return None, None

    def insert_subclip(self, subclip, first_username, speaker, ordered_results, timestamp_at_start) -> None:
        """Indexes subclip into database and firebase-backend and stores into FTP server.
        Deletes the tmp file after the process is over.

        Arguments
        ---------
        subclip : list
            Subclip path in tmp folder to get indexed and segment.
        first_username : int
            User id of the user who recorded the subclip.
        speaker : int
            Speaker id to whom it should be associated to. [0 = unknown].
        ordered_results : str
            Hash referring to the hash that confirmed the subclip.
        timestamp_at_start : int
            Timestamp at which the clip recording started.
        """
        # Getting path from subclip
        path = subclip[0]
        # printare il path perche non risulta completo il percorso, errore linea 199, sicuramente perche la cartella non
        # e stata inserita prima, bisogna metterla a mano - non male - tramite percorso hard-coded, sempre in linea 199
        # Stores the tmp .wav subclip into the FTP server
        self.ftp.storbinary('STOR ' + path + '.wav', open('tmp_audio_files_save/' + path + '.wav', 'rb'))
        self.ftp.sendcmd('SITE CHMOD 644 ' + path + '.wav')
        # Removes the tmp .wav subclip from memory
        os.remove('tmp_audio_files_save/' + path + '.wav')

        # Inserts the stored subclip into the db
        handle_single_quote_from = "'"
        handle_single_quote_to = "''"
        insert_subclip_query = \
            f'INSERT INTO subclips (hash, evaluated_by, first_username, speaker, segment_json) values ' \
            f'("{path}", "{ordered_results}",{first_username}, {speaker}, ' \
            f"'[{json.dumps(subclip[1]).replace(handle_single_quote_from, handle_single_quote_to)}]')"

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}]{insert_subclip_query}")
        self.cursor.execute(insert_subclip_query)
        self.mysql.commit()

        self.push_chat_to_firebase(path, first_username,
                                   json.dumps(subclip[1]).replace(handle_single_quote_from, handle_single_quote_to),
                                   timestamp_at_start, speaker)

    def push_chat_to_firebase(self, subclip_hash, user, json_result, timestamp_at_start, speaker) -> None:
        """Pushes diarized and identified subclip to firebase server.

        Arguments
        ---------
        subclip_hash : str
            String containing hash for this subclip
        user : int
            ID for recording user
        json_result : str
            JSON compliant string with subclip result
        timestamp_at_start : int
            Timestamp at recording start
        speaker : int
            Speaker ID
        """

        results = json.loads(json_result)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}]{results}")
        try:
            firebase_datastore_login()
        except ValueError:
            # Firebase app already initialized
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] Firebase already started")
            pass
        self.firebase = firestore.client()
        doc_ref = self.firebase.collection(u'chats').document(str(user)).collection(u'messages').document(
            str(subclip_hash))
        doc_ref.set({
            u'id': str(subclip_hash),
            u'sender': str(user),
            u'speaker': speaker,
            u'time': str(int(timestamp_at_start) + int(results['start']) * 1000),
            u'text': str(results['text']).strip()

        })


    def change_subclip_user(self, subclip_id, user, new_speaker):
        """Pushes to firebase and local DB a new speaker for selected subclip

        Arguments
        ---------
        subclip_id : str
            String containing ID (hash) referencing subclip to alter
        user : str
            ID for recording user
        new_speaker : str
            New chosen speaker
        """

        # Pushes query to local DB
        change_user_query = f'UPDATE subclips SET speaker = "{new_speaker}" WHERE hash = "{subclip_id}"'
        try:
            self.cursor.execute(change_user_query)
        except mysql.connector.errors.IntegrityError:
            # Catch if user is not present
            self.mysql.commit()
            self.create_speaker(new_speaker)
            self.mysql.commit()
            self.cursor.execute(change_user_query)
        self.mysql.commit()

        # Pushes query to firebase
        try:
            firebase_datastore_login()
        except ValueError:
            # Firebase app already initialized
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] Firebase already started")
            pass
        self.firebase = firestore.client()
        doc_ref = self.firebase.collection(u'chats').document(str(user)).collection(u'messages').document(
            str(subclip_id))
        # doc_ref.set({
        #     u'id': str(subclip_id),
        #     u'sender': str(user),
        #     u'speaker': new_speaker,
        #     u'time': str(int(timestamp_at_start) + int(results['start']) * 1000),
        #     u'text': str(results['text']).strip()
        #
        # })
        doc_ref.update({
            u'speaker': new_speaker
        })

    def get_username_from_speaker(self, user, speaker):
        """Retrieves the name associated with the speaker by user

        Arguments
        ---------
        user : str
            Username ID associated with the user that initiated the request
        speaker : str
            Speaker ID associated with subclip

        Returns
        -------
        username : str
            Name associated with speaker
        """

        # Pushes query to local DB into pandas dataframe
        get_username_query = f'SELECT * FROM speaker WHERE id = "{speaker}"'
        try:
            with self._lock:
                self.cursor.execute(get_username_query)
                get_username_query_result = self.cursor.fetchall()[0][1]
                print(get_username_query_result)
        except mysql.connector.errors.IntegrityError:

            # If name is not present, defaults to "None"
            return "None"

        return get_username_query_result

    def delete_subclip(self, id):
        """Deletes subclip from database; document stored on firebase is considered deleted by frontend

        Arguments
        ---------
        id : str
            Subclip id to delete
        """

        # Prepares and send query to MySQL
        delete_subclip_query = f'DELETE FROM subclips WHERE hash = "{id}"'
        try:
            with self._lock:
                self.cursor.execute(delete_subclip_query)
                self.mysql.commit()
        except mysql.connector.errors.IntegrityError:

            # If id is not present, defaults to "None"
            return "None"

