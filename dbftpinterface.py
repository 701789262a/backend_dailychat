import json
import os
from ftplib import FTP

import mysql.connector
from typing import Tuple

import pandas as pd


class DbFtpInterface:
    def __init__(self):
        self.ftp = None
        self.cursor = None
        self.mysql = None

    def db_login(self, mysql_server, mysql_user, mysql_password)-> None:
        self.mysql = mysql.connector.connect(host=mysql_server, user=mysql_user, password=mysql_password,
                                             database='dba')
        self.cursor = self.mysql.cursor()

    def ftp_login(self, ftp_server, ftp_user, ftp_password)-> None:
        self.ftp = FTP(host=ftp_server)
        self.ftp.login(user=ftp_user, passwd=ftp_password)
        self.ftp.cwd('subclips')

        # Only for testing, folder with trial subclips
        self.ftp.cwd('trial')

    def create_speaker(self, name) -> Tuple:
        """Create a new speaker in [dba.speakers] with given name

        Arguments
        ---------
        name: str
            Chosen name for new speaker

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
            return create_user_query_result, name
        except mysql.connector.errors.IntegrityError:
            return None, None

    def insert_subclip(self, subclip, first_username, speaker, ordered_results) -> None:
        """Indexes subclip into database and stores into FTP server.
        Deletes the tmp file after the process is over.

        Arguments
        ---------
        subclip : tuple
            Subclip path in tmp folder to get indexed and segment.
        first_username : int
            User id of the user who recorded the subclip.
        speaker : int
            Speaker id to whom it should be associated to. [0 = unknown].
        ordered_results : str
            Hash referring to the hash that confirmed the subclip.
        """
        # Getting path from subclip
        path = subclip[0]
        # printare il path perche non risulta completo il percorso, errore linea 199, sicuramente perche la cartella non e stata inserita prima, bisogna metterla a mano - non male - tramite percorso hard-coded, sempre in linea 199
        # Stores the tmp .wav subclip into the FTP server
        self.ftp.storbinary('STOR ' + path + '.wav', open('tmp_audio_files_save/' + path + '.wav', 'rb'))

        # Removes the tmp .wav subclip from memory
        os.remove('tmp_audio_files_save/' + path + '.wav')

        # Inserts the stored subclip into the db
        handle_single_quote_from = "'"
        handle_single_quote_to = "''"
        insert_subclip_query = f'INSERT INTO subclips (hash, evaluated_by, first_username, speaker, segment_json) values ' \
                               f'("{path}", "{ordered_results}",{first_username}, {speaker}, ' \
                               f"'[{json.dumps(subclip[1]).replace(handle_single_quote_from, handle_single_quote_to)}]')"

        print(insert_subclip_query)
        self.cursor.execute(insert_subclip_query)
        self.mysql.commit()
