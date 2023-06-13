import json
import datetime
from api_called_functions import AppFunction


class MainService:
    def __init__(self,translator, identificator,mtb):
        self.firebase = None
        self.api = AppFunction('config.yaml',translator, identificator,mtb)

    def main_job(self, user, clip_hash, timestamp_at_start):
        time_start = datetime.datetime.now().timestamp()
        result = self.api.manage_regular_job(user, clip_hash + ".wav", timestamp_at_start)
        json_result = json.dumps(result)
        time_end = datetime.datetime.now().timestamp()
        for key in result.keys():

            clip_length = float(result[key]['subclips'][-1][0][1]['end'])
        return json_result, time_end-time_start, clip_length
