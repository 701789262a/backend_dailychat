import json
import datetime
from api_called_functions import AppFunction


class MainService:
    def __init__(self,translator, identificator,mtb):
        self.firebase = None
        self.api = AppFunction('config.yaml',translator, identificator,mtb)

    def main_job(self, user, clip_hash, timestamp_at_start):
        """Main job called by dedicated_thread after organizing input data

        Arguments
        ---------
        user : int
            User id who sent the job request.
        clip_hash : str
            Calculated hash referring to clip. Clip is saved with hash + .wav; Used to know the target clip
        timestamp_at_start : int
            Timestamp received via API referring to clip start (clip starts at timestamp and ends at timestamp + x)

        Returns
        -------
        json_result, time_took, clip_length : tuple
            Results contains JSON result, time took for job and the total length (in seconds) of the clip.
        """

        time_start = datetime.datetime.now().timestamp()

        # Job is processed and result as a json string is received
        result = self.api.manage_regular_job(user, clip_hash + ".wav", timestamp_at_start)
        json_result = json.dumps(result)

        time_end = datetime.datetime.now().timestamp()

        for key in result.keys():
            try:
                clip_length = float(result[key]['subclips'][-1][0][1]['end'])
                return json_result, time_end - time_start, clip_length
            except IndexError:
                print(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] No audio file on this subclip")
                clip_length = 1
                pass
                return json_result, time_end - time_start, clip_length


