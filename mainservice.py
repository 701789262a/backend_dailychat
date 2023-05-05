import json

from api_called_functions import AppFunction


class MainService:
    def __init__(self):
        self.api = AppFunction('config.yaml', 'cpu', 'cpu')

    def main_job(self, clip_hash):
        result = self.api.manage_regular_job(1, clip_hash + ".wav")
        return json.dumps(result)
