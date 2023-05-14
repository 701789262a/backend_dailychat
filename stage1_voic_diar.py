import hashlib

import whisper
from pydub import AudioSegment
from stable_whisper import modify_model
from typing import List


class VoiceDiarization:

    def __init__(self, model, device='cpu'):
        """Loads whisper model (chosen by user) on selected device [CUDA, CPU].
        Modifies model making it compatible with whisper-stable.

        Arguments
        ---------
        model : str
            Model to be used from whisper set [tiny, base, small, medium, large, large-v2]
        device : str
            Device where the computation will be executed, default = cpu

        """
        self.model = whisper.load_model(model, device)
        modify_model(self.model)

    def clip_transcribe(self, clip_path) -> List[List]:
        """Transcribes a clip into segments dividing by pause, speaker, punctuation.
        From segments, sub-clips are created dividing the original file into smaller ones
        according to start/end data.
        Sub-clips are saved into temporary folder for immediate use.
        Hashed sub-clips and segments are returned.

        Arguments
        ---------
        clip_path : str
            Path for the clip to analyze

        Returns
        -------
        subclips_hash : list of list
            List of names and segment given to saved subclips
        """

        # Calling Whisper model to perform diarization. Model returns a list of "segments"
        # where each segment is a sentence (or less than a sentence) divided by pause,
        # punctuation, or speaker distinction.
        split_transcription = self.model.transcribe(clip_path, suppress_silence=False, temperature=0).to_dict()

        # Loading clip to perform cuts and create sub-clips
        song = AudioSegment.from_file(clip_path, format="wav")

        # Loading segments
        segments = split_transcription['segments']

        subclips_hash = []
        # Each segment is analyzed and cuts on clip are performed according to data.
        for segment in segments:
            # A +100 ms offset is added to delay the end as Whisper model isn't accurate
            # enough at calculating precise timestamps.
            start = (segment['start'] * 1000)
            end = (segment['end'] * 1000) + 100

            cut = song[start:end]

            # Hash is calculated and cut is saved into temporary folder for successive
            # and immediate use.
            filename = hashlib.sha256(cut.raw_data).hexdigest()
            print(f"[] Hashed as: {str(filename)} | "
                  f"with text: <<{segment['text']}>> | "
                  f"start: {str(segment['start'])} | "
                  f"stop: {str(segment['end'])}")

            cut.export('tmp_audio_files_save/' + filename + '.wav', format='wav')
            subclips_hash.append([filename, segment])

        return subclips_hash
