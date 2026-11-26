import io
from pathlib import Path
import unittest
from unittest.mock import patch

import numpy as np
import soundfile

from callarchive.transcription.nano import Nano, SAMPLE_RATE, Segment


def wav(samples):
    data = io.BytesIO()
    soundfile.write(data, samples, SAMPLE_RATE, format="WAV")
    return data.getvalue()


class NanoChannelTest(unittest.TestCase):
    def setUp(self):
        self.engine = Nano("nano-test", Path("models"))
        self.left = np.zeros(4 * SAMPLE_RATE, dtype=np.float32)
        self.right = np.zeros_like(self.left)
        self.left[:2 * SAMPLE_RATE] = 0.25
        self.right[SAMPLE_RATE:3 * SAMPLE_RATE] = -0.25

    def test_stereo_keeps_channel_and_time_even_when_speech_overlaps(self):
        def decode(audio, segments, indices, words, *, completed):
            self.assertEqual(words, "测试")
            self.assertEqual(completed, indices[0])
            return {indices[0]: "左侧" if np.max(audio) > 0 else "右侧"}

        content = wav(np.column_stack((self.left, self.right)))
        with patch.object(self.engine, "_voice_activity", side_effect=[[(0, 2000)], [(1000, 3000)]]), patch.object(
            self.engine, "_diarize"
        ) as diarize, patch.object(self.engine, "_decode_group", side_effect=decode):
            result = self.engine.transcribe(content, {"hotwords": ["测试"], "speaker_count": 2})
        diarize.assert_not_called()
        self.assertEqual(result["text"], "左声道：左侧\n右声道：右侧")
        self.assertEqual([(s["channel"], s["start_ms"], s["end_ms"]) for s in result["segments"]],
                         [("left", 0, 2000), ("right", 1000, 3000)])

    def test_mono_and_duplicate_stereo_follow_diarization(self):
        cases = ((self.left, 1, 1), (np.column_stack((self.left, self.left)), 2, 3))
        for samples, speakers, vad_calls in cases:
            with self.subTest(channels=samples.ndim), patch.object(
                self.engine, "_voice_activity", return_value=[(0, 2000)]
            ) as voice_activity, patch.object(self.engine, "_diarize", return_value=[Segment(0, 2, 0)]) as diarize, patch.object(
                self.engine, "_decode_group", return_value={0: "通话"}
            ):
                result = self.engine.transcribe(wav(samples), {"speaker_count": speakers})
                self.assertEqual(result["text"], "说话人 1：通话")
                self.assertEqual(diarize.call_args.args[2], speakers)
                self.assertEqual(voice_activity.call_count, vad_calls)
                self.assertNotIn("channel", result["segments"][0])
