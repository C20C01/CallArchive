import io
import logging
import math
import re
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from .engines import DemoEngine, TranscriptionEngine
from .progress import report_progress


SAMPLE_RATE = 16_000
LOGGER = logging.getLogger(__name__)


def _validate_expected_speakers(value):
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ValueError("expected_speakers must be a positive integer or None")


def cluster_turns(chunks, embeddings, cluster, postprocess, expected_speakers: int | None = 2):
    _validate_expected_speakers(expected_speakers)
    # Both spectral clustering and the long-recording UMAP branch expect NumPy.
    features = embeddings.detach().cpu().numpy()
    options = {} if expected_speakers is None else {"oracle_num": expected_speakers}
    return postprocess(chunks, None, cluster(features, **options), features)


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    speaker: int | None


def speech_segments(vad, turns):
    boundaries = [
        round((left[1] + right[0]) * 500) / 1000
        for left, right in zip(turns, turns[1:])
        if left[2] != right[2]
    ]
    result = []
    for start_ms, end_ms in vad:
        start, end = start_ms / 1000, end_ms / 1000
        edges = [start, *[t for t in boundaries if start + 0.3 < t < end - 0.3], end]
        for a, b in zip(edges, edges[1:]):
            count = math.ceil((b - a) / 20)
            for i in range(count):
                low = a + (b - a) * i / count
                high = a + (b - a) * (i + 1) / count
                overlap = Counter()
                for x, y, speaker in turns:
                    overlap[speaker] += max(0, min(high, y) - max(low, x))
                label, seconds = max(overlap.items(), key=lambda item: item[1], default=(None, 0))
                result.append(Segment(low, high, int(label) if seconds else None))
    return result


def _norm(text):
    return re.sub(r"[\W_]+", "", text.casefold())


def _follows(text, anchor, phrase):
    for size in range(min(8, len(anchor)), 2, -1):
        suffix = anchor[-size:]
        if text.count(suffix) == 1 and text.split(suffix, 1)[1].startswith(phrase):
            return True
    return False


def corrected_short_text(original, previous, readings, contexts):
    original_key = _norm(original)
    if not original_key:
        return original
    anchor = _norm(previous)
    context = [_norm(text) for text in contexts]
    if any(_follows(text, anchor, original_key) for text in context):
        return original
    candidates = [
        key for key, count in Counter(map(_norm, readings)).items()
        if len(key) >= 2 and count >= 2
        and not key.startswith(original_key) and not original_key.startswith(key)
        and sum(_follows(text, anchor, key) for text in context) >= 2
    ]
    if len(candidates) == 1:
        return next(text for text in readings if _norm(text) == candidates[0])
    return original


def _slice(audio, start, end):
    return audio[max(0, round(start * SAMPLE_RATE)):round(end * SAMPLE_RATE)]


def isolated_channels(audio, channel_vad):
    """Require speech on each side while the opposite channel is quiet."""
    import numpy as np

    frame_size = SAMPLE_RATE // 10
    count = len(audio) // frame_size
    if count < 20 or not np.isfinite(audio).all():
        return False
    power = (audio[:count * frame_size].reshape(count, frame_size, 2) ** 2).mean(axis=1)
    centers_ms = np.arange(count) * 100 + 50
    for side, vad in enumerate(channel_vad):
        speech = np.zeros(count, dtype=bool)
        for start, end in vad:
            first, last = centers_ms.searchsorted([start, end])
            speech[first:last] = True
        own, other = power[:, side], power[:, 1 - side]
        loud = own >= max(1e-6, own.max() * 0.001)
        independent = speech & loud & (own >= other * 100)
        if independent.sum() < max(10, speech.sum() * 0.15):
            return False
    return True


def _quiet_cut(audio, center, low, high):
    def energy(t):
        window = _slice(audio, t - 0.035, t + 0.035)
        return float((window ** 2).mean()) if len(window) else 0.0

    begin, end = max(low, center - 0.18), min(high, center + 0.18)
    if not begin <= center <= end:
        return center
    baseline = energy(center)
    candidates = [center, *(begin + i * 0.015 for i in range(math.floor((end - begin) / 0.015)))]
    best = min(candidates, key=lambda t: energy(t) * (1 + abs(t - center) * 4))
    if abs(best - center) >= 0.03 and energy(best) < baseline * 0.28:
        return best
    return center


class Nano(TranscriptionEngine):
    def __init__(self, name, model_dir: Path, *, vad_model="fsmn-vad", speaker_model="cam++",
                 hotwords=(), opening_hotwords=(), opening_seconds=0, expected_speakers=None,
                 short_review=True):
        if not name or name == DemoEngine.name:
            raise ValueError("Nano requires a distinct engine ID")
        self.name = name
        self.model_dir = model_dir
        self.vad_model = vad_model
        self.speaker_model = speaker_model
        self.hotwords = ",".join(hotwords)
        self.opening_hotwords = ",".join(opening_hotwords)
        self.opening_seconds = opening_seconds
        self.expected_speakers = expected_speakers
        self.short_review = short_review
        self._frontend = None
        self._vad_model = None

    def _voice_activity(self, audio):
        from funasr import AutoModel

        if self._vad_model is None:
            self._vad_model = AutoModel(model=self.vad_model, device="cpu", ncpu=2,
                                        disable_update=True, disable_pbar=True)
        vad = self._vad_model.generate(input=audio, fs=SAMPLE_RATE)[0]["value"]
        return [(start, end) for start, end in vad if end > start]

    def _prepare(self, recording, expected_speakers):
        import soundfile

        audio, rate = soundfile.read(io.BytesIO(recording), dtype="float32", always_2d=True)
        report_progress("detecting_speech")
        stereo = audio.shape[1] == 2 and expected_speakers in (None, 2)
        audio = audio.T.copy() if stereo else audio.mean(axis=1)
        if rate != SAMPLE_RATE:
            import torch
            from torchaudio.functional import resample

            audio = resample(torch.from_numpy(audio), rate, SAMPLE_RATE).numpy()
        if stereo:
            channel_vad = [self._voice_activity(channel) for channel in audio]
            if isolated_channels(audio.T, channel_vad):
                segments = [
                    Segment(segment.start, segment.end, side)
                    for side, vad in enumerate(channel_vad)
                    for segment in speech_segments(vad, [])
                ]
                LOGGER.info("Transcription uses isolated stereo channels")
                return audio, sorted(segments, key=lambda segment: segment.start)
            audio = audio.mean(axis=0)
        LOGGER.info("Transcription stage=vad")
        vad = self._voice_activity(audio)
        if not vad:
            return audio, []
        report_progress("diarizing")
        return audio, self._diarize(audio, vad, expected_speakers)

    def _diarize(self, audio, vad, expected_speakers):
        import torch
        from funasr import AutoModel
        from funasr.models.campplus.cluster_backend import ClusterBackend
        from funasr.models.campplus.utils import postprocess, sv_chunk

        if self._frontend is None:
            settings = dict(device="cpu", ncpu=2, disable_update=True, disable_pbar=True)
            self._frontend = (
                AutoModel(model=self.speaker_model, **settings),
                ClusterBackend(max_num_spks=15),
            )
        speaker_model, cluster = self._frontend
        LOGGER.info("Transcription stage=diarizing vad_segments=%s", len(vad))
        chunks = sv_chunk([
            [start / 1000, end / 1000, _slice(audio, start / 1000, end / 1000)]
            for start, end in vad
        ])
        features = []
        with torch.inference_mode():
            for _, _, samples in chunks:
                result, _ = speaker_model.model.inference([samples], device="cpu", fs=SAMPLE_RATE)
                features.append(result[0]["spk_embedding"])
            embeddings = torch.cat(features, dim=0)
            turns = cluster_turns(chunks, embeddings, cluster, postprocess,
                                  expected_speakers=expected_speakers)
        return speech_segments(vad, turns)

    def _recognizer(self, hotwords):
        import sherpa_onnx

        paths = [self.model_dir / name for name in (
            "encoder_adaptor.int8.onnx", "llm.int8.onnx", "embedding.int8.onnx",
        )]
        tokenizer = self.model_dir / "Qwen3-0.6B"
        required = [*paths, tokenizer / "tokenizer.json"]
        if not all(path.is_file() and path.stat().st_size for path in required):
            raise ValueError(f"Nano model files are missing in {self.model_dir}")
        return sherpa_onnx.OfflineRecognizer.from_funasr_nano(
            encoder_adaptor=str(paths[0]), llm=str(paths[1]), embedding=str(paths[2]),
            tokenizer=str(tokenizer), num_threads=2, language="中文", hotwords=hotwords,
        )

    def _decode_group(self, audio, segments, indices, hotwords, *, completed):
        recognizer = self._recognizer(hotwords)

        def decode(clip):
            stream = recognizer.create_stream()
            stream.accept_waveform(SAMPLE_RATE, clip)
            recognizer.decode_stream(stream)
            text = stream.result.text.strip()
            return "" if text.casefold() == "/sil" else text

        texts = {}
        total = len(segments)
        for count, i in enumerate(indices, completed + 1):
            segment = segments[i]
            began = time.monotonic()
            LOGGER.debug("Recognizing segment=%s/%s start=%.3f end=%.3f", i+1, total, segment.start, segment.end)
            try:
                texts[i] = decode(_slice(audio, segment.start, segment.end))
                report_progress("recognizing", count, total)
            except Exception:
                LOGGER.exception("Recognition failed segment=%s/%s start=%.3f end=%.3f elapsed=%.3f",
                                   i+1, total, segment.start, segment.end, time.monotonic()-began)
                raise
        return self._review_short(audio, segments, texts, decode) if self.short_review else texts

    def _review_short(self, audio, segments, texts, decode):
        import numpy as np

        reviewed = texts.copy()
        for i, original in texts.items():
            segment = segments[i]
            if not original or not 0 < segment.end - segment.start < 1.1 or not (
                texts.get(i - 1) and texts.get(i + 1)
            ):
                continue
            previous, following = segments[i - 1], segments[i + 1]
            a, b = segment.start, segment.end
            if abs(previous.end - a) >= 0.03 or abs(b - following.start) >= 0.03:
                continue
            shifted_a, shifted_b = a, b
            if previous.speaker != segment.speaker and abs(previous.end - a) < 0.002:
                shifted_a = _quiet_cut(audio, a, previous.start + 0.3, b - 0.3)
            if segment.speaker != following.speaker and abs(b - following.start) < 0.002:
                shifted_b = _quiet_cut(audio, b, a + 0.3, following.end - 0.3)
            clip = _slice(audio, shifted_a, shifted_b)
            plain = original if (a, b) == (shifted_a, shifted_b) else decode(clip)
            padded = decode(np.pad(clip, (2880, 4000)))
            if _norm(original) == _norm(plain) == _norm(padded):
                continue
            readings = [plain, padded, *(decode(_slice(audio, a + t, b + t)) for t in (-0.1, 0.1))]
            contexts = [decode(_slice(audio, a - left, b + right))
                        for left, right in ((1.6, 2.2), (2.2, 1.6), (2.6, 2.6))]
            reviewed[i] = corrected_short_text(original, texts[i - 1], readings, contexts)
        return reviewed

    def transcribe(self, audio: bytes, options: dict) -> dict:
        expected_speakers = options.get("speaker_count")
        if expected_speakers is None:
            expected_speakers = self.expected_speakers
        _validate_expected_speakers(expected_speakers)
        if options.get("language") not in (None, "zh", "zh-CN"):
            raise ValueError("Nano is configured for Chinese speech")
        report_progress("preparing")
        waveform, segments = self._prepare(audio, expected_speakers)
        stereo = waveform.ndim == 2
        groups = {}
        override = ",".join(options.get("hotwords", []))
        for index, segment in enumerate(segments):
            channel = segment.speaker if stereo else None
            opening = self.opening_hotwords if segment.end <= self.opening_seconds else ""
            hotwords = override or opening or self.hotwords
            groups.setdefault((channel, hotwords), []).append(index)

        texts = {}
        report_progress("recognizing", 0, len(segments))
        for (channel, hotwords), indices in groups.items():
            samples = waveform if channel is None else waveform[channel]
            texts.update(self._decode_group(samples, segments, indices, hotwords, completed=len(texts)))

        report_progress("formatting")

        speaker_numbers = {}
        lines = []
        timestamps = []
        for index, segment in enumerate(segments):
            text = texts[index]
            if not text:
                continue
            entry = {"text": text, "start_ms": round(segment.start * 1000),
                     "end_ms": round(segment.end * 1000)}
            if stereo:
                entry["channel"] = ("left", "right")[segment.speaker]
                label = ("左声道", "右声道")[segment.speaker]
            elif segment.speaker is not None:
                number = speaker_numbers.setdefault(segment.speaker, len(speaker_numbers) + 1)
                entry["speaker"] = number
                label = f"说话人 {number}"
            else:
                label = "说话人未识别"
            lines.append(f"{label}：{text}")
            timestamps.append(entry)
        if not lines:
            raise ValueError("Nano returned no text")
        return {"text": "\n".join(lines), "segments": timestamps}
