from abc import ABC, abstractmethod
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory


class TranscriptionEngine(ABC):
    name: str

    @abstractmethod
    def transcribe(self, audio: bytes, options: dict) -> dict:
        """Convert audio to a result containing text."""


class DemoEngine(TranscriptionEngine):
    name = "demo"

    def transcribe(self, audio, options):
        return {"text": "这是演示结果。当前引擎仅用于验证转写服务流程。"}


class CommandEngine(TranscriptionEngine):
    def __init__(self, command, *, name="command", timeout=3600):
        if not isinstance(command, list) or not command or any(
            not isinstance(argument, str) or not argument for argument in command
        ):
            raise ValueError("Engine command must be a JSON array of arguments.")
        if not name or name == DemoEngine.name or timeout <= 0:
            raise ValueError("Engine ID must be distinct from demo, with a positive timeout.")
        self.command = tuple(command)
        self.name = name
        self.timeout = timeout

    def transcribe(self, audio, options):
        with TemporaryDirectory(prefix="callarchive-") as directory:
            root = Path(directory)
            recording = root / "audio"
            settings = root / "options.json"
            result = root / "result.json"
            recording.write_bytes(audio)
            settings.write_text(json.dumps(options, ensure_ascii=False), encoding="utf-8")
            subprocess.run([*self.command, str(recording), str(settings), str(result)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           stdin=subprocess.DEVNULL, check=True, timeout=self.timeout)
            return json.loads(result.read_text(encoding="utf-8"))
