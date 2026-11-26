import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

from . import ArchiveConflict


class LocalArchive:
    """Publish immutable attachments under a local recording directory."""

    def __init__(self, root: Path):
        self.root = root

    def find_targets(self, recording_id):
        if not re.fullmatch(r"[a-f0-9]{32}", recording_id):
            raise ValueError("Invalid local archive target")
        return (recording_id,)

    def put_attachment(self, target, name, content):
        if not re.fullmatch(r"[a-f0-9]{32}", target) or name not in {
            "recording.wav", "recording.mp3", "recording.flac", "transcript.txt",
        }:
            raise ValueError("Invalid local attachment path")
        relative = Path(target) / name
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with NamedTemporaryFile(dir=path.parent, prefix=".attachment-", delete=False) as handle:
                pending = Path(handle.name)
                try:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                    handle.close()
                    try:
                        os.link(pending, path)
                    except FileExistsError:
                        pass
                finally:
                    handle.close()
                    pending.unlink(missing_ok=True)
        if path.read_bytes() != content:
            raise ArchiveConflict
        return relative.as_posix()
