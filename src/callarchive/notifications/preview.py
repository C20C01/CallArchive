import json
import os
import re
from dataclasses import asdict
from pathlib import Path
from tempfile import NamedTemporaryFile

from . import Delivery, Message, Receipt


class PreviewChannel:
    supports_idempotency = True

    def __init__(self, directory: Path):
        self.directory = directory

    def send(self, message: Message) -> Receipt:
        if not re.fullmatch(r"[a-f0-9]{64}", message.id):
            raise ValueError("Invalid notification identifier")
        filename = f"{message.id}.json"
        path = self.directory / filename
        data = json.dumps(asdict(message), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        self.directory.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=self.directory, delete=False) as stream:
            pending = Path(stream.name)
            try:
                stream.write(data.encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
                stream.close()
                try:
                    os.link(pending, path)
                except FileExistsError:
                    pass
            finally:
                stream.close()
                pending.unlink(missing_ok=True)
        if path.read_text(encoding="utf-8") != data:
            return Receipt(Delivery.REJECTED)
        return Receipt(Delivery.CONFIRMED, filename)
