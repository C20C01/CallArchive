import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from callarchive.__main__ import main


class TranscriptionCommandTest(unittest.TestCase):
    def test_serve_uses_the_configured_web_gateway_owner(self):
        with TemporaryDirectory() as directory:
            config = Path(directory) / "callarchive.toml"
            config.write_text("""
[transcription]
database = "transcription.sqlite"
web_gateway_owner = "browser-gateway"
""", encoding="utf-8")
            identities = {
                "browser-gateway": {"token": "test-gateway-token"},
                "partner": {"token": "test-partner-token"},
            }
            with (
                patch("sys.argv", ["callarchive", "serve", "--config", str(config)]),
                patch.dict("os.environ", {"CALLARCHIVE_IDENTITIES": json.dumps(identities)}),
                patch("uvicorn.run") as run,
            ):
                main()
            run.assert_called_once()
            with TestClient(run.call_args.args[0]) as client:
                url = "/v1/transcription/external-jobs"
                gateway = client.get(url, headers={"Authorization": "Bearer test-gateway-token"})
                partner = client.get(url, headers={"Authorization": "Bearer test-partner-token"})
            self.assertEqual(gateway.status_code, 200)
            self.assertEqual(partner.status_code, 403)
