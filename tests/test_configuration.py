import tempfile
import unittest
from pathlib import Path

from callarchive.configuration import ConfigError, load_config


class ConfigurationTest(unittest.TestCase):
    def test_section_settings_and_relative_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            source = folder / "callarchive.toml"
            source.write_text("""
[backend]
port = 8876
database = "records/main.sqlite"
admins = ["root-operator"]
[transcription]
engine = "command"
engine_id = "engine-v1"
command = ["speech-to-text"]
[archive]
attachments = "files"
[outbound]
manifest = "samples/calls.json"
owner = "operator"
[notifications]
page_size = 12
""", encoding="utf-8")
            settings = load_config(source)
            self.assertEqual(settings.backend.port, 8876)
            self.assertEqual(settings.backend.database, folder / "records/main.sqlite")
            self.assertEqual(settings.transcription.database, folder / "data/transcription.sqlite")
            self.assertEqual(settings.transcription.command, ["speech-to-text"])
            self.assertEqual(settings.archive.attachments, folder / "files")
            self.assertEqual(settings.outbound.manifest, folder / "samples/calls.json")
            self.assertEqual(settings.notifications.page_size, 12)

    def test_unknown_fields_are_reported_without_showing_values(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "conf.toml"
            source.write_text('[backend]\nsecret_token = "sensitive-value"\n', encoding="utf-8")
            with self.assertRaises(ConfigError) as captured:
                load_config(source)
            self.assertIn("backend.secret_token", str(captured.exception))
            self.assertNotIn("sensitive-value", str(captured.exception))
