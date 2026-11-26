import tempfile
import unittest
from contextlib import AsyncExitStack, ExitStack
from pathlib import Path

from callarchive.backend.providers import configured_archive, driver_factory
from callarchive.backend.import_outbound import configured_source
from callarchive.configuration import load_config


def outbound_example(resources, name, options):
    return name, options


def archive_example(resources, name, options):
    return name, options


def notification_example(resources, name, options):
    from callarchive.notifications import Delivery, Receipt

    class Channel:
        supports_idempotency = False

        def send(self, message):
            with Path(options["output"]).open("a", encoding="utf-8") as target:
                kind = message.content.get("kind", "daily")
                target.write(f"{name} {message.destination} {kind}\n")
            return Receipt(Delivery.CONFIRMED)

    return Channel()


class ProviderConfigurationTest(unittest.IsolatedAsyncioTestCase):
    async def test_third_party_notification_uses_the_same_daily_and_alert_core(self):
        from datetime import date

        from callarchive.backend.report import run_report
        from callarchive.backend.store import ArchiveStore
        from test_service import sample_audio

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "local.toml"
            output = root / "transport.log"
            config.write_text(f"""
[backend]
database = "archive.sqlite"

[notifications]
channel = "test-transport"
owner = "operator"
destination = "team"

[notifications.channels.test-transport]
driver = "{__name__}:notification_example"

[notifications.channels.test-transport.options]
output = "{output.as_posix()}"
""", encoding="utf-8")
            settings = load_config(config)
            store = ArchiveStore(settings.backend.database)
            store.submit("operator", "audio", {"flow": "local", "options": {}, "call": {}}, sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "needs_review", error="attachment_write_failed")
            self.assertEqual(run_report(settings, "day", day=date(2026, 11, 25)), {"confirmed": 1})
            self.assertEqual(run_report(settings, "alerts"), {"confirmed": 1})
            self.assertEqual(run_report(settings, "alerts"), {"confirmed": 1})
            self.assertEqual(output.read_text(encoding="utf-8").splitlines(),
                             ["test-transport team daily", "test-transport team alert"])

    async def test_installed_factories_can_be_selected_by_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "local.toml"
            file.write_text(f"""
[outbound]
active_source = "test-outbound"
[outbound.sources.test-outbound]
driver = "{__name__}:outbound_example"
[outbound.sources.test-outbound.options]
region = "sample"
[archive]
active_provider = "test-archive"
[archive.providers.test-archive]
driver = "{__name__}:archive_example"
[archive.providers.test-archive.options]
region = "sample"
""", encoding="utf-8")
            settings = load_config(file)
            with ExitStack() as resources:
                self.assertEqual(configured_source(resources, settings.outbound),
                                 ("test-outbound", {"region": "sample"}))
            async with AsyncExitStack() as resources:
                self.assertEqual(await configured_archive(
                    resources, "test-archive", settings.archive.providers["test-archive"]),
                    ("test-archive", {"region": "sample"}))

    def test_invalid_factory_has_generic_error(self):
        for specification in ("other", "no_such_package:adapter", f"{__name__}:__name__"):
            with self.subTest(specification=specification), self.assertRaises(ValueError):
                driver_factory(specification)

    def test_cannot_select_unknown_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "local.toml"
            file.write_text('[outbound]\nactive_source = "missing"\n', encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(file)
