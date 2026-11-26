import argparse
import json
import os
import time
from contextlib import ExitStack
from pathlib import Path

import httpx

from .configuration import TranscriptionSettings, config_from_args
from .observability import configure_logging, event
from .http import load_credentials
from .transcription.engines import CommandEngine, DemoEngine
from .transcription.jobs import JobStore
from .transcription.worker import process_one


def load_engine(settings=None):
    settings = settings or TranscriptionSettings()
    env_command = os.getenv("CALLARCHIVE_ENGINE_COMMAND")
    env_model = os.getenv("CALLARCHIVE_NANO_MODEL_DIR")
    if env_command and env_model:
        raise ValueError("Configure one transcription engine.")
    mode = "command" if env_command else "nano" if env_model else settings.engine
    engine_id = os.getenv("CALLARCHIVE_ENGINE_ID") or settings.engine_id
    if mode == "demo":
        return DemoEngine()
    if not engine_id:
        raise ValueError("Set an engine ID for a custom engine.")
    if mode == "command":
        command = json.loads(env_command) if env_command else settings.command
        if not command:
            raise ValueError("Configure an argument array for the command engine.")
        return CommandEngine(command, name=engine_id)

    model = env_model or settings.nano_model_dir
    if not model:
        raise ValueError("Set the Nano model directory.")
    from .transcription.nano import Nano

    return Nano(engine_id, Path(model),
                vad_model=os.getenv("CALLARCHIVE_NANO_VAD_MODEL") or str(settings.nano_vad_model_dir or "fsmn-vad"),
                speaker_model=os.getenv("CALLARCHIVE_NANO_SPEAKER_MODEL") or str(settings.nano_speaker_model_dir or "cam++"),
                hotwords=settings.nano_hotwords, opening_hotwords=settings.nano_opening_hotwords,
                opening_seconds=settings.nano_opening_seconds, expected_speakers=settings.nano_expected_speakers,
                short_review=settings.nano_short_review)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("serve", "worker", "cleanup"))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--retention", type=int, help="Retention in seconds.")
    parser.add_argument("--max-unclaimed", type=int, help="Unclaimed retention in seconds.")
    args = parser.parse_args()
    config = config_from_args(parser, args)
    settings = config.transcription
    configure_logging(config.logging, {"serve": "transcription-api", "worker": "transcription-worker",
                                       "cleanup": "transcription-cleanup"}[args.mode])
    database = args.database if args.database is not None else settings.database
    port = args.port if args.port is not None else settings.port
    retention = args.retention if args.retention is not None else settings.retention
    max_unclaimed = args.max_unclaimed if args.max_unclaimed is not None else settings.max_unclaimed
    if min(retention, max_unclaimed) <= 0:
        parser.error("Retention values must be positive.")
    if args.mode == "cleanup":
        store = JobStore(database)
        store.discard_completed_external_audio()
        store.purge(retention, max_unclaimed)
        return
    try:
        engine = load_engine(settings)
    except (TypeError, ValueError) as error:
        parser.error(str(error))
    event("process_started")
    if args.mode == "serve":
        import uvicorn

        from .transcription.api import create_app

        app = create_app(database, load_credentials(), engine=engine.name,
                         retention=retention, max_unclaimed=max_unclaimed,
                         web_gateway_owner=settings.web_gateway_owner)
        uvicorn.run(app, host="127.0.0.1", port=port, access_log=False)
        return

    store = JobStore(database)
    callback_url = os.getenv("CALLARCHIVE_ARCHIVE_TRIGGER_URL")
    callback_token = os.getenv("CALLARCHIVE_ARCHIVE_TRIGGER_TOKEN")
    if callback_url and not callback_token:
        parser.error("Set CALLARCHIVE_ARCHIVE_TRIGGER_TOKEN with the archive trigger URL.")
    next_cleanup = 0
    try:
        with ExitStack() as resources:
            notify = None
            if callback_url:
                client = resources.enter_context(httpx.Client(
                    base_url=callback_url, timeout=5, trust_env=False,
                    headers={"Authorization": f"Bearer {callback_token}"},
                ))

                def notify(job_id):
                    client.post(f"/internal/transcriptions/{job_id}/complete").raise_for_status()

            while True:
                if time.monotonic() >= next_cleanup:
                    store.purge(retention, max_unclaimed)
                    next_cleanup = time.monotonic() + 60
                if not process_one(store, engine, on_finished=notify):
                    time.sleep(1)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
