import argparse
import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
import uvicorn

from ..archive.local import LocalArchive
from ..configuration import config_from_args
from ..observability import configure_logging, event
from ..http import load_credentials
from .api import create_app
from .import_outbound import retry_call_recordings, run_import
from .report import run_report
from .password_accounts import load_password_accounts
from .providers import configured_archive
from .reporting import ReportDelivery
from .store import ARCHIVE_HISTORY_SECONDS, ArchiveStore
from .transcription_cleanup import flush_retired_transcriptions
from .workflow import Workflow


def maintain_archive(store, reports, *, now=None, history_seconds=ARCHIVE_HISTORY_SECONDS):
    store.purge_archive_cache(now, history_seconds=history_seconds)
    reports.redact_snapshots(now=now)


async def maintain_transcriptions(store, service_url, token):
    async with httpx.AsyncClient(base_url=service_url, timeout=20, trust_env=False,
                                 headers={"Authorization": f"Bearer {token}"}) as client:
        return await flush_retired_transcriptions(store, client)


@asynccontextmanager
async def archive_workflow(database, attachments, service_url, token, settings):
    async with AsyncExitStack() as resources:
        client = await resources.enter_async_context(
            httpx.AsyncClient(base_url=service_url, timeout=20, trust_env=False,
                              headers={"Authorization": f"Bearer {token}"})
        )
        providers = {}
        for name, provider_settings in settings.archive.providers.items():
            provider_token = os.getenv(provider_settings.token_env) if provider_settings.token_env else None
            if provider_settings.token_env and not provider_token:
                event("archive_provider_not_configured", level="WARNING", reason="missing_provider_credential")
                continue
            providers[name] = await configured_archive(resources, name, provider_settings)
        store = ArchiveStore(database)
        yield Workflow(store, client, LocalArchive(attachments), providers=providers)


async def check_alerts(settings, database):
    if settings.notifications.owner:
        await asyncio.to_thread(run_report, settings, "alerts", database=database)


async def run_due(database, attachments, service_url, token, settings, *, overlap_seconds=None):
    async with archive_workflow(database, attachments, service_url, token, settings) as worker:
        now = datetime.now(timezone.utc)
        outbound = settings.outbound
        processed_new = set()
        try:
            if outbound.owner and (outbound.active_source or outbound.source):
                loop = asyncio.get_running_loop()

                async def advance(recording_ids):
                    for recording_id in recording_ids:
                        await worker.resume(recording_id)
                        processed_new.add(recording_id)

                def advance_new(recording_ids):
                    asyncio.run_coroutine_threadsafe(advance(recording_ids), loop).result()

                await asyncio.to_thread(run_import, settings, database=database, scheduled_at=now,
                                        overlap_seconds=overlap_seconds, advance_new=advance_new)
        except Exception as error:
            event("outbound_import_stopped", level="ERROR", reason=type(error).__name__)
            raise
        finally:
            processed = await worker.run_due(already_processed=processed_new)
            await check_alerts(settings, database)
        return processed


def main():
    parser = argparse.ArgumentParser(description="Serve the archive API or advance queued recordings.")
    parser.add_argument("mode", nargs="?", choices=("serve", "run-due", "cleanup"), default="serve")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--attachments", type=Path)
    parser.add_argument("--transcription-url")
    parser.add_argument("--overlap-hours", type=int, help="Scan overlap for the run-due command.")
    args = parser.parse_args()
    if args.overlap_hours is not None and (args.mode != "run-due" or args.overlap_hours < 0):
        parser.error("--overlap-hours requires run-due and a nonnegative value.")
    settings = config_from_args(parser, args)
    defaults = {
        "database": settings.backend.database, "port": settings.backend.port,
        "attachments": settings.archive.attachments, "transcription_url": settings.backend.transcription_url,
    }
    for key, value in defaults.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    component = {"serve": "archive-api", "run-due": "archive-worker",
                 "cleanup": "archive-cleanup"}[args.mode]
    runtime_log = configure_logging(settings.logging, component)
    if args.mode == "cleanup":
        store = ArchiveStore(args.database)
        maintain_archive(store, ReportDelivery(store),
                         history_seconds=settings.archive.history_retention_days * 86400)
        token = os.getenv("CALLARCHIVE_TRANSCRIPTION_TOKEN")
        if token:
            asyncio.run(maintain_transcriptions(store, args.transcription_url, token))
        return
    event("process_started")
    if args.mode == "run-due":
        token = os.getenv("CALLARCHIVE_TRANSCRIPTION_TOKEN")
        if not token:
            parser.error("Set CALLARCHIVE_TRANSCRIPTION_TOKEN for the archive worker.")
        try:
            overlap = args.overlap_hours * 3600 if args.overlap_hours is not None else None
            asyncio.run(run_due(args.database, args.attachments, args.transcription_url, token, settings,
                                overlap_seconds=overlap))
        except KeyboardInterrupt:
            pass
        except ValueError:
            parser.error("Archive batch failed. Check the runtime log and the configured adapters.")
        return
    environment_admins = os.getenv("CALLARCHIVE_ADMIN_OWNERS")
    admins = settings.backend.admins if environment_admins is None else environment_admins.split(",")
    admins = {name.strip() for name in admins if name.strip()}
    try:
        accounts = load_password_accounts(settings.backend.account_file) if settings.backend.account_file else None
    except ValueError as error:
        parser.error(str(error))
    credentials = load_credentials()

    workflow_token = os.getenv("CALLARCHIVE_TRANSCRIPTION_TOKEN")
    completion_token = os.getenv("CALLARCHIVE_ARCHIVE_TRIGGER_TOKEN")

    async def resume_transcription(job_id):
        async with archive_workflow(args.database, args.attachments, args.transcription_url, workflow_token, settings) as worker:
            if await worker.resume_job(job_id):
                await check_alerts(settings, args.database)

    async def resume_recording(recording_id):
        async with archive_workflow(args.database, args.attachments, args.transcription_url, workflow_token, settings) as worker:
            await worker.resume_call(recording_id)
            await check_alerts(settings, args.database)

    async def resume_call(call_key):
        async with archive_workflow(args.database, args.attachments, args.transcription_url, workflow_token, settings) as worker:
            await asyncio.to_thread(retry_call_recordings, settings, worker.store, call_key)
            await worker.resume_call(call_key)
            await check_alerts(settings, args.database)

    def app(upstream=None):
        return create_app(args.database, credentials, admins=admins, runtime_log=runtime_log,
                          external_archive_id=settings.archive.active_provider, password_accounts=accounts,
                          secure_cookies=settings.backend.secure_cookies, operator_transcription=upstream,
                          transcription_api_owners=settings.backend.transcription_api_owners,
                          transcription_admin_results=settings.backend.transcription_admin_results,
                          transcription_admin_system_results=settings.backend.transcription_admin_system_results,
                          followup_names=settings.backend.followup_names, attachments=args.attachments,
                          completion_token=completion_token,
                          resume_transcription=resume_transcription if workflow_token else None,
                          resume_recording=resume_recording if workflow_token else None,
                          resume_call=resume_call if workflow_token else None)

    browser_token = os.getenv("CALLARCHIVE_BROWSER_TRANSCRIPTION_TOKEN")
    if not browser_token:
        uvicorn.run(app(), host="127.0.0.1", port=args.port, access_log=False)
        return

    async def serve_pages():
        async with httpx.AsyncClient(base_url=args.transcription_url, timeout=20, trust_env=False,
                                     headers={"Authorization": f"Bearer {browser_token}"}) as upstream:
            server = uvicorn.Server(uvicorn.Config(app(upstream), host="127.0.0.1", port=args.port, access_log=False))
            await server.serve()

    asyncio.run(serve_pages())


if __name__ == "__main__":
    main()
