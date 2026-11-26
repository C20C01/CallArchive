import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from pathlib import Path

import httpx

from ..configuration import config_from_args
from ..observability import configure_logging, event
from ..outbound.http import HTTPSource
from ..outbound.manifest import ManifestSource
from .call_state import DOWNLOAD_ATTEMPT_LIMIT
from .outbound import OutboundImport
from .providers import driver_factory, provider_headers
from .scheduling import OutboundSchedule, ScanWindow
from .store import ArchiveStore


def timestamp(value):
    return datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)


def configured_source(resources, settings, manifest_override=None, *, source_id=None):
    if manifest_override is not None:
        return ManifestSource(manifest_override)
    name = source_id or settings.active_source or settings.source
    if name in settings.sources:
        provider = settings.sources[name]
        if provider.driver != "http":
            factory = driver_factory(provider.driver, {"qianke": "callarchive.outbound.qianke:create_source"})
            return factory(resources, name, provider.options)
    elif settings.active_source is None:
        if settings.manifest is not None:
            return ManifestSource(settings.manifest)
        provider = settings
    else:
        raise ValueError("Outbound source is not configured.")
    if provider.url is None:
        raise ValueError("Set an outbound manifest or provider URL.")
    client = resources.enter_context(httpx.Client(base_url=str(provider.url),
                                                  headers=provider_headers(provider.token_env),
                                                  timeout=20, trust_env=False))
    return HTTPSource(client)


def configured_loader(settings, store, owner, source_id, source):
    outbound = settings.outbound
    return OutboundImport(store, owner, source_id, source,
                          archive_instance=settings.archive.active_provider if outbound.flow == "remote" else None,
                          skip_call_duration_ms=outbound.skip_call_duration_ms, min_free_mib=outbound.min_free_mib,
                          recording_followup_interval_seconds=outbound.recording_followup_interval_seconds,
                          recording_followup_windows_per_poll=outbound.recording_followup_windows_per_poll)


def retry_call_recordings(settings, store, call_id):
    outbound = settings.outbound
    if not outbound.owner:
        return
    with closing(store.connect()) as db:
        row = db.execute("""SELECT source FROM outbound_console_retries JOIN outbound_calls USING(call_id)
                             WHERE call_id=? AND owner=?""", (call_id, outbound.owner)).fetchone()
    if row is None:
        return
    source_id = row["source"]
    if outbound.active_source is None and source_id != outbound.source:
        return
    with ExitStack() as resources:
        source = configured_source(resources, outbound, source_id=source_id)
        loader = configured_loader(settings, store, outbound.owner, source_id, source)
        loader.refresh_waiting(only_call=call_id)
        loader.download(only_calls=(call_id,))


def continue_previous_sources(settings, store, owner, active_source, *, now):
    outbound = settings.outbound
    with closing(store.connect()) as db:
        sources = db.execute("""SELECT DISTINCT c.source FROM outbound_calls c
            WHERE c.owner=? AND c.source<>?
              AND c.call_id NOT IN (SELECT call_key FROM console_call_removals)
              AND c.call_id NOT IN (SELECT call_id FROM outbound_short_calls)
              AND (c.complete=0 OR c.call_id IN (SELECT call_id FROM outbound_recordings
                    WHERE recording_id IS NULL AND error IS NOT 'invalid_audio' AND download_attempts<?))
            ORDER BY c.source""", (owner, active_source, DOWNLOAD_ATTEMPT_LIMIT)).fetchall()
    imported = pending = 0
    for row in sources:
        source_id = row["source"]
        with ExitStack() as resources:
            try:
                source = configured_source(resources, outbound, source_id=source_id)
            except (OSError, ValueError) as error:
                event("outbound_recording_lookup_failed", level="WARNING", source=source_id,
                      reason=type(error).__name__)
                continue
            loader = configured_loader(settings, store, owner, source_id, source)
            loader.refresh_waiting(now=now)
            sourced_imports, sourced_failures = loader.download()
            imported += sourced_imports
            pending += sourced_failures
    return imported, pending


def run_import(settings, *, owner=None, source_id=None, database=None, manifest=None,
               start=None, end=None, scheduled_at=None, now=None, write_guard=None,
               overlap_seconds=None, advance_new=None):
    outbound = settings.outbound
    owner = owner or outbound.owner
    active_source = outbound.active_source or outbound.source
    source_id = source_id or active_source
    database = settings.backend.database if database is None else database
    scheduled = scheduled_at is not None
    clock = (lambda: datetime.now(timezone.utc)) if now is None else lambda: now
    now = clock()
    history_seconds = settings.archive.history_retention_days * 86400
    with ExitStack() as resources:
        source = configured_source(resources, outbound, manifest, source_id=source_id)
        store = ArchiveStore(database, write_guard=write_guard)
        loader = configured_loader(settings, store, owner, source_id, source)

        def scan_cutoff(db):
            return store.outbound_scan_cutoff(db, clock().timestamp(), history_seconds)

        with closing(store.connect()) as db:
            oldest = datetime.fromtimestamp(scan_cutoff(db), timezone.utc)
        schedule = None
        if scheduled:
            overlap = outbound.regular_lookback_seconds
            if overlap is None:
                overlap = getattr(source, "scan_overlap_seconds", 0)
            schedule = OutboundSchedule(
                store, owner, source_id, interval=outbound.poll_interval_seconds,
                lookback=outbound.initial_lookback_seconds, overlap=overlap,
            )
            window = schedule.window(scheduled_at, overlap=overlap_seconds, oldest=oldest)
        else:
            if start.utcoffset() is None or end.utcoffset() is None or start >= end:
                raise ValueError("Expected a valid time range")
            window = ScanWindow(max(start, oldest), end) if end > oldest else None
        scanned_window = None
        new_imported = new_pending = 0
        attempted = set() if advance_new is not None else None
        try:
            while window is not None:
                seen_cursors = set()
                while not loader.scan_page(*window, scan_cutoff=scan_cutoff, seen_cursors=seen_cursors):
                    pass
                # Completed windows overlap or join at the previous watermark.
                if scanned_window is None:
                    scanned_window = window
                else:
                    scanned_window = ScanWindow(min(scanned_window.start, window.start), window.end)
                if schedule is None:
                    break
                schedule.finish(window)
                window = schedule.window(scheduled_at, overlap=overlap_seconds, oldest=oldest)
            if advance_new is not None and loader.scanned_call_ids:
                downloaded = []
                new_imported, new_pending = loader.download(only_calls=loader.scanned_call_ids,
                                                           attempted=attempted, downloaded=downloaded)
                if downloaded:
                    advance_new(downloaded)
            loader.refresh_waiting(now=now, scanned_window=scanned_window)
        finally:
            imported, pending = loader.download(attempted=attempted)
            imported += new_imported
            pending += new_pending
    if scheduled and source_id == active_source and manifest is None and outbound.active_source is not None:
        previous_imports, previous_failures = continue_previous_sources(settings, store, owner, active_source, now=now)
        imported += previous_imports
        pending += previous_failures
    if scanned_window:
        status = "scan_finished" if loader.calls_returned else "scan_empty"
    else:
        status = "scan_current"
    through = scanned_window.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if scanned_window else None
    event("outbound_import_finished", imported=imported, pending=pending, status=status,
          scan_through=through, calls_returned=loader.calls_returned)
    return imported, pending


def main():
    parser = argparse.ArgumentParser(description="Import outbound calls and recordings.")
    parser.add_argument("--config", type=Path)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("--owner")
    parser.add_argument("--source", help="Stable identifier for this source instance.")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--start", type=timestamp)
    parser.add_argument("--end", type=timestamp)
    parser.add_argument("--scheduled", action="store_true")
    # Older invocations can retain this option while reading through the final page.
    parser.add_argument("--max-pages", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--overlap-hours", type=int, help="Override the scheduled scan overlap for this invocation.")
    args = parser.parse_args()
    settings = config_from_args(parser, args)
    outbound = settings.outbound
    configure_logging(settings.logging, "outbound-import")
    owner = args.owner or outbound.owner
    source_id = args.source or outbound.active_source or outbound.source
    database = args.database if args.database is not None else settings.backend.database
    def invalid_arguments(reason, message):
        event("outbound_import_stopped", level="ERROR", reason=reason)
        parser.error(message)

    if args.scheduled and (args.start is not None or args.end is not None):
        invalid_arguments("outbound_schedule_arguments", "Use --scheduled by itself or pass both --start and --end.")
    if not args.scheduled and (args.start is None or args.end is None):
        invalid_arguments("outbound_schedule_arguments", "Set both --start and --end, or use --scheduled.")
    if not (owner and source_id):
        invalid_arguments("outbound_source_configuration", "Set the outbound owner and source in configuration or arguments.")
    if args.overlap_hours is not None and (not args.scheduled or args.overlap_hours < 0):
        invalid_arguments("outbound_overlap_arguments", "--overlap-hours requires --scheduled and a nonnegative value.")
    try:
        imported, pending = run_import(
            settings, owner=owner, source_id=source_id, database=database,
            manifest=args.manifest, start=args.start, end=args.end,
            scheduled_at=datetime.now(timezone.utc) if args.scheduled else None,
            overlap_seconds=args.overlap_hours * 3600 if args.overlap_hours is not None else None,
        )
    except (OSError, ValueError) as error:
        event("outbound_import_stopped", level="WARNING", reason=type(error).__name__)
        parser.exit(1, f"Import stopped ({type(error).__name__}). Check the provider and time range.\n")
    except Exception as error:
        event("outbound_import_stopped", level="ERROR", reason=type(error).__name__)
        raise
    print(f"Imported {imported} recordings. Failed download attempts this run: {pending}.")
    return int(pending != 0)


if __name__ == "__main__":
    raise SystemExit(main())
