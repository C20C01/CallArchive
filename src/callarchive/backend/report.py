import argparse
from contextlib import ExitStack
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from ..configuration import config_from_args
from ..observability import configure_logging
from ..notifications.feishu import FeishuChannel
from ..notifications.preview import PreviewChannel
from .providers import driver_factory
from .reporting import ReportDelivery
from .scheduling import report_day
from .store import ARCHIVE_CACHE_SECONDS, ArchiveStore


def configured_channel(resources, settings, name, output, page_size):
    if name == "preview":
        return PreviewChannel(output)
    if name == "feishu":
        webhook = os.getenv("CALLARCHIVE_FEISHU_WEBHOOK")
        if not webhook:
            raise ValueError("Set CALLARCHIVE_FEISHU_WEBHOOK for the Feishu channel.")
        if page_size > 50:
            raise ValueError("The Feishu channel supports at most 50 entries per page.")
        client = resources.enter_context(httpx.Client(timeout=10, trust_env=False))
        return FeishuChannel(client, webhook, secret=os.getenv("CALLARCHIVE_FEISHU_SECRET"))
    provider = settings.channels.get(name)
    if provider is None:
        raise ValueError("Notification channel is not configured.")
    channel = driver_factory(provider.driver)(resources, name, provider.options)
    if not callable(getattr(channel, "send", None)) or not isinstance(
        getattr(channel, "supports_idempotency", None), bool
    ):
        raise ValueError("Notification adapter must implement send and declare idempotency support.")
    return channel


def run_report(settings, mode, *, now=None, day=None, owner=None, destination=None,
               database=None, output=None, page_size=None, channel_name=None, replay_rejected=False, write_guard=None):
    notifications = settings.notifications
    owner = owner or notifications.owner
    destination = destination or notifications.destination
    database = settings.backend.database if database is None else database
    output = notifications.output if output is None else output
    page_size = notifications.page_size if page_size is None else page_size
    try:
        zone = ZoneInfo(notifications.timezone)
    except ZoneInfoNotFoundError:
        raise ValueError("Set notifications.timezone to a valid IANA time zone.") from None
    if mode in {"scheduled", "retry"}:
        now = datetime.now(timezone.utc) if now is None else now
        day = report_day(now, zone, notifications.daily_at)
        if day is None:
            return {}
    if not owner:
        raise ValueError("Set a report owner in configuration or arguments.")
    if page_size < 1:
        raise ValueError("Page size must be positive.")
    url = str(notifications.workbench_url) if notifications.workbench_url else None
    store = ArchiveStore(database, write_guard=write_guard)
    reports = ReportDelivery(store, page_size=page_size, workbench_url=url,
                             followup_names=settings.backend.followup_names, zone=zone)
    channel_name = channel_name or notifications.channel
    channel_id = notifications.channel_id if notifications.channel == channel_name else None
    channel_id = channel_id or channel_name
    def deliver(channel):
        if mode == "alerts":
            return reports.deliver_alerts(owner, channel_id, destination, channel,
                                          replay_rejected=replay_rejected)
        if mode == "retry":
            return reports.retry_report(day, owner, channel_id, destination, channel)
        # An explicit preview date uses that day's configured report time.
        end = now if mode == "scheduled" else datetime.combine(day, notifications.daily_at, zone)
        end_stamp = end.timestamp()
        window = end_stamp - ARCHIVE_CACHE_SECONDS, end_stamp
        return reports.deliver(day, owner, channel_id, destination, channel, archive_window=window,
                               replay_rejected=replay_rejected, fresh_only=mode == "scheduled")

    with ExitStack() as resources:
        channel = configured_channel(resources, notifications, channel_name, output, page_size)
        return deliver(channel)


def main():
    parser = argparse.ArgumentParser(description="Create or replay a daily report preview.")
    parser.add_argument("--config", type=Path)
    period = parser.add_mutually_exclusive_group(required=True)
    period.add_argument("--day", type=date.fromisoformat)
    period.add_argument("--scheduled", action="store_true", help="Send today when the daily time is due.")
    period.add_argument("--retry-scheduled", action="store_true",
                        help="Retry only today's saved report pages explicitly rejected by a transient error.")
    period.add_argument("--alerts", action="store_true", help="Deliver single alerts for active blocked recordings.")
    parser.add_argument("--replay-rejected", action="store_true",
                        help="Explicitly replay previously rejected messages with --day or --alerts.")
    parser.add_argument("--owner")
    parser.add_argument("--destination")
    parser.add_argument("--channel", help="Select a built-in or configured notification channel.")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--page-size", type=int)
    args = parser.parse_args()
    if args.replay_rejected and not (args.day or args.alerts):
        parser.error("--replay-rejected requires --day or --alerts.")
    settings = config_from_args(parser, args)
    configure_logging(settings.logging, "notification")
    if args.alerts:
        mode = "alerts"
    elif args.retry_scheduled:
        mode = "retry"
    else:
        mode = "scheduled" if args.scheduled else "day"
    try:
        result = run_report(
            settings, mode, day=args.day, owner=args.owner, destination=args.destination,
            database=args.database, output=args.output, page_size=args.page_size,
            channel_name=args.channel, replay_rejected=args.replay_rejected,
        )
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result))
    return int(bool(set(result) - {"confirmed"}))


if __name__ == "__main__":
    raise SystemExit(main())
