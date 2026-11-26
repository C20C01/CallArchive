"""Adapter for the Qianke outbound call and recording APIs."""

import hashlib
import io
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import httpx
from pydantic import Field, ValidationError

from ..configuration import Section
from . import CallRecord, CursorExpired, Page, RecordingRef
from .http import OutboundUnavailable


LIST = "/open/api/phonesale/getCallRecordList"
FILES = "/open/api/file/getFileUrl"
BASE_URL = "https://api.saas-work.cn"


def call_identity(timestamp, caller, callee):
    identity = json.dumps([timestamp, caller, callee], separators=(",", ":")).encode()
    return f"{timestamp}-{hashlib.sha256(identity).hexdigest()}"


class QiankeOptions(Section):
    app_id: str = Field(min_length=1)
    company_id: str = Field(min_length=1)
    secret_env: str = Field(default="QIANKE_APP_SECRET", pattern=r"^[A-Z_][A-Z0-9_]*$")
    recording_hosts: tuple[str, ...] = ("qianke-pro.saas-work.cn",)
    max_bytes: int = Field(default=25 * 1024**2, gt=0)


def create_source(resources, name, options):
    try:
        settings = QiankeOptions.model_validate(options)
    except ValidationError:
        raise ValueError("Invalid Qianke adapter options.") from None
    secret = os.getenv(settings.secret_env)
    if not secret:
        raise ValueError("Set the configured Qianke credential.")
    client = resources.enter_context(httpx.Client(base_url=BASE_URL, timeout=20, trust_env=False))
    return QiankeSource(client, settings.app_id, settings.company_id, secret,
                        settings.recording_hosts, max_bytes=settings.max_bytes)


class QiankeSource:
    scan_overlap_seconds = 3 * 3600
    recording_window_timezone = timezone(timedelta(hours=8))

    def __init__(self, client, app_id, company_id, secret, recording_hosts, *, max_bytes=25 * 1024**2):
        self.client = client
        self.credentials = {"appid": app_id, "did": company_id, "secret": secret, "expire": 7200}
        self.recording_hosts = {host.lower() for host in recording_hosts}
        self.max_bytes = max_bytes
        self.token = ""
        self.refresh_at = 0
        self.download_urls = {}

    def access_token(self, *, force=False):
        if force or time.monotonic() >= self.refresh_at:
            response = self.client.get("/cgi-bin/oauth/access_token", params=self.credentials, follow_redirects=False)
            payload = response.raise_for_status().json()
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise ValueError("Unexpected Qianke authentication response.")
            self.token = token
            self.refresh_at = time.monotonic() + self.credentials["expire"] - 60
        return self.token

    def call(self, path, body):
        try:
            for force in (False, True):
                response = self.client.post(path, json=body, params={"access_token": self.access_token(force=force)},
                                            follow_redirects=False)
                if response.status_code != 401 or force:
                    break
            payload = response.raise_for_status().json()
            if not isinstance(payload, dict) or payload.get("result") != 0 or not isinstance(payload.get("data"), dict):
                raise ValueError("Unexpected Qianke response.")
            return payload["data"]
        except (httpx.HTTPError, ValueError):
            raise OutboundUnavailable("Qianke request failed.") from None

    @staticmethod
    def parse_cursor(cursor):
        try:
            value = json.loads(cursor)
            if isinstance(value, dict) and {"lastId", "lastKey"} <= value.keys():
                return value
        except (TypeError, ValueError):
            pass
        raise CursorExpired("Invalid Qianke page cursor.")

    def list_calls(self, start, end, cursor, limit):
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        body = {"filter": {"timeRange": {"stime": start_ms, "etime": end_ms - 1}}, "limit": min(limit, 1000)}
        if cursor is not None:
            body["last"] = self.parse_cursor(cursor)
        data = self.call(LIST, body)
        try:
            rows, last = data["records"], data["last"]
            if not isinstance(rows, list) or (last is not None and (
                    not isinstance(last, dict) or not {"lastId", "lastKey"} <= last.keys())):
                raise ValueError
            calls = [self.record(row) for row in rows if isinstance(row, dict)
                     and row.get("type") == 1 and row.get("status") == 1]
        except (KeyError, OverflowError, TypeError, ValueError):
            raise OutboundUnavailable("Invalid Qianke call page.") from None
        return Page([row for row in calls if start <= row.started_at < end],
                    json.dumps(last, separators=(",", ":"), sort_keys=True) if last is not None else None)

    def record(self, row):
        timestamp = int(row["callTime"])
        caller = str(row.get("caller") or "").strip() or None
        callee = str(row.get("called") or "").strip() or None
        call_id = call_identity(timestamp, caller, callee)
        fingerprint = hashlib.sha256(json.dumps([timestamp, caller or "", callee or ""]).encode()).hexdigest()
        aliases = [f"awaiting-{fingerprint}"]
        talk_id = str(row.get("talkId") or "")
        if talk_id.isascii() and talk_id.isdecimal() and talk_id != "0":
            aliases.append(talk_id)
        duration = row.get("talkLen")
        if isinstance(duration, str) and duration.isascii() and duration.isdecimal():
            duration = int(duration)
        duration = duration if type(duration) is int and duration >= 0 else None
        files = row.get("audioInfos") or []
        if not isinstance(files, list):
            raise ValueError("Invalid Qianke recording list.")
        recordings = []
        for file in files:
            if not isinstance(file, dict) or not file.get("id") or not file.get("name"):
                continue
            reference = RecordingRef(id=str(file["id"]), reference=str(file["name"]))
            recordings.append(reference)
            if isinstance(file.get("url"), str) and self.allowed_url(file["url"]):
                self.download_urls[(reference.id, reference.reference)] = file["url"]
        return CallRecord(external_call_id=call_id, started_at=datetime.fromtimestamp(timestamp / 1000, timezone.utc),
                          caller=caller, callee=callee, connected=True, call_duration_ms=duration,
                          source_aliases=aliases,
                          recordings=recordings, recordings_complete=bool(files) and len(files) == len(recordings))

    def get_recordings(self, call_id):
        stamp, separator, digest = call_id.partition("-")
        if not separator or not stamp.isdecimal() or len(digest) != 64:
            raise OutboundUnavailable("Invalid Qianke call reference.")
        timestamp = int(stamp)
        start = datetime.fromtimestamp((timestamp - 1000) / 1000, timezone.utc)
        end = datetime.fromtimestamp((timestamp + 1000) / 1000, timezone.utc)
        cursor = None
        seen = set()
        incomplete_matches = {}
        while True:
            page = self.list_calls(start, end, cursor, 100)
            match = next((row for row in page.calls if row.external_call_id == call_id), None)
            if match is not None:
                return match.recordings, match.recordings_complete
            for row in page.calls:
                if row.caller and row.callee and row.external_call_id.startswith(stamp + "-"):
                    aliases = (call_identity(timestamp, None, row.callee), call_identity(timestamp, row.caller, None))
                    if call_id in aliases:
                        incomplete_matches[row.external_call_id] = row
            cursor = page.cursor
            if cursor is None:
                if len(incomplete_matches) == 1:
                    match = next(iter(incomplete_matches.values()))
                    return match.recordings, match.recordings_complete
                return [], False
            if cursor in seen:
                raise OutboundUnavailable("Qianke repeated a recording lookup cursor.")
            seen.add(cursor)

    def get_recordings_in_window(self, call_ids, start, end):
        calls = self.get_calls_in_window(call_ids, start, end)
        return {identifier: (row.recordings, row.recordings_complete) for identifier, row in calls.items()}

    def get_calls_in_window(self, call_ids, start, end):
        requested = set(call_ids)
        exact = {}
        aliases = {}
        cursor = None
        seen = set()
        while True:
            page = self.list_calls(start, end, cursor, 100)
            for row in page.calls:
                if row.external_call_id in requested:
                    exact[row.external_call_id] = row
                identifiers = set(row.source_aliases)
                if row.caller and row.callee:
                    stamp = int(row.external_call_id.partition("-")[0])
                    identifiers.update((call_identity(stamp, None, row.callee),
                                        call_identity(stamp, row.caller, None)))
                for alias in identifiers & requested:
                    aliases.setdefault(alias, {})[row.external_call_id] = row
            cursor = page.cursor
            if cursor is None:
                break
            if cursor in seen:
                raise OutboundUnavailable("Qianke repeated a recording lookup cursor.")
            seen.add(cursor)
        matches = {alias: next(iter(rows.values())) for alias, rows in aliases.items() if len(rows) == 1}
        matches.update(exact)
        return matches

    def allowed_url(self, value):
        try:
            url = urlsplit(value)
            return (url.scheme == "https" and url.hostname in self.recording_hosts and url.port in (None, 443)
                    and url.username is None and url.password is None)
        except ValueError:
            return False

    def download(self, url):
        if not isinstance(url, str) or not self.allowed_url(url):
            raise OutboundUnavailable("Qianke recording URL is not permitted.")
        try:
            with self.client.stream("GET", url, follow_redirects=False, auth=None) as response:
                response.raise_for_status()
                buffer = io.BytesIO()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if buffer.tell() + len(chunk) > self.max_bytes:
                        raise ValueError("Source recording exceeds upload limit.")
                    buffer.write(chunk)
                buffer.seek(0)
                return buffer
        except httpx.HTTPError:
            raise OutboundUnavailable("Qianke recording download failed.") from None

    @contextmanager
    def open_recording(self, recording):
        key = (recording.id, recording.reference)
        url = self.download_urls.pop(key, None)
        if url is not None:
            try:
                buffer = self.download(url)
            except OutboundUnavailable:
                pass
            else:
                with buffer:
                    yield buffer
                return
        with self.reopen_recording(recording) as buffer:
            yield buffer

    @contextmanager
    def reopen_recording(self, recording):
        self.download_urls.pop((recording.id, recording.reference), None)
        data = self.call(FILES, {"infos": [{"id": recording.id, "name": recording.reference}]})
        infos = data.get("infos")
        if isinstance(infos, list):
            for entry in infos:
                if isinstance(entry, dict) and str(entry.get("id")) == recording.id:
                    with self.download(entry.get("url")) as buffer:
                        yield buffer
                    return
        raise OutboundUnavailable("Qianke did not return the recording URL.")
