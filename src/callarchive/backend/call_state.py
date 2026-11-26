"""Current call and recording facts used by the console and notifications."""


DOWNLOAD_ATTEMPT_LIMIT = 4
TRANSCRIPTION_ATTEMPT_LIMIT = 4

RETRYABLE_ERRORS = frozenset({
    "attachment_write_failed", "attachment_conflict", "invalid_workflow_response",
    "transcription_failed", "transcription_http_401", "transcription_http_403",
    "archive_instance_unavailable", "archive_target_missing", "archive_target_conflict", "archive_routing_changed",
    "archive_attachment_unknown", "archive_attachment_rejected", "archive_lookup_unavailable",
    "archive_response_invalid",
})


def recording_retry(status, error, resume_status, retry_at, lease_until, now):
    if (lease_until or 0) > now:
        return None
    if status == "queued" and error == "transcription_failed" and (retry_at or 0) > now:
        return "expedite_transcription"
    if status in {"needs_review", "needs_input"} and resume_status and error in RETRYABLE_ERRORS:
        return "resume"
    return None


CALL_STATE = f"""
WITH recording_facts AS (
    SELECT r.rowid AS position, r.id, r.owner, coalesce(r.call_id,r.id) AS call_key,
           r.metadata, r.status, m.duration_ms, w.lease_until,
           recording_retry(r.status,w.error,recovery.resume_status,w.retry_at,w.lease_until,:now)
               IS NOT NULL AS retryable,
           CASE WHEN r.status='needs_input' THEN 'follow_up'
                WHEN r.status='needs_review' AND NOT
                     (coalesce(w.error,'')='transcription_failed' AND coalesce(w.transcription_failures,0)<{TRANSCRIPTION_ATTEMPT_LIMIT})
                     THEN 'admin'
                WHEN r.status IN ('queued','transcribing','acknowledging')
                     AND w.transcription_failures>={TRANSCRIPTION_ATTEMPT_LIMIT} THEN 'admin' END AS category,
           coalesce(w.error, CASE WHEN r.status IN ('queued','transcribing','acknowledging')
                                  THEN 'transcription_unavailable' ELSE r.status END) AS reason
    FROM recordings r
    LEFT JOIN workflow w ON w.recording_id=r.id
    LEFT JOIN recording_recovery recovery ON recovery.recording_id=r.id
    LEFT JOIN recording_media m ON m.recording_id=r.id
    WHERE r.status<>'removed'
), segments AS (
    SELECT call_key, min(metadata) AS metadata, count(*) AS recording_count,
           count(*) FILTER (WHERE status='archived') AS archived_count,
           CASE WHEN count(*)=count(duration_ms) THEN sum(duration_ms) END AS duration,
           max(category='admin') AS attention, max(category='follow_up') AS needs_input,
           max(status<>'archived' AND coalesce(lease_until,0)>:now) AS busy,
           max(retryable) AS retryable
    FROM recording_facts GROUP BY call_key
), source_facts AS (
    SELECT rowid AS position, call_id AS call_key, metadata,
           CASE WHEN error='invalid_audio' THEN 'source_recording_invalid'
                WHEN error IS NOT NULL AND download_attempts>={DOWNLOAD_ATTEMPT_LIMIT} THEN 'source_recording_unavailable'
           END AS reason
    FROM outbound_recordings WHERE recording_id IS NULL
), staged AS (
    SELECT call_key, count(*) AS pending, max(reason='source_recording_invalid') AS invalid,
           max(reason='source_recording_unavailable') AS failed
    FROM source_facts GROUP BY call_key
), entries AS (
    SELECT c.id AS call_key, c.owner, o.metadata AS outbound_metadata, o.complete
    FROM calls c LEFT JOIN outbound_calls o ON o.call_id=c.id
    UNION ALL
    SELECT id, owner, NULL, NULL FROM recordings WHERE call_id IS NULL
), summaries AS (
    SELECT e.call_key, e.owner, coalesce(e.outbound_metadata,json_extract(s.metadata,'$.call'),'{{}}') AS call,
           e.outbound_metadata IS NOT NULL AS outbound, e.complete=0 AS waiting_recording,
           coalesce(o.pending,0) AS pending, coalesce(s.recording_count,0) AS loaded,
           coalesce(s.archived_count,0) AS archived_count, coalesce(s.busy,0) AS busy,
           coalesce(s.retryable,0) AS retryable,
           CASE WHEN coalesce(o.pending,0)=0 THEN s.duration END AS downloaded_duration,
           CASE WHEN coalesce(o.pending,0)=0 AND coalesce(e.complete,1)=1 THEN s.duration END AS duration,
           CASE WHEN o.invalid THEN 'recording_invalid'
                WHEN s.attention OR o.failed THEN 'attention'
                WHEN s.needs_input THEN 'waiting_applicant'
                WHEN o.pending THEN 'waiting_download'
                WHEN e.complete=0 THEN 'waiting_recording'
                ELSE 'processing' END AS status
    FROM entries e LEFT JOIN segments s ON s.call_key=e.call_key
    LEFT JOIN staged o ON o.call_key=e.call_key
    LEFT JOIN console_call_removals removed ON removed.call_key=e.call_key
    WHERE removed.call_key IS NULL AND e.call_key NOT IN (SELECT call_id FROM outbound_short_calls)
      AND (s.recording_count>s.archived_count OR o.pending>0 OR e.complete=0)
)
"""
