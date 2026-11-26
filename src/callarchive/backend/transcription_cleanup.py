from contextlib import closing

import httpx

from ..observability import event


RETIRE_PATH = "/internal/transcription/archive-groups/retire"
RETIRE_BATCH_SIZE = 100


async def flush_retired_transcriptions(store, transcription):
    """Clear one batch of retired archive groups, keeping unconfirmed work for the next cleanup."""
    with closing(store.connect()) as db:
        group_ids = [row["recording_id"] for row in db.execute("""
            SELECT recording_id FROM retired_transcription_groups ORDER BY rowid LIMIT ?
        """, (RETIRE_BATCH_SIZE,))]

    if not group_ids:
        return 0
    try:
        reply = await transcription.post(RETIRE_PATH, json={"group_ids": group_ids})
    except httpx.RequestError as error:
        event("archive_transcription_retirement_deferred", level="WARNING", reason=type(error).__name__)
        return 0
    if reply.status_code != 204:
        event("archive_transcription_retirement_deferred", level="WARNING", reason=f"http_{reply.status_code}")
        return 0

    with store.transaction() as db:
        db.executemany("DELETE FROM retired_transcription_groups WHERE recording_id=?",
                       ((group_id,) for group_id in group_ids))
    return len(group_ids)
