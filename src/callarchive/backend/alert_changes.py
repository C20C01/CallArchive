"""Track committed changes that can affect single notifications."""

from contextlib import closing

from .call_state import DOWNLOAD_ATTEMPT_LIMIT, TRANSCRIPTION_ATTEMPT_LIMIT


class AlertChanges:
    def __init__(self, store):
        self.store = store
        with closing(store.connect()) as db:
            db.executescript(f"""
                CREATE TABLE IF NOT EXISTS alert_revision (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    revision INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO alert_revision VALUES (1,1);
                CREATE TABLE IF NOT EXISTS alert_progress (
                    scope TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS alert_status_changed AFTER UPDATE OF status ON recordings
                WHEN NEW.status IS NOT OLD.status AND (
                    NEW.status IN ('needs_input','needs_review') OR OLD.status IN ('needs_input','needs_review')
                    OR EXISTS (SELECT 1 FROM workflow WHERE recording_id=NEW.id AND transcription_failures>={TRANSCRIPTION_ATTEMPT_LIMIT}))
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS alert_workflow_changed
                AFTER UPDATE OF error, transcription_failures ON workflow
                WHEN (OLD.error IS NOT NEW.error OR OLD.transcription_failures IS NOT NEW.transcription_failures)
                  AND (OLD.transcription_failures>={TRANSCRIPTION_ATTEMPT_LIMIT}
                       OR NEW.transcription_failures>={TRANSCRIPTION_ATTEMPT_LIMIT} OR EXISTS (
                      SELECT 1 FROM recordings WHERE id=NEW.recording_id AND status IN ('needs_input','needs_review')))
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
                DROP TRIGGER IF EXISTS alert_download_changed;
                CREATE TRIGGER alert_download_changed
                AFTER UPDATE OF error, download_attempts, recording_id ON outbound_recordings
                WHEN (OLD.recording_id IS NULL AND
                      (OLD.error IS 'invalid_audio' OR (OLD.error IS NOT NULL AND OLD.download_attempts>={DOWNLOAD_ATTEMPT_LIMIT})))
                   IS NOT (NEW.recording_id IS NULL AND
                      (NEW.error IS 'invalid_audio' OR (NEW.error IS NOT NULL AND NEW.download_attempts>={DOWNLOAD_ATTEMPT_LIMIT})))
                  OR (OLD.error IS NOT NEW.error AND
                      (OLD.download_attempts>={DOWNLOAD_ATTEMPT_LIMIT} OR NEW.download_attempts>={DOWNLOAD_ATTEMPT_LIMIT}))
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS alert_call_changed AFTER UPDATE OF metadata ON outbound_calls
                WHEN OLD.metadata IS NOT NEW.metadata
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS alert_call_removed AFTER INSERT ON console_call_removals
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS alert_call_short AFTER INSERT ON outbound_short_calls
                BEGIN UPDATE alert_revision SET revision=revision+1 WHERE id=1; END;
            """)

    def revision(self):
        with closing(self.store.connect()) as db:
            return db.execute("SELECT revision FROM alert_revision WHERE id=1").fetchone()[0]

    def inspect(self, scope, retry_slot):
        with closing(self.store.connect()) as db:
            row = db.execute("""
                SELECT a.revision, p.revision AS checked, t.status, t.slot
                FROM alert_revision a LEFT JOIN alert_progress p ON p.scope=?
                LEFT JOIN recurring_tasks t ON t.task='alerts' AND t.scope=?
                WHERE a.id=1
            """, (scope, scope)).fetchone()
        failed = row["status"] == "failed"
        due = (row["revision"] != row["checked"] or row["status"] == "running"
               or (failed and row["slot"] < retry_slot))
        return row["revision"], due, failed

    def checkpoint(self, scope, revision, write_guard):
        with self.store.transaction() as db:
            write_guard(db)
            db.execute("""
                INSERT INTO alert_progress VALUES (?,?)
                ON CONFLICT(scope) DO UPDATE SET revision=max(revision,excluded.revision)
            """, (scope, revision))
