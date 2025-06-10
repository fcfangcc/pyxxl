import logging
import sqlite3
import threading
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pyxxl.ctx import g
from pyxxl.log import executor_logger
from pyxxl.logger.common import TASK_FORMATTER, LogBase, PyxxlStreamHandler
from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import try_import

if TYPE_CHECKING:
    import sqlite3
else:
    sqlite3 = try_import("sqlite3")


def _dbpath(dbname: str, log_path: str) -> Path:
    return Path(log_path) / f"pyxxl-{dbname}.db"


class DB:
    def __init__(self, dbname: str, log_path: str = "./") -> None:
        super().__init__()
        self.db_path = _dbpath(dbname, log_path)
        self.dblock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Create logs table if not exists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id TEXT NOT NULL,
                level TEXT NOT NULL,
                ms_timestamp INTEGER NOT NULL,
                record TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def record(self, log_id: str, level: str, ms: int, message: str) -> None:
        try:
            with self.dblock:
                self.cursor.execute(
                    """
                    INSERT INTO logs (log_id, level, ms_timestamp, record)
                    VALUES (?, ?, ?, ?)
                """,
                    (log_id, level, ms, message),
                )
                self.conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            traceback.print_exc()

    def query(self, log_id: str, from_line_num: int, limit: int = 1000) -> tuple[int, list[str]]:
        """Query logs for a specific log_id starting from a line number."""
        with self.dblock:
            # total
            self.cursor.execute(
                """
                SELECT COUNT(*) FROM logs
                WHERE log_id = ?
            """,
                (log_id,),
            )
            total = self.cursor.fetchone()[0]

            self.cursor.execute(
                """
                SELECT record FROM logs
                WHERE log_id = ?
                ORDER BY ms_timestamp, id ASC
                LIMIT ?, ?
            """,
                (log_id, from_line_num - 1, limit),
            )
            return total, [i[0] for i in self.cursor.fetchall()]

    def delete_expired(self, last_ms: int) -> None:
        with self.dblock:
            self.cursor.execute(
                """
                DELETE FROM logs
                WHERE ms_timestamp <= ?
            """,
                (last_ms,),
            )
            self.conn.commit()

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()

    def clear(self) -> None:
        with self.dblock:
            self.cursor.execute("DELETE FROM logs")
            self.conn.commit()


class SQLiteHandler(logging.Handler):
    terminator = "\n"

    def __init__(self, db: DB) -> None:
        super().__init__()
        self.db = db

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to SQLite database."""
        xxl_kwargs = g.try_get_run_data()
        log_id = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
        record.logId = log_id
        ms = round(record.created * 1000)
        log_message = self.format(record) + self.terminator
        self.db.record(str(log_id), record.levelname, ms, log_message)


class SQLiteLog(LogBase):
    """Log handler for SQLite database."""

    def __init__(
        self,
        log_path: str = "./",
        logger: Optional[logging.Logger] = None,
        log_tail_lines: int = 10,
        expired_days: float = 14,
    ) -> None:
        if sqlite3 is None:
            raise ImportError("SQLite3 is not available. Please install it or use a different logging backend.")
        self.executor_logger = logger or executor_logger
        self.log_path = log_path
        self.log_tail_lines = log_tail_lines
        self.expired_seconds = round(expired_days * 3600 * 24)
        self._db_map: dict[str, DB] = dict()

    def _get_db(self, dbname: str) -> DB:
        # todo: clean dead db instances
        if dbname not in self._db_map:
            db = DB(dbname, self.log_path)
            self._db_map[dbname] = db
        return self._db_map[dbname]

    def get_logger(
        self, job_id: int, log_id: int, *, stdout: bool = True, level: int = logging.INFO
    ) -> logging.Logger:
        logger = logging.getLogger("pyxxl.task_log.sqlite.task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[logging.Handler] = [PyxxlStreamHandler()] if stdout else []
        handlers.append(SQLiteHandler(self._get_db(str(job_id))))
        for h in handlers:
            h.setFormatter(TASK_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    async def get_logs(self, request: LogRequest) -> LogResponse:
        """Retrieve logs from the SQLite database."""
        log_id = request["logId"]
        job_id = request["jobId"]
        from_line_num = request["fromLineNum"]
        limit = self.log_tail_lines

        db = self._get_db(str(job_id))
        total, records = db.query(str(log_id), from_line_num, limit)
        if total and len(records) == 0:
            to_line_num = from_line_num
            log_content = ""
        elif total and records:
            to_line_num = from_line_num + len(records) - 1
            log_content = "".join(records)
        else:
            log_content = self.NOT_FOUND_LOGS
            to_line_num = from_line_num

        return LogResponse(
            fromLineNum=from_line_num,
            toLineNum=to_line_num,
            logContent=log_content,
            isEnd=to_line_num >= total,
        )

    async def read_task_logs(self, job_id: int, log_id: int) -> str | None:
        db = self._get_db(str(job_id))
        total, logs = db.query(str(log_id), 0, 100000)
        if total == 0:
            return None

        return "".join(logs)

    async def expired_once(self, *, expired_seconds: None | int = None, **kwargs: Any) -> bool:
        """Delete expired logs from the database."""
        if expired_seconds is None:
            expired_seconds = self.expired_seconds
        last_ms = round((time.time() - expired_seconds) * 1000)
        for db in self._db_map.values():
            db.delete_expired(last_ms)

        return True
