import logging
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


class DB:
    def __init__(
        self,
        expired_seconds: int,
        *,
        log_path: str = "./",
    ) -> None:
        super().__init__()
        self.db_path = Path(log_path) / "pyxxl.db"
        self.dblock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.expired_seconds = expired_seconds

        # Create logs table if not exists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id TEXT NOT NULL,
                level TEXT NOT NULL,
                record TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
        """)
        self.conn.commit()

    def record(self, log_id: str, level: str, ms: int, message: str, *, expired_seconds: Optional[int] = None) -> None:
        expired_seconds = expired_seconds or self.expired_seconds
        try:
            with self.dblock:
                self.cursor.execute(
                    """
                    INSERT INTO logs (log_id, level, record, created_at_ms, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (log_id, level, message, ms, int(time.time() + expired_seconds)),
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
                ORDER BY created_at_ms, id ASC
                LIMIT ?, ?
            """,
                (log_id, from_line_num - 1, limit),
            )
            return total, [i[0] for i in self.cursor.fetchall()]

    def delete_expired(self) -> None:
        now_ts = round(time.time())
        with self.dblock:
            self.cursor.execute(
                """
                DELETE FROM logs
                WHERE expires_at <= ?
            """,
                (now_ts,),
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

    def __init__(self, db: DB, expired_seconds: Optional[int] = None) -> None:
        super().__init__()
        self.db = db
        self.expired_seconds = expired_seconds

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to SQLite database."""
        xxl_kwargs = g.try_get_run_data()
        log_id = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
        record.logId = log_id
        ms = round(record.created * 1000)
        log_message = self.format(record) + self.terminator
        self.db.record(str(log_id), record.levelname, ms, log_message, expired_seconds=self.expired_seconds)


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
        self._db = DB(expired_seconds=self.expired_seconds or self.expired_seconds, log_path=self.log_path)

    def get_logger(
        self,
        log_id: int,
        *,
        stdout: bool = True,
        level: int = logging.INFO,
        expired_seconds: Optional[int] = None,
    ) -> logging.Logger:
        logger = logging.getLogger("pyxxl.task_log.sqlite.task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[logging.Handler] = [PyxxlStreamHandler()] if stdout else []
        handlers.append(SQLiteHandler(self._db, expired_seconds))
        for h in handlers:
            h.setFormatter(TASK_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    async def get_logs(self, request: LogRequest) -> LogResponse:
        """Retrieve logs from the SQLite database."""
        log_id = request["logId"]
        from_line_num = request["fromLineNum"]
        limit = self.log_tail_lines
        total, records = self._db.query(str(log_id), from_line_num, limit)
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

    async def read_task_logs(self, log_id: int) -> Optional[str]:
        total, logs = self._db.query(str(log_id), 0, 100000)
        if total == 0:
            return None

        return "".join(logs)

    async def expired_once(self, **kwargs: Any) -> bool:
        """Delete expired logs from the database."""
        self._db.delete_expired()
        return True
