"""API 元数据库（SQLite，WAL）。

只存：原片元数据、密文 blob 路径、信封 DEK（密文）、主密钥 id、状态。
绝不存：明文 DEK、明文原片、主密钥材料。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    image_id        TEXT PRIMARY KEY,
    filename        TEXT,
    content_type    TEXT,
    status          TEXT NOT NULL CHECK (status IN
                        ('receiving','ready','failed')),
    master_key_id   TEXT NOT NULL,
    wrapped_dek     TEXT NOT NULL,           -- 十六进制信封，始终为密文
    blob_path       TEXT NOT NULL,
    plain_size      INTEGER,
    enc_size        INTEGER,
    sha256          TEXT,
    idem_token      TEXT UNIQUE,
    error           TEXT,
    created_at      REAL NOT NULL,
    finalized_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_images_mk ON images(master_key_id);
CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);

CREATE TABLE IF NOT EXISTS rotation_jobs (
    job_id          TEXT PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN
                        ('migrating','destroying','completed','failed')),
    old_key_id      TEXT NOT NULL,
    new_key_id      TEXT NOT NULL,
    total           INTEGER NOT NULL DEFAULT 0,
    migrated        INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    started_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    finished_at     REAL
);

-- 密文存储卷迁移：作业与逐条目状态全部持久化，进程重启后可接续。
CREATE TABLE IF NOT EXISTS migration_jobs (
    job_id          TEXT PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN
                        ('running','completed','failed')),
    target_dir      TEXT NOT NULL,
    total           INTEGER NOT NULL DEFAULT 0,
    migrated        INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    started_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    finished_at     REAL
);
CREATE TABLE IF NOT EXISTS migration_items (
    job_id          TEXT NOT NULL,
    image_id        TEXT NOT NULL,
    source_path     TEXT,              -- 计划时记录的旧卷路径（清理依据）
    target_path     TEXT NOT NULL,     -- 新卷目标路径
    state           TEXT NOT NULL CHECK (state IN
                        ('pending','copying','copied','switched',
                         'done','failed')),
    error           TEXT,
    updated_at      REAL NOT NULL,
    PRIMARY KEY (job_id, image_id)
);
"""

TERMINAL_STATES = ("completed", "failed")


class MetadataDB:
    """每线程独立连接（SQLite 连接不跨线程复用），写操作串行化。"""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._db_path = db_path
        self._write_lock = threading.RLock()
        self._local = threading.local()
        conn = self._connect()
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = self._connect()
            self._local.conn = c
        return c

    # ---- 原片 -----------------------------------------------------------
    def create_image(self, image_id: str, filename: str | None,
                     content_type: str | None, master_key_id: str,
                     wrapped_dek: str, blob_path: str,
                     idem_token: str | None) -> str:
        with self._write_lock:
            if idem_token:
                existing = self.conn.execute(
                    "SELECT image_id FROM images WHERE idem_token=?",
                    (idem_token,)).fetchone()
                if existing:
                    return existing["image_id"]
            now = time.time()
            self.conn.execute(
                "INSERT INTO images (image_id, filename, content_type, status, "
                "master_key_id, wrapped_dek, blob_path, idem_token, created_at) "
                "VALUES (?,?,?, 'receiving', ?,?,?,?,?)",
                (image_id, filename, content_type, master_key_id,
                 wrapped_dek, blob_path, idem_token, now))
            self.conn.commit()
            return image_id

    def get_image(self, image_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM images WHERE image_id=?", (image_id,)).fetchone()
        return dict(row) if row else None

    def get_by_token(self, idem_token: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM images WHERE idem_token=?", (idem_token,)).fetchone()
        return dict(row) if row else None

    def finalize_image(self, image_id: str, plain_size: int, enc_size: int,
                       sha256: str) -> None:
        with self._write_lock:
            self.conn.execute(
                "UPDATE images SET status='ready', plain_size=?, enc_size=?, "
                "sha256=?, finalized_at=?, error=NULL WHERE image_id=?",
                (plain_size, enc_size, sha256, time.time(), image_id))
            self.conn.commit()

    def fail_image(self, image_id: str, error: str) -> None:
        with self._write_lock:
            # 失败行释放幂等令牌，允许客户端用同一令牌重新发起上传
            self.conn.execute(
                "UPDATE images SET status='failed', error=?, finalized_at=?, "
                "idem_token=NULL WHERE image_id=?",
                (error[:500], time.time(), image_id))
            self.conn.commit()

    def list_images(self, limit: int = 100, offset: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT image_id, filename, status, master_key_id, plain_size, "
            "sha256, created_at, finalized_at FROM images "
            "ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return [dict(r) for r in rows]

    def distinct_key_ids(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT master_key_id FROM images").fetchall()
        return [r["master_key_id"] for r in rows]

    def count_under_key(self, key_id: str,
                        only_migratable: bool = False) -> int:
        sql = "SELECT COUNT(*) AS n FROM images WHERE master_key_id=?"
        if only_migratable:
            # 仅统计会阻塞销毁的行：ready（必须 rewrap）与在途 receiving；
            # failed 行永不会被下载，无需有效信封，不阻塞销毁。
            sql += " AND status IN ('ready','receiving')"
        return self.conn.execute(sql, (key_id,)).fetchone()["n"]

    def iter_under_key(self, key_id: str) -> Iterator[dict]:
        rows = self.conn.execute(
            "SELECT * FROM images WHERE master_key_id=? "
            "AND status IN ('ready','receiving') ORDER BY created_at",
            (key_id,)).fetchall()
        for r in rows:
            yield dict(r)

    def rewrap_row(self, image_id: str, expect_old_key: str,
                   new_key_id: str, new_wrapped: str) -> bool:
        """条件更新：仅当该行仍绑定旧 MK 时改包，返回是否实际更新。

        这样迁移与"迟到的在途上传最终化"互不覆盖：后者若已自行改绑新 MK，
        迁移跳过它即可。
        """
        with self._write_lock:
            cur = self.conn.execute(
                "UPDATE images SET master_key_id=?, wrapped_dek=? "
                "WHERE image_id=? AND master_key_id=?",
                (new_key_id, new_wrapped, image_id, expect_old_key))
            self.conn.commit()
            return cur.rowcount > 0

    def mark_stale_receiving_failed(self) -> int:
        """进程重启后，所有 receiving 都是上次崩溃的在途上传，
        不允许晚发布（防止它依赖已销毁的旧 MK 才完成）。"""
        with self._write_lock:
            cur = self.conn.execute(
                "UPDATE images SET status='failed', "
                "error='interrupted by service restart', finalized_at=? "
                "WHERE status='receiving'", (time.time(),))
            self.conn.commit()
            return cur.rowcount

    def list_ready_image_ids(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT image_id FROM images WHERE status='ready' "
            "ORDER BY created_at").fetchall()
        return [r["image_id"] for r in rows]

    def update_image_blob_path(self, image_id: str, expect_path: str,
                               new_path: str) -> bool:
        """条件更新：仅当元数据仍指向旧路径时切换到新卷路径。

        只改 blob_path 一列——绝不触碰 wrapped_dek/master_key_id，
        因此与主密钥轮换的条件 rewrap 互不覆盖，迁移不可能把旧信封写回。
        """
        with self._write_lock:
            cur = self.conn.execute(
                "UPDATE images SET blob_path=? WHERE image_id=? AND blob_path=?",
                (new_path, image_id, expect_path))
            self.conn.commit()
            return cur.rowcount > 0

    # ---- 轮换作业 -------------------------------------------------------
    def active_rotation(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs WHERE state NOT IN ('completed','failed')"
        ).fetchone()
        return dict(row) if row else None

    def create_rotation(self, old_key_id: str, new_key_id: str,
                        total: int) -> str:
        with self._write_lock:
            if self.active_rotation() is not None:
                raise RuntimeError("rotation already in progress")
            job_id = "rot-" + uuid.uuid4().hex
            now = time.time()
            self.conn.execute(
                "INSERT INTO rotation_jobs (job_id, state, old_key_id, "
                "new_key_id, total, migrated, failed, started_at, updated_at) "
                "VALUES (?, 'migrating', ?,?,?, 0, 0, ?, ?)",
                (job_id, old_key_id, new_key_id, total, now, now))
            self.conn.commit()
            return job_id

    def has_rotation_for(self, old_key_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM rotation_jobs WHERE old_key_id=? LIMIT 1",
            (old_key_id,)).fetchone() is not None

    def get_rotation(self, job_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def latest_rotation(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def bump_rotation(self, job_id: str, migrated: int = 0, failed: int = 0,
                      state: str | None = None, error: str | None = None) -> None:
        with self._write_lock:
            sets = ["updated_at=?"]
            params: list[Any] = [time.time()]
            if migrated:
                sets.append("migrated = migrated + ?")
                params.append(migrated)
            if failed:
                sets.append("failed = failed + ?")
                params.append(failed)
            if state:
                sets.append("state=?")
                params.append(state)
            if error is not None:
                sets.append("error=?")
                params.append(error[:500])
            if state in TERMINAL_STATES:
                sets.append("finished_at=?")
                params.append(time.time())
            params.append(job_id)
            self.conn.execute(
                f"UPDATE rotation_jobs SET {', '.join(sets)} WHERE job_id=?",
                params)
            self.conn.commit()

    # ---- 卷迁移作业 -------------------------------------------------------
    def active_migration(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM migration_jobs "
            "WHERE state NOT IN ('completed','failed')").fetchone()
        return dict(row) if row else None

    def create_migration(self, target_dir: str,
                         items: list[tuple[str, str]]) -> str:
        """items 为 (image_id, target_path) 列表；同一时刻只允许一个作业。"""
        with self._write_lock:
            if self.active_migration() is not None:
                raise RuntimeError("migration already in progress")
            job_id = "mig-" + uuid.uuid4().hex
            now = time.time()
            self.conn.execute(
                "INSERT INTO migration_jobs (job_id, state, target_dir, total, "
                "migrated, failed, started_at, updated_at) "
                "VALUES (?, 'running', ?, ?, 0, 0, ?, ?)",
                (job_id, target_dir, len(items), now, now))
            self.conn.executemany(
                "INSERT INTO migration_items (job_id, image_id, source_path, "
                "target_path, state, updated_at) "
                "VALUES (?,?,NULL,?,'pending',?)",
                [(job_id, image_id, target_path, now)
                 for image_id, target_path in items])
            self.conn.commit()
            return job_id

    def get_migration(self, job_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM migration_jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def latest_migration(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM migration_jobs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def bump_migration(self, job_id: str, migrated: int = 0, failed: int = 0,
                       state: str | None = None, error: str | None = None) -> None:
        with self._write_lock:
            sets = ["updated_at=?"]
            params: list[Any] = [time.time()]
            if migrated:
                sets.append("migrated = migrated + ?")
                params.append(migrated)
            if failed:
                sets.append("failed = failed + ?")
                params.append(failed)
            if state:
                sets.append("state=?")
                params.append(state)
            if error is not None:
                # 空串用于清除上一条错误（如清理重试最终成功）
                sets.append("error=?")
                params.append(error[:500] if error else None)
            if state in TERMINAL_STATES:
                sets.append("finished_at=?")
                params.append(time.time())
                if state == "completed" and error is None:
                    # 完成即意味着新卷全部可取数且旧件已清除，不残留旧错误
                    sets.append("error=NULL")
            params.append(job_id)
            self.conn.execute(
                f"UPDATE migration_jobs SET {', '.join(sets)} WHERE job_id=?",
                params)
            self.conn.commit()

    # ---- 迁移条目 ---------------------------------------------------------
    def get_migration_item(self, job_id: str, image_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM migration_items WHERE job_id=? AND image_id=?",
            (job_id, image_id)).fetchone()
        return dict(row) if row else None

    def migration_open_items(self, job_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM migration_items WHERE job_id=? "
            "AND state NOT IN ('done','failed') ORDER BY rowid",
            (job_id,)).fetchall()
        return [dict(r) for r in rows]

    def migration_failed_items(self, job_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM migration_items WHERE job_id=? AND state='failed' "
            "ORDER BY rowid", (job_id,)).fetchall()
        return [dict(r) for r in rows]

    def set_migration_item(self, job_id: str, image_id: str,
                           state: str | None = None,
                           source_path: str | None = None,
                           error: str | None = None) -> None:
        with self._write_lock:
            sets = ["updated_at=?"]
            params: list[Any] = [time.time()]
            if state:
                sets.append("state=?")
                params.append(state)
            if source_path is not None:
                sets.append("source_path=?")
                params.append(source_path)
            if error is not None:
                sets.append("error=?")
                params.append(error[:500] if error else None)
            params.extend([job_id, image_id])
            self.conn.execute(
                f"UPDATE migration_items SET {', '.join(sets)} "
                "WHERE job_id=? AND image_id=?", params)
            self.conn.commit()

    def close(self):
        c = getattr(self._local, "conn", None)
        if c:
            c.close()
