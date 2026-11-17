"""密文存储卷迁移后台工作线程。

把已发布（ready）原片的密文 blob 从旧卷搬到 Compose 挂载的新卷：
  copying  ->  流式复制 source -> 目标临时文件，逐字节核对一致后原子 rename；
               复制失败则条目判失败，元数据不动，旧卷继续供数
  copied   ->  条件 UPDATE 切换元数据 blob_path（只改路径列，绝不回写
               wrapped_dek/master_key_id，与主密钥轮换的 rewrap 互不干扰）
  switched ->  等该原片在途下载归零后删除旧卷文件；清理失败持续重试，
               只有新卷可取数且旧件已清除，条目才算 done
  done / failed

保证：
  * 全程不解密、不改写原片正文与信封（复制的是密文，信封列从不经手）；
  * 已开始及新发起的下载都完整：切换前读到旧路径的下载被 DownloadTracker
    等到结束才删旧文件，切换后发起的下载从新路径取数；
  * 作业与条目状态持久化在 SQLite，进程重启后自动接续到终态；
  * 同号重试：元数据已指向新卷则跳过复制，只清理有记录的旧件；
    清理的不变式是"绝不删除元数据当前指向的有效副本"。
"""
from __future__ import annotations

import asyncio
import os
import threading
import traceback

from .metadata import MetadataDB

POLL_INTERVAL = 1.0
COPY_ATTEMPTS = 3               # 单条目复制重试预算（失败则旧卷继续供数）
COPY_CHUNK = 1024 * 1024
ITEM_PAUSE = 0.02               # 条目间让出，避免挤占在线流量


class MigrationError(Exception):
    """迁移执行错误（复制/校验失败等）。"""


class DownloadTracker:
    """按原片编号统计在途下载；迁移清理旧文件前要求其计数归零。

    使用时序约定：下载端点在读取元数据之前 begin()，响应流结束后 end()；
    迁移在"切换元数据"之后才清理旧文件。于是：
      * 切换前读到旧路径的下载必然已被计数，active() 非零，清理等待；
      * 切换后 begin 的下载读到的是新路径，与旧文件无关。
    因此"active()==0 才删旧文件"不存在删除竞争。
    """

    def __init__(self):
        self._cond = threading.Condition()
        self._counts: dict[str, int] = {}

    def begin(self, image_id: str) -> None:
        with self._cond:
            self._counts[image_id] = self._counts.get(image_id, 0) + 1

    def end(self, image_id: str) -> None:
        with self._cond:
            remaining = self._counts.get(image_id, 0) - 1
            if remaining > 0:
                self._counts[image_id] = remaining
            else:
                self._counts.pop(image_id, None)
            self._cond.notify_all()

    def active(self, image_id: str) -> int:
        """当前在途下载数（非阻塞；为零才可删除其旧卷文件）。"""
        with self._cond:
            return self._counts.get(image_id, 0)


class MigrationWorker:
    def __init__(self, db: MetadataDB, tracker: DownloadTracker,
                 loop: asyncio.AbstractEventLoop):
        self._db = db
        self._tracker = tracker
        self._loop = loop
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._thread_main, daemon=True,
                                        name="migration-worker")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _thread_main(self):
        try:
            fut = asyncio.run_coroutine_threadsafe(self._run(), self._loop)
            fut.result()
        except Exception:
            traceback.print_exc()

    async def _run(self):
        """常驻协程：发现未完成迁移作业就驱动到终态，然后继续轮询。"""
        while not self._stop.is_set():
            job = self._db.active_migration()
            if job is not None:
                await self._drive(job["job_id"])
            else:
                await asyncio.sleep(POLL_INTERVAL)

    async def _drive(self, job_id: str):
        while not self._stop.is_set():
            job = self._db.get_migration(job_id)
            if job is None or job["state"] in ("completed", "failed"):
                return
            items = self._db.migration_open_items(job_id)
            if not items:
                failed = self._db.migration_failed_items(job_id)
                if failed:
                    first = failed[0]
                    self._db.bump_migration(
                        job_id, state="failed",
                        error=f"{len(failed)} item(s) failed; first: "
                              f"{first['image_id']}: {first['error']}")
                else:
                    # 只有全部条目 done（新卷可取数且旧件已清除）才报完成
                    self._db.bump_migration(job_id, state="completed", error="")
                return
            made_progress = False
            for item in items:
                if self._stop.is_set():
                    return
                try:
                    # 单件暂时推进不了（如在途下载未归零、清理重试）不阻塞其他件
                    made_progress |= await self._process_item(job, item)
                except Exception as exc:  # 防御：任何意外都落条目 failed，不静默
                    traceback.print_exc()
                    self._db.set_migration_item(
                        job_id, item["image_id"], state="failed",
                        error=f"unexpected: {exc!r}")
                    self._db.bump_migration(job_id, failed=1)
                    made_progress = True
                await asyncio.sleep(ITEM_PAUSE)
            if not made_progress:
                await asyncio.sleep(POLL_INTERVAL)

    # ------------------------------------------------------------------
    # 条目状态机
    # ------------------------------------------------------------------
    async def _process_item(self, job: dict, item: dict) -> bool:
        """推进一个条目；返回本轮是否有进展（False = 需要等待外部条件）。"""
        job_id = job["job_id"]
        state = item["state"]
        # 从 DB 里捞起来就是 copied：说明上次崩溃在"复制完成"之后，
        # 切换前必须重新逐字节核对，不信任停机窗口
        resumed_copied = (state == "copied")
        if state in ("pending", "copying"):
            if not await self._copy_phase(job, item):
                return True  # 条目已终态（failed）或收到停止信号
            item = self._db.get_migration_item(job_id, item["image_id"])
            if item is None:
                return True
            state = item["state"]
        if state == "copied":
            await self._switch_phase(job, item, verify=resumed_copied)
            item = self._db.get_migration_item(job_id, item["image_id"])
            if item is None:
                return True
            state = item["state"]
        if state == "switched":
            return await self._cleanup_phase(job, item)
        return True

    async def _copy_phase(self, job: dict, item: dict) -> bool:
        """复制并核对。返回 False 表示条目已终态或收到停止信号。"""
        job_id, image_id = job["job_id"], item["image_id"]
        target = item["target_path"]
        meta = self._db.get_image(image_id)
        if meta is None:
            self._fail_item(job_id, image_id, "image metadata not found")
            return False
        if meta["status"] != "ready":
            self._fail_item(job_id, image_id,
                            f"image is {meta['status']}; only ready migrates")
            return False
        if meta["blob_path"] == target:
            # 已在新卷（同号重试/崩溃接续）：不复制；有记录的旧件才需清理
            src = item["source_path"]
            if src and src != target and os.path.exists(src):
                self._db.set_migration_item(job_id, image_id, state="switched")
            else:
                self._db.set_migration_item(job_id, image_id, state="done",
                                            error="")
                self._db.bump_migration(job_id, migrated=1)
            return True
        source = meta["blob_path"]
        if item["source_path"] != source:
            self._db.set_migration_item(job_id, image_id, source_path=source)
        if not os.path.exists(source):
            self._fail_item(job_id, image_id, f"source blob missing: {source}")
            return False
        self._db.set_migration_item(job_id, image_id, state="copying")
        last_exc: Exception | None = None
        for attempt in range(1, COPY_ATTEMPTS + 1):
            if self._stop.is_set():
                return False
            try:
                await self._copy_verified(source, target, job_id)
                last_exc = None
                break
            except Exception as exc:  # 复制失败：元数据不动，旧卷继续供数
                last_exc = exc
                await asyncio.sleep(min(0.5 * attempt, 2.0))
        if last_exc is not None:
            self._fail_item(job_id, image_id,
                            f"copy failed after {COPY_ATTEMPTS} attempts: "
                            f"{last_exc!r}")
            return False
        self._db.set_migration_item(job_id, image_id, state="copied", error="")
        return True

    async def _switch_phase(self, job: dict, item: dict, verify: bool):
        job_id, image_id = job["job_id"], item["image_id"]
        source, target = item["source_path"], item["target_path"]
        meta = self._db.get_image(image_id)
        already = bool(meta and meta["blob_path"] == target)
        if verify and not already:
            if not await self._files_equal(source, target):
                # 目标不可信且不是当前有效副本：作废后重新复制
                await self._remove_quiet(target)
                self._db.set_migration_item(job_id, image_id, state="copying")
                return
        if not already:
            changed = self._db.update_image_blob_path(image_id, source, target)
            if not changed:
                meta = self._db.get_image(image_id)
                if meta and meta["blob_path"] == target:
                    pass  # 上次崩溃在状态落库前，切换其实已生效
                else:
                    now = meta["blob_path"] if meta else "missing"
                    self._fail_item(job_id, image_id,
                                    f"blob_path changed concurrently: {now}")
                    return
        self._db.set_migration_item(job_id, image_id, state="switched")

    async def _cleanup_phase(self, job: dict, item: dict) -> bool:
        """删除旧卷文件。返回 False 表示需等待（在途下载/删除失败）。"""
        job_id, image_id = job["job_id"], item["image_id"]
        source = item["source_path"]
        meta = self._db.get_image(image_id)
        current = meta["blob_path"] if meta else None
        # 不变式：绝不删除元数据当前指向的有效副本（同号重试安全网）
        if source and source != current and os.path.exists(source):
            # 切换前读到旧路径的在途下载必须先结束（切换后发起的下载
            # 读的是新路径，与旧文件无关，不在计数内也无需等待）
            if self._tracker.active(image_id):
                return False
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, os.remove, source)
            except FileNotFoundError:
                pass  # 旧件已不在，视同清理完成
            except OSError as exc:
                # 清理失败（旧卷只读/暂不可写）：记录并持续重试，
                # 只有旧件真正清除才让条目 done、作业才可能 completed
                msg = f"cleanup retry: cannot remove old blob: {exc!r}"
                self._db.set_migration_item(job_id, image_id, error=msg)
                self._db.bump_migration(job_id, error=msg)
                return False
        self._db.set_migration_item(job_id, image_id, state="done", error="")
        self._db.bump_migration(job_id, migrated=1, error="")
        return True

    def _fail_item(self, job_id: str, image_id: str, error: str):
        self._db.set_migration_item(job_id, image_id, state="failed",
                                    error=error)
        self._db.bump_migration(job_id, failed=1)

    # ------------------------------------------------------------------
    # 文件操作（全部密文原样搬运，不解密、不改写）
    # ------------------------------------------------------------------
    async def _copy_verified(self, source: str, target: str, job_id: str):
        """复制到临时文件 -> 逐字节核对 -> 原子 rename 就位。"""
        loop = asyncio.get_running_loop()
        tmp = f"{target}.migpart-{job_id}"
        await loop.run_in_executor(None, self._remove_quiet_sync, tmp)
        await loop.run_in_executor(None, self._copy_file, source, tmp)
        if not await self._files_equal(source, tmp):
            await loop.run_in_executor(None, self._remove_quiet_sync, tmp)
            raise MigrationError(
                f"byte verify failed after copy: {source} -> {tmp}")
        await loop.run_in_executor(None, os.replace, tmp, target)
        await loop.run_in_executor(None, self._fsync_dir,
                                   os.path.dirname(target))

    async def _files_equal(self, path_a: str, path_b: str) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._files_equal_sync,
                                          path_a, path_b)

    async def _remove_quiet(self, path: str):
        await asyncio.get_running_loop().run_in_executor(
            None, self._remove_quiet_sync, path)

    @staticmethod
    def _copy_file(src: str, dst: str):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, "rb") as fin, open(dst, "wb") as fout:
            while True:
                chunk = fin.read(COPY_CHUNK)
                if not chunk:
                    break
                fout.write(chunk)
            fout.flush()
            os.fsync(fout.fileno())

    @staticmethod
    def _files_equal_sync(path_a: str, path_b: str) -> bool:
        """逐字节核对两个文件是否完全一致。"""
        try:
            if os.path.getsize(path_a) != os.path.getsize(path_b):
                return False
        except OSError:
            return False
        try:
            with open(path_a, "rb") as fa, open(path_b, "rb") as fb:
                while True:
                    ca, cb = fa.read(COPY_CHUNK), fb.read(COPY_CHUNK)
                    if ca != cb:
                        return False
                    if not ca:
                        return True
        except OSError:
            return False

    @staticmethod
    def _remove_quiet_sync(path: str):
        try:
            os.remove(path)
        except OSError:
            pass

    @staticmethod
    def _fsync_dir(path: str):
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass
