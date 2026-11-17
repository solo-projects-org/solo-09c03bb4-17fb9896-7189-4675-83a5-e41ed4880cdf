"""T13-T21：密文存储卷迁移。

覆盖：
  T13 按号迁移：逐字节一致、切换元数据、清理旧卷、原编号仍可下载、信封不动
  T14 缺省迁移全部 ready；failed 行不受影响
  T15 请求校验：未知编号 404、目标等于当前卷 400、未发布 409、进行中 409
  T16 迁移期间：已开始的慢下载与切换后新发起的下载都完整
  T17 迁移与主密钥轮换交错：信封被轮换改包（迁移绝不回写旧信封），正文不动
  T18 复制失败：旧卷继续供数；同号重试成功且不删当前有效副本
  T19 迁移完成后同号重试为幂等；迁往第二块新卷只清理非当前副本
  T20 API 重启接续：copying（含残留临时文件）与 switched 两种中途状态
  T21 清理失败持续重试，旧卷恢复可写后才报完成
"""
import os
import sqlite3
import threading
import time

import httpx
import pytest

from conftest import (_download, _key_headers, _upload, _wait_migration,
                      _wait_rotation)


def _db_row(stack, image_id):
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM images WHERE image_id=?",
                       (image_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _start_migration(client, ids=None, target_dir=None):
    body = {"confirm": True}
    if ids is not None:
        body["image_ids"] = ids
    if target_dir is not None:
        body["target_dir"] = target_dir
    r = client.post("/migrations", json=body)
    assert r.status_code == 202, r.text
    return r.json()


def _seed_job(stack, job_id, target_dir, items):
    """直接往元数据库写迁移作业/条目（模拟崩溃现场），items 为
    (image_id, state, source_path, target_path)。"""
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    now = time.time()
    conn.execute(
        "INSERT INTO migration_jobs (job_id, state, target_dir, total, "
        "migrated, failed, started_at, updated_at) "
        "VALUES (?, 'running', ?, ?, 0, 0, ?, ?)",
        (job_id, target_dir, len(items), now, now))
    for image_id, state, src, dst in items:
        conn.execute(
            "INSERT INTO migration_items (job_id, image_id, source_path, "
            "target_path, state, updated_at) VALUES (?,?,?,?,?,?)",
            (job_id, image_id, src, dst, state, now))
    conn.commit()
    conn.close()


def test_migration_moves_blobs_byte_identical_and_ids_still_serve(stack):
    client = stack["client"]
    payloads = {
        "mig-a": b"",
        "mig-b": b"slide-bytes-\x00\x01" * 1000,
        "mig-c": os.urandom(2 * 1024 * 1024 + 33),  # 跨帧
    }
    before = {}
    for image_id, data in payloads.items():
        assert _upload(client, data, image_id=image_id).status_code == 201
        row = _db_row(stack, image_id)
        before[image_id] = {
            "blob_path": row["blob_path"],
            "blob_bytes": open(row["blob_path"], "rb").read(),
            "wrapped_dek": row["wrapped_dek"],
            "master_key_id": row["master_key_id"],
        }

    job = _start_migration(client, ids=list(payloads))
    assert job["total"] == 3 and job["state"] == "running"
    result = _wait_migration(client, job["job_id"])
    assert result["state"] == "completed", result
    assert result["migrated"] == 3 and result["failed"] == 0
    assert result["progress_percent"] == 100.0

    for image_id, data in payloads.items():
        meta = client.get(f"/images/{image_id}/meta").json()
        # 元数据已切换到新卷；新卷密文与旧卷逐字节一致；旧件已清除
        assert meta["blob_path"].startswith(str(stack["new_blob_dir"]))
        assert open(meta["blob_path"], "rb").read() == before[image_id]["blob_bytes"]
        assert not os.path.exists(before[image_id]["blob_path"])
        # 原编号仍能取数，且明文逐字节一致
        assert _download(client, image_id) == data
        # 信封与主密钥绑定完全未被迁移触碰
        row = _db_row(stack, image_id)
        assert row["wrapped_dek"] == before[image_id]["wrapped_dek"]
        assert row["master_key_id"] == before[image_id]["master_key_id"]

    # latest 可查；Swagger/OpenAPI 暴露迁移接口
    assert client.get("/migrations/latest").json()["job_id"] == job["job_id"]
    paths = client.get("/openapi.json").json()["paths"]
    assert "/migrations" in paths and "/migrations/{job_id}" in paths


def test_migration_defaults_to_all_ready_and_skips_failed(stack):
    client = stack["client"]
    _upload(client, b"ready-1", image_id="mig-r1")
    _upload(client, b"ready-2", image_id="mig-r2")
    # 一件 failed 原片（无有效 blob），不应进入迁移清单
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    mk = client.get("/keys/status").json()["current_key_id"]
    conn.execute(
        "INSERT INTO images (image_id, status, master_key_id, wrapped_dek, "
        "blob_path, created_at) VALUES ('mig-dead','failed',?,?,?,?)",
        (mk, "00" * 64, str(stack["blob_dir"] / "mig-dead.msef"), time.time()))
    conn.commit()
    conn.close()

    job = _start_migration(client)  # 不带 image_ids -> 全部 ready
    assert job["total"] == 2
    result = _wait_migration(client, job["job_id"])
    assert result["state"] == "completed", result
    assert _db_row(stack, "mig-dead")["blob_path"].endswith("mig-dead.msef")
    assert _download(client, "mig-r1") == b"ready-1"


def test_migration_request_validation(stack):
    client = stack["client"]
    _upload(client, b"victim", image_id="mig-val")
    # 未知编号 -> 404
    r = client.post("/migrations", json={"image_ids": ["nope-1"]})
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "images_not_found"
    # 目标就是当前卷 -> 400
    r = client.post("/migrations",
                    json={"image_ids": ["mig-val"],
                          "target_dir": str(stack["blob_dir"])})
    assert r.status_code == 400
    # 空列表 -> 400
    assert client.post("/migrations", json={"image_ids": []}).status_code == 400
    # 未发布（receiving）-> 409
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    mk = client.get("/keys/status").json()["current_key_id"]
    conn.execute(
        "INSERT INTO images (image_id, status, master_key_id, wrapped_dek, "
        "blob_path, created_at) VALUES ('mig-recv','receiving',?,?,?,?)",
        (mk, "00" * 64, str(stack["blob_dir"] / "mig-recv.msef"), time.time()))
    conn.commit()
    conn.close()
    r = client.post("/migrations", json={"image_ids": ["mig-recv"]})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "images_not_ready"
    # 正常发起；进行中再次发起 -> 409
    job = _start_migration(client, ids=["mig-val"])
    r = client.post("/migrations", json={"image_ids": ["mig-val"]})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "migration_in_progress"
    assert _wait_migration(client, job["job_id"])["state"] == "completed"


def test_inflight_and_new_downloads_complete_during_migration(stack):
    client = stack["client"]
    payload = os.urandom(8 * 1024 * 1024)
    _upload(client, payload, image_id="mig-dl")
    old_path = _db_row(stack, "mig-dl")["blob_path"]

    received, errors = [], []

    def slow_download():
        try:
            with httpx.Client(base_url=stack["api"].base, timeout=60) as c:
                with c.stream("GET", "/images/mig-dl") as resp:
                    assert resp.status_code == 200
                    buf = bytearray()
                    for chunk in resp.iter_bytes(16384):
                        buf.extend(chunk)
                        time.sleep(0.01)  # 慢速客户端，覆盖整个迁移窗口
                    received.append(bytes(buf))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=slow_download)
    t.start()
    time.sleep(0.5)  # 确认下载已开始（响应头已发、DEK 已解包）

    job = _start_migration(client, ids=["mig-dl"])
    # 等元数据切换到新卷；此刻慢下载仍在读旧文件，清理必须等待 -> 旧件仍在
    deadline = time.time() + 30
    while time.time() < deadline:
        if client.get("/images/mig-dl/meta").json()["blob_path"] != old_path:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("metadata was not switched")
    assert os.path.exists(old_path), "old blob deleted while download in flight"

    # 切换后新发起的下载（走新卷）也必须完整
    assert _download(client, "mig-dl") == payload

    t.join(timeout=60)
    assert not errors and received and received[0] == payload
    result = _wait_migration(client, job["job_id"])
    assert result["state"] == "completed", result
    assert not os.path.exists(old_path)  # 下载结束后旧件才被清除


def test_migration_interleaved_with_rotation_never_writes_back_envelope(stack):
    client, kc = stack["client"], stack["key_client"]
    payloads = {f"mig-rot-{i}": os.urandom(512 * 1024 + i) for i in range(3)}
    before = {}
    for image_id, data in payloads.items():
        _upload(client, data, image_id=image_id)
        row = _db_row(stack, image_id)
        before[image_id] = {"blob_bytes": open(row["blob_path"], "rb").read(),
                            "wrapped_dek": row["wrapped_dek"],
                            "master_key_id": row["master_key_id"],
                            "blob_path": row["blob_path"]}
    old_key = before["mig-rot-0"]["master_key_id"]

    # 慢密钥服务拉长轮换窗口，使迁移与轮换真正交错
    env = dict(stack["keys"]._env)
    env["KEY_OP_DELAY"] = "0.3"
    stack["keys"].stop()
    stack["keys"]._env = env
    stack["keys"].start()

    rot = client.post("/rotations", json={"confirm": True})
    assert rot.status_code == 202
    mig = _start_migration(client)  # 轮换进行中照常接受迁移
    rot_result = _wait_rotation(client, rot.json()["job_id"], timeout=120)
    mig_result = _wait_migration(client, mig["job_id"], timeout=120)
    assert rot_result["state"] == "completed", rot_result
    assert mig_result["state"] == "completed", mig_result

    new_key = client.get("/keys/status").json()["current_key_id"]
    assert new_key != old_key
    for image_id, data in payloads.items():
        row = _db_row(stack, image_id)
        # 信封被轮换改包到新 MK——迁移只换路径，绝不把旧信封写回
        assert row["master_key_id"] == new_key
        assert row["wrapped_dek"] != before[image_id]["wrapped_dek"]
        # 新信封在新 MK 下可解包（密钥服务视角验证信封有效）
        r = kc.post("/internal/dek/unwrap",
                    json={"key_id": new_key, "wrapped_dek": row["wrapped_dek"]},
                    headers=_key_headers())
        assert r.status_code == 200
        # 正文逐字节未变，且已在新卷；旧件已清除
        assert row["blob_path"].startswith(str(stack["new_blob_dir"]))
        assert open(row["blob_path"], "rb").read() == before[image_id]["blob_bytes"]
        assert not os.path.exists(before[image_id]["blob_path"])
        assert _download(client, image_id) == data
    # 旧 MK 已被轮换流程销毁
    states = {k["key_id"]: k["state"]
              for k in client.get("/keys/status").json()["keys"]}
    assert states[old_key] == "destroyed"


def test_copy_failure_keeps_old_volume_serving_and_same_id_retry(stack):
    client = stack["client"]
    payload = b"fragile-volume-" * 1000
    _upload(client, payload, image_id="mig-fail")
    old_path = _db_row(stack, "mig-fail")["blob_path"]

    # 新卷只读 -> 复制必然失败
    target = stack["new_blob_dir"]
    target.mkdir(parents=True, exist_ok=True)
    os.chmod(target, 0o500)
    try:
        job = _start_migration(client, ids=["mig-fail"])
        result = _wait_migration(client, job["job_id"])
        assert result["state"] == "failed", result
        assert result["failed"] == 1
        assert result["failed_items"][0]["image_id"] == "mig-fail"
        # 元数据未切换，旧卷继续供数
        assert _db_row(stack, "mig-fail")["blob_path"] == old_path
        assert _download(client, "mig-fail") == payload
        # 新卷不留半成品
        assert list(target.iterdir()) == []
    finally:
        os.chmod(target, 0o700)

    # 同号重试：成功迁移；有效副本（新卷）完好，旧件清除
    job2 = _start_migration(client, ids=["mig-fail"])
    assert _wait_migration(client, job2["job_id"])["state"] == "completed"
    meta = client.get("/images/mig-fail/meta").json()
    assert meta["blob_path"].startswith(str(target))
    assert not os.path.exists(old_path)
    assert _download(client, "mig-fail") == payload


def test_retry_after_success_is_noop_and_second_volume_swing(stack):
    client = stack["client"]
    payload = b"swing-me-" * 500
    _upload(client, payload, image_id="mig-swing")
    first_dir = stack["new_blob_dir"]
    second_dir = stack["next_dir"] / "blobs2"

    job1 = _start_migration(client, ids=["mig-swing"])
    assert _wait_migration(client, job1["job_id"])["state"] == "completed"
    first_path = _db_row(stack, "mig-swing")["blob_path"]
    assert first_path.startswith(str(first_dir))
    first_bytes = open(first_path, "rb").read()

    # 同号同目标重试：幂等完成，当前有效副本不被删除
    job2 = _start_migration(client, ids=["mig-swing"])
    assert _wait_migration(client, job2["job_id"])["state"] == "completed"
    assert os.path.exists(first_path)
    assert open(first_path, "rb").read() == first_bytes
    assert _download(client, "mig-swing") == payload

    # 再迁往第二块新卷：切换后只清理第一块卷（非当前副本），新副本完好
    job3 = _start_migration(client, ids=["mig-swing"],
                            target_dir=str(second_dir))
    assert _wait_migration(client, job3["job_id"])["state"] == "completed"
    row = _db_row(stack, "mig-swing")
    assert row["blob_path"].startswith(str(second_dir))
    assert open(row["blob_path"], "rb").read() == first_bytes
    assert not os.path.exists(first_path)
    assert _download(client, "mig-swing") == payload


def test_migration_resumes_after_api_restart(stack):
    client = stack["client"]
    new_dir = stack["new_blob_dir"]
    new_dir.mkdir(parents=True, exist_ok=True)
    payloads = {f"mig-res-{i}": os.urandom(512 * 1024 + i) for i in range(1, 6)}
    rows, blob_bytes, new_paths = {}, {}, {}
    for image_id, data in payloads.items():
        _upload(client, data, image_id=image_id)
        rows[image_id] = _db_row(stack, image_id)
        blob_bytes[image_id] = open(rows[image_id]["blob_path"], "rb").read()
        new_paths[image_id] = str(new_dir / f"{image_id}.msef")

    # 崩溃现场 1：复制到一半（copying），残留垃圾临时文件
    stale_tmp = f"{new_paths['mig-res-1']}.migpart-mig-seeded"
    with open(stale_tmp, "wb") as f:
        f.write(b"garbage-partial")
    # 崩溃现场 2：已切换元数据（switched）但旧件未清理
    with open(new_paths["mig-res-2"], "wb") as f:
        f.write(blob_bytes["mig-res-2"])
    # 崩溃现场 3：复制完成（copied）尚未切换
    with open(new_paths["mig-res-3"], "wb") as f:
        f.write(blob_bytes["mig-res-3"])
    # 崩溃现场 4：copied 且元数据其实已切换（崩溃在状态落库前）
    with open(new_paths["mig-res-4"], "wb") as f:
        f.write(blob_bytes["mig-res-4"])
    # 崩溃现场 5：copied 但新卷文件在停机窗口被损 -> 必须重新核对并复制
    with open(new_paths["mig-res-5"], "wb") as f:
        f.write(b"corrupted-target")
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    conn.execute("UPDATE images SET blob_path=? WHERE image_id='mig-res-2'",
                 (new_paths["mig-res-2"],))
    conn.execute("UPDATE images SET blob_path=? WHERE image_id='mig-res-4'",
                 (new_paths["mig-res-4"],))
    conn.commit()
    conn.close()
    _seed_job(stack, "mig-seeded", str(new_dir), [
        ("mig-res-1", "copying", rows["mig-res-1"]["blob_path"],
         new_paths["mig-res-1"]),
        ("mig-res-2", "switched", rows["mig-res-2"]["blob_path"],
         new_paths["mig-res-2"]),
        ("mig-res-3", "copied", rows["mig-res-3"]["blob_path"],
         new_paths["mig-res-3"]),
        ("mig-res-4", "copied", rows["mig-res-4"]["blob_path"],
         new_paths["mig-res-4"]),
        ("mig-res-5", "copied", rows["mig-res-5"]["blob_path"],
         new_paths["mig-res-5"]),
    ])

    stack["api"].restart()
    result = _wait_migration(client, "mig-seeded", timeout=60)
    assert result["state"] == "completed", result
    assert result["migrated"] == 5 and result["failed"] == 0
    for image_id, data in payloads.items():
        # 全部：新卷逐字节一致、旧件清除、原编号可取
        assert open(new_paths[image_id], "rb").read() == blob_bytes[image_id]
        assert not os.path.exists(rows[image_id]["blob_path"]), image_id
        assert _download(client, image_id) == data
    assert not os.path.exists(stale_tmp)  # 垃圾临时文件已清


def test_cleanup_failure_retried_until_old_volume_allows(stack):
    client = stack["client"]
    payload = b"cleanup-retry-" * 1000
    _upload(client, payload, image_id="mig-clean")
    row = _db_row(stack, "mig-clean")
    new_dir = stack["new_blob_dir"]
    new_dir.mkdir(parents=True, exist_ok=True)
    new_path = str(new_dir / "mig-clean.msef")
    with open(row["blob_path"], "rb") as f:
        data = f.read()
    with open(new_path, "wb") as f:
        f.write(data)
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db", timeout=30)
    conn.execute("UPDATE images SET blob_path=? WHERE image_id='mig-clean'",
                 (new_path,))
    conn.commit()
    conn.close()
    _seed_job(stack, "mig-cleaning", str(new_dir), [
        ("mig-clean", "switched", row["blob_path"], new_path),
    ])

    # 旧卷只读：删除旧件必然失败，作业不得报完成
    os.chmod(stack["blob_dir"], 0o500)
    try:
        stack["api"].restart()
        time.sleep(3.0)  # 覆盖数次清理重试
        job = client.get("/migrations/mig-cleaning").json()
        assert job["state"] == "running", job
        assert "cleanup" in (job["error"] or "")
        assert os.path.exists(row["blob_path"])
        # 清理受阻期间新卷照常供数
        assert _download(client, "mig-clean") == payload
    finally:
        os.chmod(stack["blob_dir"], 0o700)

    # 旧卷恢复可写：接续清理，旧件清除后才报完成
    result = _wait_migration(client, "mig-cleaning", timeout=60)
    assert result["state"] == "completed", result
    assert not os.path.exists(row["blob_path"])
    assert _download(client, "mig-clean") == payload


def test_other_images_ingest_and_download_unaffected(stack):
    client = stack["client"]
    big = os.urandom(4 * 1024 * 1024)
    _upload(client, big, image_id="mig-bg-1")
    _upload(client, b"bystander", image_id="mig-bg-2")

    job = _start_migration(client, ids=["mig-bg-1"])
    # 迁移进行中：新原片照常入仓（仍写当前卷），其他原片照常下载
    r = _upload(client, b"new-arrival", image_id="mig-bg-3")
    assert r.status_code == 201
    assert _download(client, "mig-bg-2") == b"bystander"
    result = _wait_migration(client, job["job_id"])
    assert result["state"] == "completed", result

    # 不在迁移清单里的原片留在原卷且可读；被迁移的已切到新卷
    assert _db_row(stack, "mig-bg-3")["blob_path"].startswith(str(stack["blob_dir"]))
    assert _db_row(stack, "mig-bg-1")["blob_path"].startswith(
        str(stack["new_blob_dir"]))
    assert _download(client, "mig-bg-1") == big
    assert _download(client, "mig-bg-3") == b"new-arrival"
