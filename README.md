# 显微成像原片加密数仓（单实例 API + 独立密钥服务）

显微仪器原片（raw image）的接收、流式加密存储与按号读取。每件原片使用
**独立数据密钥（DEK）**，DEK 由独立持久化的**密钥服务**用主密钥（MK）以
AES-256-GCM 信封包裹；支持主密钥在线轮换：只迁移密钥信封、**不重写图像正文**，
迁移核实完成后再销毁旧主密钥。支持**密文存储卷在线更换**：按编号把已发布
原片的密文搬到 Compose 挂载的新卷，逐字节核对后切换元数据并清理旧卷，
全程不解密、不改写原片与信封，原编号取数不中断。

## 架构

```
仪器/客户端
   │  HTTP（octet-stream 流式上传 / 按号下载）
   ▼
┌──────────────────────────────┐        内部 HTTP（共享令牌）     ┌──────────────────────────┐
│ image-api (FastAPI, 单实例)   │  ────────────────────────────▶  │ keystore (FastAPI)        │
│  - 原片流式分帧加解密（内存）   │     generate/unwrap/rewrap     │  - 主密钥生命周期          │
│  - SQLite 元数据（只存密文信封）│     rotate/destroy/status      │  - DEK 包裹/解包/改包       │
│  - 轮换作业状态机 + 后台线程    │                                 │  - SQLite 持久卷（MK 本体） │
│  - 卷迁移作业状态机 + 后台线程  │                                 └──────────────────────────┘
│  - blob：密文原片 .msef        │                                            │
└──────────────┬───────────────┘                                            │
               │ 卷 api-data（当前）+ 卷 api-data-next（迁移目标）  卷 keystore-data ◀┘
        /data/metadata.db
        /data/blobs/*.msef（密文）  →迁移→  /data-next/blobs/*.msef（密文）
```

**安全边界**

| 资产 | 存在位置 | 不存在位置 |
|---|---|---|
| 主密钥材料 | 仅密钥服务进程内存与其持久卷 SQLite | API 进程/API 卷/日志 |
| 明文 DEK | 仅单次请求的 API 进程内存（请求结束即释放） | 任何磁盘 |
| 明文原片 | 仅请求/响应的内存流（multipart 假脱机已禁用） | 任何磁盘、任何临时文件 |
| 信封 DEK（密文） | API 的 SQLite `images.wrapped_dek` | — |
| 原片密文 | API 卷 `/data/blobs/*.msef`（AES-256-GCM 分帧） | — |

销毁主密钥 = 密钥库中密钥材料被零覆写并置 `destroyed`，密码学意义不可恢复；
对已销毁 MK 的 `unwrap/rewrap` 返回 `410 key_destroyed`。

## 快速启动

```bash
cp .env.example .env          # 修改 KEY_SERVICE_TOKEN 为强随机值
docker compose up --build
```

- API Swagger：<http://localhost:8000/docs>
- 密钥服务 Swagger：<http://localhost:8001/docs>（仅绑定 127.0.0.1）
- 健康检查：`GET /health`（两服务均有）

## 常用接口（API）

| 方法/路径 | 说明 |
|---|---|
| `POST /images` | 流式上传（随机编号）；`X-Idempotency-Token` 幂等；`X-Filename` 可选 |
| `PUT /images/{image_id}` | 按调用方编号上传 |
| `GET /images/{image_id}` | 按号下载，流式解密、逐帧 GCM 验签、SHA-256 校验 |
| `GET /images/{image_id}/meta` | 元数据/状态（不返回信封密文） |
| `GET /images` | 列表 |
| `POST /rotations` | 发起主密钥轮换（body `{"confirm": true}`），返回 202 + job_id |
| `GET /rotations/{job_id}` | 轮换进度（state/migrated/failed/progress_percent） |
| `GET /rotations/latest` | 最近一次轮换 |
| `POST /migrations` | 发起卷迁移（`image_ids` 缺省为全部 ready；`target_dir` 缺省为 `MIGRATION_TARGET_DIR`），返回 202 + job_id |
| `GET /migrations/{job_id}` | 迁移进度（state/migrated/failed/failed_items/progress_percent） |
| `GET /migrations/latest` | 最近一次迁移 |
| `GET /keys/status` | 代理密钥服务只读状态 |

上传请使用原始 `application/octet-stream`（`curl --data-binary @file`），
避免 multipart 带来的额外内存占用。

```bash
# 上传（按号）
curl -sS -X PUT --data-binary @slide_001.czi \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Filename: slide_001.czi' \
  http://localhost:8000/images/slide-001

# 下载并逐字节校验
curl -sS http://localhost:8000/images/slide-001 -o /tmp/slide_001.czi
sha256sum slide_001.czi /tmp/slide_001.czi

# 发起轮换 / 查进度
JOB=$(curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":true}' http://localhost:8000/rotations | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
watch -n1 "curl -sS http://localhost:8000/rotations/$JOB"

# 更换密文卷：按编号迁移（image_ids 缺省时迁移全部 ready 原片）
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":true,"image_ids":["slide-001"]}' http://localhost:8000/migrations
watch -n1 "curl -sS http://localhost:8000/migrations/latest"
```

## 轮换语义（需求对照）

1. `POST /rotations` 先在密钥服务生成并切换新 MK——**新入湖立即使用新 MK**；
   本地建作业（状态持久化），未完成时再次发起返回 `409 rotation_in_progress`。
2. 后台常驻线程逐行把旧 MK 包裹的 DEK 调密钥服务 `rewrap` 改包到新 MK，
   **只更新元数据里的 64 字节信封，blob 密文正文逐字节不动**。
3. 销毁前两次零残留核验（扫描后 + 销毁前），确认没有 `ready/receiving` 行
   仍依赖旧 MK，才调用销毁；销毁后再用只读 `status` 核实，**不提前报完成**。
4. **在途下载**：下载开始时只向密钥服务解包一次 DEK，之后帧解密完全在 API
   进程内存完成；即使下载很慢、期间旧 MK 被销毁，下载仍完整成功。
5. **在途上传**：上传期间发生轮换时，finalize 会先 rewrap 改绑当前 MK 再发布；
   若旧 MK 已被销毁（极端窗口）则返回 `409 old_key_destroyed` 并判失败，
   **绝不晚发布不可读原片**。进程崩溃留下的 `receiving` 在重启时一律判失败。
6. **慢密钥服务不阻塞**：迁移在独立线程、按批让出；在线上传/下载照常完成。
7. **回执丢失**：`destroy` 网络层失败/连接被重置时，API 自动重试并以只读状态
   核实销毁是否真实生效；rewrap 带幂等令牌，重放返回同一结果。
8. **进程重启接续**：轮换作业状态在 SQLite；API 重启后自动驱动未完成作业到
   终态；若崩溃发生在“MK 已切换、作业未建”窗口，启动时自动对账补建作业。

## 卷迁移语义（更换密文存储卷）

1. `POST /migrations` 按编号迁移 `ready` 原片（`image_ids` 缺省 = 全部 ready；
   `target_dir` 缺省 = 环境变量 `MIGRATION_TARGET_DIR`，即 Compose 挂载的新卷）。
   同一时刻只允许一个迁移作业，进行中再次发起返回 `409 migration_in_progress`。
2. 每件原片的状态机：`copying`（流式复制到新卷临时文件，**逐字节核对**一致后
   原子 rename）→ `copied`（条件 UPDATE 切换元数据 `blob_path`，**只改路径列**）
   → `switched`（等在途下载归零后删除旧卷文件）→ `done`。
   **全程不解密、不改写原片正文与信封**（信封列从不经手）。
3. **取数不中断**：下载在读取元数据前登记，迁移切换路径后会等该原片的在途
   下载全部结束才删旧文件；切换后新发起的下载直接从新卷取数。
4. **与轮换并存**：轮换的 rewrap 只条件更新信封列，迁移只条件更新路径列，
   互不覆盖——迁移绝不可能把旧信封写回。
5. **复制失败**：条目判失败、元数据不动，**旧卷继续供数**；作业整体报
   `failed` 并列出 `failed_items`，修复后同号重试即可。
6. **清理失败/进程重启**：清理持续重试（旧卷恢复可写后自动接续）；作业与
   条目状态持久化，API 重启后自动从 `copying/copied/switched` 任一阶段接续。
   **只有新卷可取数且旧件已清除，作业才报 `completed`。**
7. **同号重试安全**：元数据已指向新卷时跳过复制；清理的不变式是
   “绝不删除元数据当前指向的有效副本”。
8. 迁移只动清单内的原片：其他原片照常入仓（仍写当前卷 `BLOB_DIR`）与下载。
   迁移完成后如需让新入仓也写新卷，把 `BLOB_DIR` 指到新卷挂载点即可
   （元数据按件记录绝对路径，历史件不受影响），详见 docs/OPERATIONS.md。

## 故障注入（演练用，密钥服务环境变量）

| 变量 | 作用 |
|---|---|
| `KEY_OP_DELAY=0.3` | 每个密钥操作人为延迟 0.3s，模拟慢密钥服务 |
| `KEY_DESTROY_DROP_RECEIPT=true` | 销毁成功落盘后故意不回完整 HTTP 响应，模拟销毁后丢回执 |

```bash
KEY_DESTROY_DROP_RECEIPT=true docker compose up --build keystore
```

## 自测

```bash
# tests/ 下为真实 uvicorn 子进程的端到端测试（32 项，含离线加密单测）
python -m pytest tests/ -q
```

覆盖：逐字节往返（空文件/跨帧/多帧、密文布局尺寸）、信封 AAD/错钥拒绝、
明文不落盘（blob/元数据库/临时文件三处核查）、幂等上传与密钥操作幂等、
篡改 GCM 失败、完整轮换（信封迁移+正文不动+二次轮换仍不动+销毁后可读）、
进度与并发轮换拒绝、慢密钥服务并发、在途下载跨销毁、销毁丢回执自动核实、
双服务重启可读性、轮换中途重启接续、崩溃在途上传不晚发布、
跨越轮换的上传自动改绑；卷迁移：逐字节搬运与旧卷清理、缺省全量与请求校验、
迁移中在途/新发下载完整、迁移×轮换交错（信封不回写）、复制失败旧卷供数、
同号重试幂等与双卷摆荡、API 重启接续（copying/switched 中途态）、
清理失败重试至成功、迁移期间其他原片入仓下载不受影响。

## 限制与生产化建议（见 docs/ 下说明）

单实例 + SQLite 面向单机/单节点部署；扩成多实例需把轮换协调改为分布式锁、
blob 存储换成对象存储，并在密钥服务前加 mTLS/审计与 HSM/KMS 托管根密钥。
