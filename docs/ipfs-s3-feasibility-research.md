# IPFS(Kubo) 分布式 S3 兼容网关 — 可行性调研报告

> 日期: 2026-07-02
> 状态: 调研阶段
> 范围: 既有项目调研、分布式拓扑方案、元数据一致性、状态协调、本地 docker compose dev 拓扑、风险清单

---

## 1. 既有同类项目调研

### 1.1 对比总表

| 项目 | 语言 | 架构 | 活跃度 | 可复用性 | 关键教训 |
|------|------|------|--------|----------|----------|
| **aricanduva** (bltavares) | Rust | S3→IPFS 代理，对接外部 Kubo RPC | ⚠️ 个人项目，低活跃 (latest commit 2024) | ★★★ 架构参考价值高 | 简单桥接模式可行，支持 `x-ipfs-path` header 与 IPFS Companion 互操作，仅覆盖基础 S3 操作 |
| **s3x** (RTradeLtd) | Go | MinIO Gateway 插件，依赖 TemporalX→IPFS | ❌ 已死 (MinIO 2022-10 彻底移除 Gateway 代码) | ★ 不可复用 | **关键教训**: 依赖第三方 Gateway 框架有被上游移除的风险。MinIO Gateway 因 S3 API 演进(版本控制/加密/压缩等)无法在无状态代理模式下支持而废弃 |
| **ipfs_kit_py** (endomorphosis) | Python | 多后端(Master/Worker/Leecher 角色) + IPFS Cluster 管理 | ⚠️ S3 Gateway 标为 "📋 Planned" 未实现 | ★★ Cluster 管理思路参考 | 角色分离设计(主/工作/只读)有参考价值，但 S3 部分未落地 |
| **zs3** (Lulzx) | Zig | 原生 S3 + IPFS-like 分布式(Kademlia DHT, BLAKE3 内容寻址) | 🟢 活跃 (2025-2026, 0 deps) | ★★★ 设计思路极佳 | 零依赖、分布式模式值得学习，但不是 IPFS 生态兼容(用自己的协议) |
| **SynapS3** (strahe) | Go | S3 兼容网关 → Filecoin 存储 | 🟢 活跃 (2026-04 创建) | ★ 目标不同 | Filecoin 而非 IPFS，S3 兼容矩阵做得细致 |
| **Filebase** | 商业 | S3 API → IPFS pinning (两个 endpoint: s3.filebase.io vs s3.filebase.com) | 🟢 商业运营 | ★★ API 设计参考 | 证明了 S3→IPFS 的商业模式可行。专用网关+CDN 是关键增值 |
| **Pinata** | 商业 | 托管 IPFS pinning + 专用网关 + CDN，自称 "Easier than S3" | 🟢 商业运营 | ★★ Gateway/CDN 设计参考 | 专用网关(account-scoped)模式验证了隔离与性能的平衡 |
| **Textile Buckets** | Go | Buckets(类 S3) → ThreadDB + Powergate + UnixFS → IPFS/Filecoin | ❌ 已弃坑 (Hosted Hub 2023-01-09 下线) | ★ 不可复用 | **关键教训**: 依赖中心化 Hub 的架构有单点风险；项目复杂度(ThreadDB+Powergate+Buckets 三层)过高导致维护困难 |
| **js-datastore-s3** (IPFS 官方) | JS/TS | 反向: S3 作为 IPFS 数据存储后端 | ❌ 已归档 | 不适用 | 方向相反(S3→IPFS datastore 而非 S3 API over IPFS) |

### 1.2 综合结论

1. **不存在成熟的、活跃维护的「S3 over IPFS」开源网关**。s3x 死了因为 MinIO 砍 Gateway；Textile Buckets 死了因为中心化 Hub 不可持续；aricanduva 太简单仅作 homelab 用。

2. **Filebase 和 Pinata 证明了商业模式**，但它们是闭源商业服务。

3. **可参考的设计模式**:
   - aricanduva 的 "S3 API → Kubo RPC" 桥接模式(最简)
   - ipfs_kit_py 的 Master/Worker/Leecher 角色分离
   - zs3 的零依赖、内容寻址+元数据索引分离的存储布局
   - Filebase 的双 endpoint 模式(S3 兼容层 vs IPFS 原生层)

4. **核心教训**:
   - 不要依赖第三方 Gateway 框架(MinIO 教训)
   - 不要引入过多抽象层(Textile 教训)
   - 元数据与内容存储必须解耦(S3 语义 ≠ IPFS 语义)

---

## 2. 分布式网关节点拓扑方案对比

### 2.1 三种方案

| 维度 | 方案 A: 每节点独立 Kubo + IPFS Cluster | 方案 B: 多节点共享同一 Kubo | 方案 C: 独立 Kubo + Bitswap 自然共享 |
|------|----------------------------------------|-----------------------------|--------------------------------------|
| **架构** | 每网关旁路一个 Kubo + Cluster sidecar，Cluster 协调 pinset | 所有网关连接同一个 Kubo RPC | 每网关独立 Kubo，依赖 IPFS 网络自然发现和 Bitswap 传输 |
| **Pin 协调** | Cluster CRDT/Raft 保证强最终一致 pinset | 无需协调(单 Kubo) | 无协调，各自 pin |
| **数据冗余** | Cluster 控制 replication_factor (min/max)，自动分配到多个 IPFS peer | 单点，Kubo 挂了全挂 | 各节点各自 pin = 全冗余(每节点有完整副本) |
| **写入路径** | S3 Put → 网关 → 任意 Kubo add → CID 返回 → Cluster pin(CID) → Cluster 按策略分配复制 | S3 Put → 网关 → 单一 Kubo add → CID | S3 Put → 网关 → 本地 Kubo add+pin → 其他节点通过 Bitswap 发现 |
| **读取路径** | 网关 → 本地 Kubo(cat/pin 已有) 或任意 Cluster peer 的 Kubo | 网关 → 共享 Kubo cat | 网关 → 本地 Kubo(如有) → Bitswap 从其他 peer 拉取 |
| **故障隔离** | ★★★ 好，单 Kubo 故障不影响其他网关节点 | ★ 差，Kubo 是单点 | ★★ 中，节点独立但无协调 |
| **运维复杂度** | ★★ 中，需管理 Cluster 配置(secret/trusted_peers/bootstrap) | ★★★ 低 | ★★★ 低 |
| **适合场景** | 生产多节点 | 单机 dev/小规模 | 对等/去中心化场景 |

### 2.2 推荐

**Dev 环境**: 方案 B(单 Kubo)，最简。

**生产环境**: 方案 A(独立 Kubo + IPFS Cluster)。理由:
- IPFS Cluster 提供 pinset 全局视图、自动复制、故障恢复
- `replication_factor_min/max` 允许灵活配置冗余度
- CRDT 模式支持动态添加/移除节点(trusted_peers)
- `stateless` pin tracker 适合大规模 pinset

**不推荐方案 C** 用于 S3 网关场景: 缺少 pin 协调导致数据可能丢失(某节点 unpin 后其他节点不一定有)，且无全局 pinset 可见性。

### 2.3 IPFS Cluster 关键配置点

- **共识组件**: `crdt`(默认，强最终一致，可动态加节点) vs `raft`(强一致，需半数以上在线)
- **`CLUSTER_SECRET`**: 所有 peer 共享的 32 字节 hex secret，控制集群准入
- **`CLUSTER_CRDT_TRUSTEDPEERS`**: 信任的 peer ID 列表，`'*'` 表示信任所有
- **`replication_factor_min/max`**: 最小/最大复制因子，`-1` 表示复制到所有节点
- **`CLUSTER_IPFSHTTP_NODEMULTIADDRESS`**: 指向本地 Kubo RPC 的多地址
- **Cluster Swarm 端口**: `9096/tcp`，用于 peer 间通信，多机部署必须暴露
- **Cluster REST API**: `9094/tcp`，管理接口

---

## 3. 元数据一致性策略

### 3.1 核心设计

**前提**: IPFS 内容寻址天然不可变(CID = hash(content))。S3 语义的「可变 key」通过**元数据层**实现——key 指向的是 CID，而不是内容本身。

```
S3 Key "photos/cat.jpg" → 元数据记录 { key, bucket, cid, size, content_type, version_id, created_at }
                         → CID "QmXxx..." → IPFS 内容(不可变)
```

### 3.2 读后写强一致实现

AWS S3 自 2020-12 起所有操作(GET/PUT/LIST/DELETE)均为强一致。我们的实现:

**方案: 共享外部元数据 DB (Postgres) + IPFS 不可变内容**

```
写入路径:
1. 网关收到 PutObject(key, data)
2. 将 data 写入 IPFS(kubo add) → 获得 CID
3. 在 Postgres 中 UPSERT 元数据: (bucket, key) → (cid, size, ...)
4. 返回 200

读取路径:
1. 网关收到 GetObject(key)
2. 从 Postgres 查询: SELECT cid FROM objects WHERE bucket=$1 AND key=$2
3. 从 IPFS 获取内容(kubo cat <cid>)
4. 返回内容
```

**为什么这是强一致的**:
- Postgres 的 ACID 事务保证: UPSERT 提交后，后续所有 SELECT 都看到最新值
- IPFS 内容不可变: CID 一旦产生就永远对应同一内容，不存在「读到旧版本内容」的可能
- 单 key 的读后写一致性由 DB 的 MVCC 天然保证

### 3.3 并发写同一 Key

多网关节点同时写同一个 key:

```
方案 1 (推荐): DB 层乐观锁
- 元数据表加 version 列(单调递增)
- UPDATE ... WHERE version = $old_version
- 冲突时返回 409 Conflict 或重试

方案 2: DB 层悲观锁
- SELECT ... FOR UPDATE 锁定行
- 但会阻塞其他写请求

方案 3: 唯一约束 + 幂等
- (bucket, key, version_id) 联合唯一约束
- 依赖 S3 语义(最后写入者胜)做 UPSERT
```

**推荐方案 1(乐观锁)**，符合 S3 语义且不阻塞读。

### 3.4 List 操作强一致

- `ListObjectsV2` 直接从 Postgres 查询，利用 DB 的 MVCC 快照隔离
- 无需额外机制即可满足「list 返回的是调用时刻的准确状态」
- 分页用 `ContinuationToken`(即 DB cursor/offset 的 opaque 编码)

### 3.5 元数据表结构要点

```sql
CREATE TABLE objects (
    id BIGSERIAL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL DEFAULT 'null',
    cid TEXT NOT NULL,
    size BIGINT NOT NULL,
    content_type TEXT,
    etag TEXT,
    metadata JSONB,
    is_delete_marker BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (bucket, key, version_id)
);

CREATE INDEX idx_objects_list ON objects (bucket, key_prefix, key)
    WHERE is_delete_marker = FALSE;
```

---

## 4. 状态协调方案

### 4.1 方案对比

| 维度 | OpenRaft (嵌入式 Raft) | 外部 Postgres | CRDT |
|------|------------------------|---------------|------|
| **一致性** | 强一致(线性化) | 强一致(ACID) | 最终一致 |
| **部署复杂度** | 中(需 3+ 节点，网络配置) | 低(单实例或托管) | 低 |
| **Dev 环境** | 单节点可运行但无 HA | 单容器即可 | 单节点可运行 |
| **运维负担** | 高(Leader 选举、日志压缩、快照) | 低(成熟运维生态) | 低 |
| **写入延迟** | 需 Quorum 确认 | 单 DB round-trip | 本地写入 |
| **冲突处理** | 无冲突(单 Leader 串行化) | 应用层处理(乐观锁) | CRDT 自动合并但语义复杂 |
| **Rust 生态** | openraft 成熟(Databend 生产使用) | sqlx/diesel 成熟 | autocommit/crdt_rs 较新 |

### 4.2 推荐

**Dev 环境(本地 docker compose)**: **外部 Postgres**(或 SQLite)。

理由:
- 最简路径: 1 个容器，无分布式协调代码
- 元数据一致性天然由 DB 保证
- 网关无状态，横向扩展只需加网关实例指向同一 DB
- openraft 在单节点 dev 场景过度设计

**生产环境**: **外部 Postgres (托管或 HA 部署)**。

理由:
- 网关节点无状态(元数据全在 DB)，横向扩展友好
- Postgres HA 方案成熟(Patroni/Cloud SQL/RDS)
- 避免在应用层引入分布式共识的复杂性
- openraft 适用于「需要嵌入式共识且不想依赖外部 DB」的场景(如 Danube 消息队列)，但 S3 网关已有 DB 依赖(元数据)，再引入 Raft 是多余的一层

**CRDT 不推荐**: S3 语义需要强一致(读后写、list 准确)，最终一致性不够。

### 4.3 状态边界清晰

```
┌─────────────────────────────────────────────┐
│  网关节点 (无状态)                            │
│  - S3 API 处理                               │
│  - 请求路由                                  │
│  - 无本地持久化状态                           │
└──────────────┬──────────────────────────────┘
               │
    ┌──────────┴──────────┐
    │                     │
    ▼                     ▼
┌──────────────┐   ┌──────────────┐
│  Postgres    │   │  Kubo RPC    │
│  (元数据)     │   │  (内容存储)   │
│  有状态       │   │  有状态       │
└──────────────┘   └──────────────┘
```

---

## 5. 可用性与故障转移

### 5.1 无状态网关的可行性: ✅ 可行且推荐

**架构**:
- 网关节点本身**无任何本地持久化状态**
- 所有 S3 元数据存储在外部 Postgres
- 所有内容数据存储在 Kubo(IPFS)
- 网关启动时只需知道 DB 连接串和 Kubo RPC 地址

**横向扩展**:
- 在负载均衡器(nginx/HAProxy/traefik)后加任意数量网关实例
- 无需 session stickiness(除非 S3 分段上传的 upload_id 需要路由到同一节点——但 upload 状态也存 DB，所以不需要)
- 节点发现: DNS-based(如 Docker Compose 的 service name 或 K8s Service)

**故障转移**:
- 网关挂了 → LB 自动切到其他实例
- Postgres 挂了 → 需 Postgres HA(生产)，dev 环境重启即可
- Kubo 挂了 → 单 Kubo 场景内容不可用，多 Kubo+Cluster 场景自动切换

### 5.2 推荐拓扑

```
                  ┌──────────────┐
                  │   LB / 反向代理 │
                  └──────┬───────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ 网关 #1   │  │ 网关 #2   │  │ 网关 #N   │
    └────┬─────┘  └────┬─────┘  └────┬─────┘
         │              │              │
         └──────────────┼──────────────┘
                        │
              ┌─────────┴─────────┐
              │                   │
              ▼                   ▼
        ┌──────────┐       ┌──────────┐
        │ Postgres │       │ Kubo(s)  │
        └──────────┘       └──────────┘
```

---

## 6. 本地 Docker Compose Dev 拓扑

### 6.1 最小可运行组合

```
┌─────────────────────────────────────────────────────┐
│                  Docker Network: ipfs-s3-dev         │
│                                                      │
│  ┌──────────────┐   ┌──────────┐   ┌──────────────┐ │
│  │ s3-gateway   │   │  kubo    │   │  postgres    │ │
│  │ (Rust)       │   │ (ipfs/   │   │ (postgres:16)│ │
│  │ port: 9000   │   │  kubo)   │   │ port: 5432   │ │
│  │              │   │          │   │              │ │
│  │ depends_on:  │   │ ports:   │   │ volumes:     │ │
│  │ - kubo       │   │  5001    │   │ pgdata:/var/ │ │
│  │ - postgres   │   │  8080    │   │ lib/postgres │ │
│  │              │   │          │   │              │ │
│  │ env:         │   │ volumes: │   │ healthcheck: │ │
│  │  KUBO_RPC=   │   │  ipfs_data│  │ pg_isready   │ │
│  │  http://     │   │ :/data/  │   │              │ │
│  │  kubo:5001   │   │  ipfs     │   │              │ │
│  │  DATABASE_   │   │          │   │              │ │
│  │  URL=postgres│   │ env:     │   │              │ │
│  │  ://postgres │   │  IPFS_   │   │              │ │
│  │  /ipfs_s3    │   │  SWARM_  │   │              │ │
│  │              │   │  KEY=... │   │              │ │
│  │              │   │  IPFS_   │   │              │ │
│  │ healthcheck: │   │  PROFILE │   │              │ │
│  │  HTTP GET    │   │  =server │   │              │ │
│  │  :9000/health│   │          │   │              │ │
│  │              │   │ health-  │   │              │ │
│  │              │   │ check:   │   │              │ │
│  │              │   │  ipfs    │   │              │ │
│  │              │   │  id      │   │              │ │
│  └──────────────┘   └──────────┘   └──────────────┘ │
│                                                      │
│  Volumes:                                            │
│    pgdata:    Postgres 数据持久化                      │
│    ipfs_data: Kubo repo(/data/ipfs) 持久化            │
└─────────────────────────────────────────────────────┘
```

### 6.2 服务清单

| 服务 | 镜像 | 端口 | 健康检查 | 关键配置 |
|------|------|------|----------|----------|
| **s3-gateway** | 自构建(Rust binary) | `9000` (S3 API) | `GET /health` → 200 | `KUBO_RPC=http://kubo:5001`, `DATABASE_URL=postgres://postgres:password@postgres/ipfs_s3` |
| **kubo** | `ipfs/kubo:release` (或 `v0.32+`) | `5001` (RPC API), `8080` (Gateway) | `ipfs id` 成功退出 | `IPFS_PROFILE=server`, `IPFS_SWARM_KEY=<生成的 swarm key>` (私有网络), `LIBP2P_FORCE_PNET=1` |
| **postgres** | `postgres:16-alpine` | `5432` | `pg_isready -U postgres` | `POSTGRES_DB=ipfs_s3`, init script 建表 |

### 6.3 依赖关系

```
postgres ← depends_on (健康) ← s3-gateway
kubo     ← depends_on (健康) ← s3-gateway
```

### 6.4 卷映射

- `pgdata:/var/lib/postgresql/data` — Postgres 数据持久化，compose down 后数据保留
- `ipfs_data:/data/ipfs` — Kubo 仓库(blocks, datastore, config, swarm.key)，不丢失 pin 的内容

### 6.5 关键配置点

**Kubo 容器**:
- `IPFS_SWARM_KEY` 环境变量注入 swarm.key 内容(私有网络隔离)
- `IPFS_PROFILE=server` 优化服务器场景
- CORS 配置(如网关需要浏览器直连): `ipfs config --json API.HTTPHeaders.Access-Control-Allow-Origin '["*"]'`
- 可选: `/container-init.d/` 挂载初始化脚本(自动配置 bootstrap、CORS)

**Postgres 容器**:
- 通过 `/docker-entrypoint-initdb.d/` 挂载 SQL 初始化脚本(建表、索引)
- 使用 named volume 持久化

**网关容器**:
- 等待 kubo 和 postgres 健康后再启动(`depends_on` + `condition: service_healthy`)
- 启动时自动运行 DB migration(如 sqlx migrate)

### 6.6 扩展到多节点(生产模拟)

加第二个网关实例(水平扩展):

```
s3-gateway-2:
  (同 gateway-1 配置，仅端口不同如 9001:9000)
  depends_on: [postgres, kubo]
```

加第二个 Kubo + IPFS Cluster(方案 A):

```
kubo-2:
  (同 kubo 配置，不同端口映射)
  depends_on: [kubo]  # bootstrap from kubo

cluster-0:
  image: ipfs/ipfs-cluster:latest
  depends_on: [kubo]
  environment:
    CLUSTER_SECRET: ${CLUSTER_SECRET}
    CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo/tcp/5001
    CLUSTER_CRDT_TRUSTEDPEERS: '*'

cluster-1:
  image: ipfs/ipfs-cluster:latest
  depends_on: [kubo-2]
  environment:
    CLUSTER_SECRET: ${CLUSTER_SECRET}
    CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-2/tcp/5001
    CLUSTER_CRDT_TRUSTEDPEERS: '*'
  command: daemon --bootstrap /dns4/cluster-0/tcp/9096/p2p/<cluster0_peer_id>
```

---

## 7. 部署形态

### 7.1 单机多容器 (Dev)

- 所有服务(docker-compose)在同一台机器上
- Kubo 数据卷绑定到宿主机目录，重启不丢数据
- 网关通过 Docker 内部 DNS 连接 `kubo:5001`

### 7.2 多机 (Prod)

- 每台机器运行: 网关 + Kubo + (可选) IPFS Cluster
- Postgres 独立部署(托管或自建 HA)
- Kubo 数据卷: 使用本地 SSD volume 或网络存储
- Bootstrap 配置:
  - 私有网络: 所有节点共享 swarm.key
  - 指定至少一个稳定的 bootstrap 节点: `ipfs bootstrap add /ip4/<bootstrap-ip>/tcp/4001/p2p/<peer-id>`
  - 移除公共 bootstrap: `ipfs bootstrap rm --all`
- 网关连 Kubo RPC: 通常同机部署，用 `localhost:5001` 或 Docker 内部 DNS

### 7.3 Kubo 端口一览

| 端口 | 协议 | 用途 | 暴露策略 |
|------|------|------|----------|
| `4001` | TCP/UDP | Swarm(peer 间通信) | 多机需暴露，单机可选 |
| `5001` | TCP | RPC API(管理接口) | **仅内部网络**，绝不暴露公网 |
| `8080` | TCP | HTTP Gateway | 可选，dev 时暴露 |

---

## 8. 风险清单

### 8.1 离线/无外网运行

**结论: ✅ 可以完全离线运行。**

实现方式:
1. **生成 swarm.key**(32 字节随机 hex)，注入所有 Kubo 容器
2. 设置 `LIBP2P_FORCE_PNET=1` 环境变量，强制私有网络模式
3. 移除所有公共 bootstrap 节点: `ipfs bootstrap rm --all`
4. 配置内部 bootstrap: 以第一个 Kubo 节点为 bootstrap 源
5. Kubo 在私有模式下**完全不连接公网 IPFS 节点**，只与共享同一 swarm.key 的 peer 通信

IPFS Cluster 在离线环境的运行: 同样只需共享 `CLUSTER_SECRET`，Cluster peer 间通过内部网络通信，无需外网。官方 docker-compose.yml 示例即是纯内部网络运行。

### 8.2 风险矩阵

| 风险 | 严重度 | 概率 | 缓解措施 |
|------|--------|------|----------|
| **S3 API 兼容性不足** | 高 | 高 | 先实现核心操作(Put/Get/Head/Delete/ListObjectsV2)，逐步覆盖。参考 SynapS3 的兼容矩阵。签名用 SigV4 |
| **大文件性能** | 中 | 中 | IPFS 默认 chunk 256KB，UnixFS DAG 有开销。大文件用 S3 Multipart Upload → IPFS MFS 分段写入。考虑直接存 raw block(不经过 UnixFS) |
| **Kubo 单点故障** | 高 | 中 | Dev: 可接受。Prod: 用 IPFS Cluster 多 Kubo 副本 |
| **Postgres 单点故障** | 高 | 中 | Dev: 可接受。Prod: 用托管 Postgres 或 Patroni HA |
| **IPFS GC 误删数据** | 中 | 低 | 所有写入内容必须 pin(通过 Cluster 或手动)。禁用 Kubo 自动 GC 或设置为极长间隔 |
| **CID v0/v1 兼容** | 低 | 中 | 统一使用 CIDv1(raw 或 dag-pb)，S3 key 映射到 CID 关系清晰 |
| **S3 分段上传一致性** | 中 | 中 | 分段上传状态存 Postgres(非本地内存)，网关可水平扩展 |
| **swarm.key 泄露** | 高 | 低 | 私有网络内容可被拥有 swarm.key 的任意节点访问。用 Docker secrets 管理 |
| **IPFS 网络延迟** | 中 | 中 | 同机部署网关+Kubo，RPC 走 localhost，延迟极低 |
| **项目复杂度膨胀** | 中 | 中 | 借鉴 Textile 教训，保持架构简单: 网关 + DB + Kubo，不引入额外抽象层 |

### 8.3 关键确认项

- [x] IPFS Cluster 在离线 Docker 环境可用(官方 docker-compose.yml 即纯内部网络)
- [x] 私有 swarm key 在离线环境可用(`LIBP2P_FORCE_PNET=1` + swarm.key)
- [x] Dev 环境可完全离线运行(私有 swarm + 内部 bootstrap + 无公网依赖)
- [x] S3 强一致可通过「Postgres + IPFS 不可变内容」组合实现
- [x] 无状态网关水平扩展可行(元数据全在 DB，内容在 IPFS)

---

## 9. 架构建议总结

### 推荐架构(Dev)

```
单机 docker compose:
  - 1x Rust S3 Gateway (无状态)
  - 1x Kubo (ipfs/kubo:release)
  - 1x Postgres 16
  - 私有 swarm (swarm.key), 完全离线可运行
```

### 推荐架构(Prod 最小)

```
多机部署:
  - Nx Rust S3 Gateway (无状态, 水平扩展)
  - Nx Kubo + IPFS Cluster (有状态, 数据冗余)
  - 1x Postgres HA (托管或自建)
  - 私有 swarm, 内部 bootstrap
  - LB 前置
```

### 下一步

1. 实现最小 S3 API 子集(PutObject, GetObject, HeadObject, DeleteObject, ListObjectsV2)
2. 对接 Kubo RPC(`/api/v0/add`, `/api/v0/cat`, `/api/v0/pin/add`, `/api/v0/pin/rm`)
3. 实现 Postgres 元数据层
4. 编写 docker-compose.yml(按第 6 节拓扑)
5. S3 客户端兼容性测试(awscli, s3cmd, minio mc)

---

## 参考来源

- [aricanduva](https://github.com/bltavares/aricanduva) — S3 to IPFS proxy
- [s3x](https://github.com/RTradeLtd/s3x) — MinIO gateway for IPFS (dead)
- [MinIO Gateway Deprecation](https://blog.min.io/deprecation-of-the-minio-gateway/) — 2022-02
- [MinIO Gateway Removal PR](https://github.com/minio/minio/pull/15929) — 2022-10
- [ipfs_kit_py](https://github.com/endomorphosis/ipfs_kit_py) — Python IPFS toolkit
- [zs3](https://github.com/Lulzx/zs3) — S3-compatible in Zig
- [SynapS3](https://github.com/strahe/SynapS3) — S3 for Filecoin
- [Filebase IPFS Docs](https://filebase.com/docs/ipfs/overview) — Commercial S3+IPFS
- [Pinata](https://pinata.cloud/) — IPFS pinning + CDN
- [Textile Hub Shutdown](https://github.com/textileio/textile/issues/578) — 2023-01
- [IPFS Cluster Architecture](https://ipfscluster.io/documentation/deployment/architecture/)
- [IPFS Cluster Consensus](https://ipfscluster.io/documentation/guides/consensus/)
- [IPFS Cluster docker-compose.yml](https://github.com/ipfs/ipfs-cluster/blob/master/docker-compose.yml)
- [Kubo Docker - Private Swarms](https://docs.ipfs.tech/install/run-ipfs-inside-docker/#private-swarms-inside-docker)
- [S3 Strong Consistency](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) — 2020-12
- [Diving Deep on S3 Consistency](https://luciansystems.com/diving-deep-on-s3-consistency/)
- [openraft](https://github.com/databendlabs/openraft) — Rust Raft consensus
- [Danube: Replacing ETCD with openraft](https://dev-state.com/posts/migrate_danube_etcd_to_raft/)
- [IPFS Cluster Setup](https://docs.ipfs.tech/install/server-infrastructure/)
- [Private IPFS Cluster Guide](https://cestoliv.com/blog/using-ipfs-for-data-replication/en)
