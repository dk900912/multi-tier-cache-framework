# AGENTS.md

本文件是本仓库的 Codex 代理指南。它按 OpenAI Codex 官方指南的建议维护：内容应短而准确，覆盖仓库结构、运行命令、工程约束、禁止事项、验证标准，以及本项目特有的一致性契约。

## 项目定位

`multi-tier-cache-framework` 是一个面向 Java 21 的 L1 + L2 多级缓存框架，目标是在高并发场景中提供低延迟读、缓存击穿保护、缓存穿透防护、乱序写防御和最终一致性能力。

整体架构：

- `L1`：进程内缓存，是可丢弃的本地副本，用于热点加速。
- `L2`：远端 Redis 缓存，是跨节点共享状态和版本裁决点。
- `Pub/Sub`：只作为跨节点 L1 失效加速器，不是可靠消息系统。
- `DB`：业务数据的 Root of Trust。缓存变更必须发生在 DB insert/update/delete 成功之后。

## 仓库结构

- `cache-api`：对外 API、配置模型、消息模型、SPI。
- `cache-core`：核心读写路径、`DefaultCacheManager`、`SingleFlight`、Lua 脚本调度、版本比较、补偿重放。
- `cache-codec`：默认 Jackson 编解码，包含反序列化白名单。
- `cache-provider-l1-jdk`：JDK Map L1 Provider。
- `cache-provider-l1-guava`：Guava L1 Provider。
- `cache-provider-l1-caffeine`：Caffeine L1 Provider，生产推荐优先考虑。
- `cache-provider-l2-jedis`：Jedis L2 Provider。
- `cache-provider-l2-lettuce`：Lettuce L2 Provider。
- `cache-provider-l2-redisson`：Redisson L2 Provider。
- `start-redis-cluster.sh` / `stop-redis-cluster.sh`：本地 Redis Cluster 测试环境脚本。

## 常用命令

优先使用 Maven 命令验证改动：

```powershell
mvn test -DskipITs
```

针对 core 模块的快速验证：

```powershell
mvn -pl cache-core -am test "-Dsurefire.failIfNoSpecifiedTests=false"
```

运行指定测试类：

```powershell
mvn -pl cache-core -am test "-Dtest=DefaultCacheManagerWriteOrderingTest,DefaultCacheManagerObservabilityTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

集成测试依赖本地 Redis Cluster：

- 需要 `127.0.0.1:7001`、`127.0.0.1:7002`、`127.0.0.1:7003` 可达。
- 当前测试配置使用 ACL 用户 `dk900912`，密码 `qwe@1234`。
- 可用 `bash start-redis-cluster.sh` 启动测试集群，用 `bash stop-redis-cluster.sh` 停止。

如果本机 Maven 需要固定本地仓库，可在命令后追加：

```powershell
"-Dmaven.repo.local=C:\Users\dk900\.m2\repository"
```

## 核心一致性契约

本框架采用 AP 倾向，不承诺 L1 与 L2 强一致，但必须提供最终一致性。

必须维持以下语义：

- `L2` 是权威裁决点；当 L2 启用时，写路径必须先让 L2 接受，再更新当前节点 L1。
- `L1` 是本地副本，不能绕过 L2 版本裁决缓存未被接受的新值。
- 远端 Pub/Sub 通知只负责让已有 L1 失效，不应把远端 payload 当作可靠预热数据写入空 L1。
- `SingleFlight` 只防止同 key 并发回源，不应把慢 loader 的首个请求误判为递归加载。
- 读路径遇到 L2 读故障时应降级到 loader；loader 成功后，若 L2 read-backfill 失败，业务读仍应返回 loader 结果，但在 L2 启用时不得把该结果写入 L1。
- 删除必须写入 `DELETE` tombstone；tombstone 在 TTL 内必须阻止旧版本回填和 DB reload。
- `PENETRATE(version=-1)` 是低优先级穿透哨兵，不能覆盖真实值或删除墓碑。

## 版本与生命周期

版本号由业务方指定，框架不生成业务版本。业务方必须保证同一条 DB 记录从插入到彻底删除的完整生命周期内，版本严格单调递增。

典型来源：

- 高精度 `updated_time`。
- 乐观锁自增字段，例如 `version = version + 1`。

写操作语义必须保持清晰：

- `insert` 表示业务方已经向 DB 新增一条记录。
- `update` 表示业务方已经更新 DB 中一条已有记录。
- `evict` 表示业务方已经删除 DB 中一条记录。
- 不要把业务 upsert 混成框架语义；调用方应明确选择 insert、update 或 evict。

同一个 cache key 不支持 DB 删除后以重置版本复活。业务记录删除后再次新增，必须使用新的 DB 主键，并形成新的 cache key。

## Lua 与 Redis 规则

涉及 `CacheLuaScripts`、`CacheMessageVersionComparator`、`DefaultCacheManager` 时必须同步思考 L1、L2、Lua 三处语义，不要只修一处。

Lua 脚本要求：

- 原子比较当前消息与 incoming 消息。
- 只接受版本和状态优先级更高的消息。
- 拒绝旧版本、重复消息、低优先级穿透哨兵、被 delete tombstone 阻挡的同 key 复活。
- 对 TTL、ARGV、KEYS 的数量和含义保持清晰，新增字段必须同步 Provider 测试和 core 测试。
- 保持 Redis Cluster 兼容，避免跨 slot 多 key 操作；当前数据 key 应作为核心裁决 key。

## 编码约束

- 优先遵循现有模块边界和 SPI 形态，不为单个 bug 引入大范围抽象。
- `cache-api` 的模型和接口变更会影响全部 provider，必须同步所有编译错误和测试。
- `cache-core` 的读写路径改动必须考虑 L1 only、L2 only、L1 + L2 三种模式。
- 修改序列化模型时必须同步 `cache-codec` 测试，注意 Jackson 反序列化白名单安全。
- 不要移除乱序、重复、旧版本、delete tombstone、penetration、read-backfill race 的防御测试。
- 不要把 Redis Pub/Sub 当作可靠投递机制设计。
- 不要引入需要外部服务的新生产依赖，除非用户明确同意。

## 测试要求

按风险选择测试范围：

- API/model 变更：至少跑 `cache-api`、`cache-codec`、`cache-core` 相关测试。
- `DefaultCacheManager`、版本比较器、Lua 脚本变更：必须跑 core 目标测试，并优先覆盖读路径、写路径、乱序和最终一致性。
- Provider 变更：跑对应 provider 模块测试。
- 跨节点一致性、Redis Lua、Pub/Sub 行为变更：跑 `CacheManagerIntegrationTest`；Redis 不可达时要明确说明集成测试被跳过。
- 任务完成前尽量跑 `mvn test -DskipITs`，无法运行时说明原因。

测试应尽量通过公共 API 和可观测指标验证行为。例如验证只使用 L1/L2 时，优先断言 `CacheRuntimeStats` 的 L1/L2 hit、miss、apply、failure 指标，而不是只看内部 map 状态。

## Review 重点

代码审查时优先找以下风险：

- L1 保存了未经过 L2 裁决的值。
- L2 故障导致业务读失败，而本应降级。
- 旧版本 backfill、PENETRATE 或重复消息覆盖了新值或 tombstone。
- Pub/Sub 消息丢失、乱序或重复时无法最终收敛。
- L1 only 模式丢失 tombstone/version 防线。
- L2 only 模式仍误触 L1。
- Lua 脚本参数顺序、返回值、TTL 或 Redis Cluster slot 假设不稳。
- 指标与真实路径不一致，导致线上无法判断实际命中层级。

## 完成标准

一次改动完成前，至少确认：

- 行为符合 DB 驱动、版本单调、最终一致性契约。
- 相关测试已新增或更新。
- 目标测试通过。
- 若改动影响共享核心逻辑，全量测试通过或明确说明未跑原因。
- 最终回复说明改动文件、验证命令和结果，不夸大强一致语义。

## 官方指南依据

OpenAI Codex 官方指南建议把 `AGENTS.md` 用作仓库级、可复用的代理说明，覆盖 repo layout、运行方式、build/test/lint 命令、工程约定、禁止事项，以及完成和验证标准。维护本文件时应保持实用、简洁、贴近真实重复摩擦；当代理反复犯同类错误时，再把稳定规则沉淀进来。
