# 多级缓存框架 (Multi-Tier Cache Framework)

一个专为 Java 应用程序设计的高性能、健壮且高度可扩展的多级缓存框架。

## 1. 接入指南 (Introduction & Integration Guide)

本框架提供了一套完整的多级缓存解决方案（L1 本地缓存 + L2 分布式缓存），旨在解决常见的缓存痛点，例如缓存穿透、缓存击穿（通过内置的 `SingleFlight` 机制防范），以及分布式节点间的缓存一致性（通过 Redis Pub/Sub 广播 L1 失效）。

**新特性声明：全面支持 Redis 7.0 的 ACL（用户名+密码）安全认证机制！**

### Maven 依赖

项目采用了 BOM (Bill of Materials) 结构。你需要引入 `cache-core` 核心模块，并根据需求挑选你喜欢的 L1 和 L2 缓存提供者实现。

```xml
<dependencies>
    <!-- 核心框架 -->
    <dependency>
        <groupId>io.github.dk900912</groupId>
        <artifactId>cache-core</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

    <!-- 选择一个 L1 Provider（例如 Caffeine） -->
    <dependency>
        <groupId>io.github.dk900912</groupId>
        <artifactId>cache-provider-l1-caffeine</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

    <!-- 选择一个 L2 Provider（例如 Lettuce） -->
    <dependency>
        <groupId>io.github.dk900912</groupId>
        <artifactId>cache-provider-l2-lettuce</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### 极简初始化与使用示例

```java
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.core.CacheManagerFactory;
import java.time.Duration;
import java.util.List;

public class CacheExample {
    public static void main(String[] args) {
        CacheConfig config = new CacheConfig();
        
        // 配置 L1 本地缓存
        config.getL1().setEnabled(true);
        config.getL1().setMaximumSize(10_000L);
        config.getL1().setExpireAfterWrite(Duration.ofMinutes(10));
        
        // 配置 L2 分布式缓存
        config.getL2().setEnabled(true);
        config.getL2().setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.getL2().setMutationChannelName("cache:mutation");
        
        // 可选：配置 Redis 7.0 ACL 认证
        // config.getL2().setUsername("default");
        // config.getL2().setPassword("your-password");
        
        // 创建并启动 CacheManager
        CacheManager cacheManager = CacheManagerFactory.create(config);
        cacheManager.bootstrap();
        
        // 核心使用示例
        String value = cacheManager.get(
            () -> "user:1", 
            () -> "Value From DB", 
            Duration.ofMinutes(30)
        );
        
        System.out.println("Cached Value: " + value);
        
        // 安全关闭，释放底层线程和资源
        cacheManager.shutdown();
    }
}
```

## 2. 模块间依赖拓扑架构 (Module Dependency Topology)

```mermaid
graph TD
    api[cache-api<br>接口与模型]
    codec[cache-codec<br>Jackson 序列化]
    core[cache-core<br>CacheManager & 核心同步逻辑]
    
    l1_jdk[cache-provider-l1-jdk]
    l1_guava[cache-provider-l1-guava]
    l1_caffeine[cache-provider-l1-caffeine]
    
    l2_jedis[cache-provider-l2-jedis]
    l2_lettuce[cache-provider-l2-lettuce]
    l2_redisson[cache-provider-l2-redisson]

    core --> api
    codec --> api
    core --> codec
    
    l1_jdk -. 实现 .-> api
    l1_guava -. 实现 .-> api
    l1_caffeine -. 实现 .-> api
    
    l2_jedis -. 实现 .-> api
    l2_lettuce -. 实现 .-> api
    l2_redisson -. 实现 .-> api
```

## 3. 拓展点与灵活定制 (Extension Points)

本框架利用 Java SPI (Service Provider Interface) 机制实现了极高的可扩展性。你可以实现以下核心接口，并将其声明在项目的 `META-INF/services/` 目录下：

- **`L1Provider` (本地缓存扩展)**
  - 接口: `io.github.dk900912.multitiercache.spi.L1Provider`
  - 默认提供: `CaffeineL1Provider`, `GuavaL1Provider`, `JdkL1Provider`.
  - 使用场景: 接入你自定义的进程内缓存组件，或者实现特定的驱逐算法。
  - *注意：框架提供的高级特性 `FineGrainedExpiry` (细粒度过期) 目前仅在配置为 Caffeine 时生效。*

- **`L2Provider` (分布式缓存扩展)**
  - 接口: `io.github.dk900912.multitiercache.spi.L2Provider`
  - 默认提供: `LettuceL2Provider`, `RedissonL2Provider`, `JedisL2Provider`.
  - 使用场景: 对接其他的分布式缓存系统（如 Memcached，或企业内部深度定制的 Redis 客户端）。

- **`CacheMessageRepository` (缓存消息补偿机制扩展)**
  - 接口: `io.github.dk900912.multitiercache.api.CacheMessageRepository`
  - 默认提供: `DefaultCacheMessageRepository` (仅打印日志，不落盘).
  - 使用场景: 当 Pub/Sub 网络抖动导致 L1 失效消息丢失时，通过扩展该接口对接数据库（JDBC、MongoDB）或 MQ，框架后台的 `CacheMessageReplayer` 将自动进行消息补偿，以保证最终一致性。

## 4. 核心业务时序图 (Core Business Sequence Diagram)

### 4.1 `get` 获取操作（带有 SingleFlight 防击穿保护）

```mermaid
sequenceDiagram
    participant Client
    participant CacheManager
    participant L1Cache
    participant L2Cache
    participant SingleFlight
    participant Database

    Client->>CacheManager: get(key, loader)
    CacheManager->>L1Cache: readFromL1(key)
    L1Cache-->>CacheManager: Miss (未命中)
    
    CacheManager->>L2Cache: readFromL2(key)
    L2Cache-->>CacheManager: Miss (未命中)
    
    CacheManager->>SingleFlight: execute(key)
    Note over SingleFlight: 阻塞其他并发请求<br/>保证只有一个线程去查库
    
    SingleFlight->>CacheManager: loadAsSingleFlightOwner
    
    Note over CacheManager: 获得执行权后进行双重检查(Double Check)
    CacheManager->>L1Cache: readFromL1(key, quiet=true)
    L1Cache-->>CacheManager: Miss
    CacheManager->>L2Cache: readFromL2(key, quiet=true)
    L2Cache-->>CacheManager: Miss
    
    CacheManager->>Database: loader.load() (回源查库)
    Database-->>CacheManager: 获得数据及版本号
    
    CacheManager->>L2Cache: 回填 L2 (设定 L2 TTL)
    CacheManager->>L1Cache: 回填 L1 (L1过期受本地配置管控)
    
    CacheManager-->>SingleFlight: 返回结果
    SingleFlight-->>Client: 唤醒并返回数据给所有被阻塞的线程
```

### 4.2 `update` 修改操作（带有 Pub/Sub 广播失效机制）

```mermaid
sequenceDiagram
    participant Client
    participant DB
    participant CacheManager
    participant L1Cache_NodeA
    participant L2Cache_Redis
    participant L1Cache_NodeB

    Client->>DB: 更新数据库数据
    DB-->>Client: 成功
    
    Client->>CacheManager: update(key, data, version, ttl)
    
    CacheManager->>L1Cache_NodeA: invalidate(key) (主动清除本地L1)
    
    CacheManager->>L2Cache_Redis: eval(UPSERT_LUA_SCRIPT)
    Note over L2Cache_Redis: Lua 脚本以原子方式:<br/>1. 更新 L2 负载<br/>2. 向指定 Channel PUBLISH 失效消息
    
    L2Cache_Redis-->>L1Cache_NodeB: Pub/Sub 广播 (Channel Message)
    Note over L1Cache_NodeB: 其他节点监听器收到变更通知
    L1Cache_NodeB->>L1Cache_NodeB: invalidate(key) (清除本地L1)
    
    L2Cache_Redis-->>CacheManager: 成功
    CacheManager-->>Client: 成功
```

## 5. 核心配置参考 (Configuration)

本框架的所有配置均封装在 `CacheConfig` 模型中，以下是各子配置项的含义及默认值参考。

### L1Config (一级缓存配置)

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | 是否启用一级本地缓存 | `true` |
| `recordStats` | `boolean` | 是否开启本地缓存的统计信息记录 | `false` |
| `maximumSize` | `Long` | 一级缓存允许存放的最大条目数 | `1000` |
| `expireAfterWrite` | `Duration` | 写入后的全局过期时间 | `null` |
| `expireAfterAccess` | `Duration` | 最后一次访问后的全局过期时间 | `null` |
| `fineGrainedExpiry`| `FineGrainedExpiry` | 细粒度过期策略接口实例（**注意：仅 Caffeine 生效**）| `null` |

### L2Config (二级缓存配置)

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | 是否启用二级分布式缓存 | `true` |
| `mutationChannelName`| `String` | 用于广播缓存失效变更的 Pub/Sub 频道名称 | `null` (必须配置) |
| `hosts` | `List<String>` | Redis 集群节点地址列表 (例如 "127.0.0.1:6379") | `null` (必须配置) |
| `maxTotal` | `Integer` | 连接池最大活跃连接数 | `20` |
| `maxIdle` | `Integer` | 连接池最大空闲连接数 | `5` |
| `minIdle` | `Integer` | 连接池最小空闲连接数 | `1` |
| `maxWait` | `Duration` | 从连接池获取连接的最大等待时间 | `2000ms` (2s) |
| `connectionTimeout`| `Duration` | Redis 连接超时时间 | `2000ms` (2s) |
| `socketTimeout` | `Duration` | Redis Socket 读写超时时间 | `2000ms` (2s) |
| `maxRedirects` | `Integer` | 集群模式下的最大重定向次数 | `5` |
| `username` | `String` | Redis 7.0 ACL 认证用户名 | `null` |
| `password` | `String` | Redis 认证密码 | `null` |

### Subscriber (Pub/Sub 订阅者线程池配置)
*嵌套在 `L2Config` 中，用于控制接收失效消息时的异步处理线程池。*

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `corePoolSize` | `int` | 核心线程数 | `1` |
| `maximumPoolSize` | `int` | 最大线程数 | `1` |
| `keepAliveTime` | `Duration` | 非核心线程的闲置超时时间 | `0` |
| `capacity` | `int` | 任务阻塞队列容量 | `100` |

### SingleFlight (并发加载保护配置)

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `awaitTimeout` | `Duration` | 缓存击穿时，等待持有锁的线程返回结果的最大超时时间 | `5000ms` (5s) |

### Compensation (本地消息补偿机制配置)

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `initialDelay` | `Duration` | 补偿后台任务启动的初始延迟时间 | `10000ms` (10s) |
| `period` | `Duration` | 补偿后台任务的执行间隔周期 | `10000ms` (10s) |
| `batchSize` | `int` | 每批次从 DB/MQ 抓取并处理的未同步消息最大数量 | `100` |

### CacheMiss (缓存未命中与穿透处理配置)

| 字段 | 类型 | 含义 | 默认值 |
| :--- | :--- | :--- | :--- |
| `penetrationTtl` | `Duration` | 发生缓存穿透（回源结果为 null）时，空值墓碑的存活时间 | `60000ms` (1m) |
| `backfillTtl` | `Duration` | 正常缓存未命中并成功回源后，回填缓存的 TTL | `60000ms` (1m) |
| `defaultTtl` | `Duration` | 未显式指定 TTL 时的默认缓存时长 | `60000ms` (1m) |

## 6. 架构哲学与 FAQ (Architecture Philosophy & FAQ)

### 6.1 本组件是否支持同一 Key 的全局有序消费？

**目前尚不支持！**

在分布式系统中，由于网络延迟抖动、消费者节点的处理速度差异等不可控因素，缓存同步消息的到达顺序极有可能与数据库事件的实际发生顺序不一致（即**消息乱序**）。如果不从框架底层强制保证同一 Key 的有序性，在对数据一致性要求极高的场景下，就会引发致命的“旧数据覆盖新数据”问题。

#### 💡 场景还原：乱序导致的“奇怪现象”

以电商核心的“库存管理”场景为例，假设缓存中维护了三个关键字段：`stock`（总库存）、`locked`（锁定库存）、`available`（可用库存，即 stock - locked）。

* **初始状态 (T0)**：`stock=10 | locked=0 | available=10`
* **事件 T1（用户下单，锁定库存）**：数据库更新为 `stock=10 | locked=10 | available=0`。系统发出同步缓存消息 **M1 (v1)**。
* **事件 T2（用户支付成功，扣减总库存并释放锁定）**：数据库再次更新为 `stock=0 | locked=0 | available=0`。系统发出同步缓存消息 **M2 (v2)**。

**正常情况（有序到达）**：M1 先到，M2 后到。缓存经历 T1 后最终停留在 T2 的状态：`stock=0 | locked=0 | available=0`，与数据库完美一致。

**异常情况（无序消费下的乱序到达）**：
如果由于网络抖动，**M2 比 M1 先被消费者处理**，就会出现以下诡异的现象：
1. **M2 先到达**：缓存被正确更新为 T2 的最新状态（`stock=0 | locked=0 | available=0`）。
2. **M1 姗姗来迟**：缓存被错误地“回退”到了 T1 的历史状态（`stock=10 | locked=10 | available=0`）。

此时，**灾难发生了**：数据库中的商品其实已经全部卖光并结算完毕，但在 L2 缓存中，系统却认为还有 10 个库存处于“被锁定”的**僵尸状态**！这种数据不一致会导致后续的对账逻辑完全错乱，甚至引发系统阻塞。

#### 🛡️ 当前的规避方案：基于版本号的乐观控制

为了解决上述乱序带来的脏数据问题，目前组件**强制要求业务方在设计缓存数据时，指定严格单调递增的数据版本号**（例如取自数据库的 `updated_time` 或是自增的 `version` 字段，配合 `@CacheVersion` 注解使用）。

在更新 L2 缓存时，组件会进行版本号比对（通过 Redis Lua 脚本保证原子性）：
* 只有当 **接收到的消息版本号 > 当前缓存中的版本号** 时，才允许执行更新。
* **带入上述场景**：M2（携带 v2）先到达，缓存更新为 v2 状态；随后 M1（携带 v1）到达，组件发现当前缓存已经是 v2，`v1 < v2`，于是**直接丢弃 M1 的更新操作**。这样就完美避免了旧版本覆盖新版本的问题。

#### 🚀 未来的演进思路：基于 Redis Stream 的 Hash 路由

如果要从框架层面彻底免除业务方维护版本号的负担，真正实现同一 Key 的严格有序消费，未来的演进方向是抛弃普通的 Pub/Sub，转向 **Redis Stream** 或专用的 MQ：
1. **一致性路由**：拦截同一 Key 的 insert/update/delete 事件，通过 `mod(hash(key), partition_size)` 算法，将其路由到固定的 Stream 分区（或 Topic Queue）。
2. **单线程消费**：确保同一个分区的消息由单一消费者线程按序拉取并处理。
3. 通过这种“物理隔离+串行处理”的方式，在底层架构上保证同一 Key 的消费顺序严格一致。

---

### 6.2 本组件是否支持缓存过期主动加载 (Refresh-Ahead)？

**不支持。** 
目前框架采用的是纯粹的被动加载模式（Cache-Aside），即缓存过期后，只有当真实流量访问并发生 Cache Miss 时，才会触发 SingleFlight 去回源加载。

---

### 6.3 本组件是否支持防缓存击穿？

**支持！本组件内置了基于 JVM 级别的单机 SingleFlight（单飞）保护机制。**

当面临突发的并发洪峰且 L1/L2 均未命中时，单机 SingleFlight 会确保**同一个微服务节点内，针对同一个 Key，只会有一个线程放行去执行回源查询（查 DB）**，其余被拦截的线程会短暂挂起，等待首个线程获取结果并回填缓存后，直接共享该结果并返回。

#### 🛡️ 为什么不做到“全局唯一回源”（跨多节点的分布式 SingleFlight）？

如果到了“只允许多节点集群中存在一个线程针对同一 Key 的数据进行回源查询”这个地步，**可能业务方的系统架构或数据库层面本身已经存在极大的不合理之处。**

本框架故意不提供基于分布式锁的全局防击穿，主要基于以下架构考量：

1. **单机防击穿已经“足够安全”**
   假设你的服务部署了 100 个节点，面对 10 万 QPS 的突发洪峰。在单机 SingleFlight 的保护下，最极端的并发回源量也就仅仅是 **100 次**。对于绝大多数现代关系型数据库（MySQL/PostgreSQL 等）来说，瞬间处理 100 个针对同一个主键或索引的简单并发读查询，简直是轻而易举，根本不会导致数据库雪崩。
   
2. **引入分布式锁是典型的“过度设计”且损害性能**
   为了拦截这区区几十上百个并发读，如果在缓存框架中引入 Redis 分布式锁或 Zookeeper，这就意味着每一次缓存未命中，都要增加至少 1-2 次额外的网络 RTT（去获取和释放锁）。这完全违背了引入缓存是为了**极致提升读性能**的初衷，而且大大增加了系统发生死锁或外部依赖故障的风险。
   
3. **不要让缓存框架掩盖 DB 的真实病灶**
   如果数据库连几十个节点的并发读请求都扛不住，那真正的问题绝对不是缓存击穿，而是：
   * 回源查询的 SQL 是一条极其消耗 CPU 的慢查询（例如缺少索引、大表 Join、深度分页）。
   * 数据库本身的配置或硬件资源已经到了物理极限，需要考虑读写分离或分库分表。

**总结**：本组件坚持“小而美且高效”的防御策略——**用最轻量的本地锁保护应用免受十万级并发洪峰的摧毁，同时允许合理范围内（节点数级别）的并发去试探 DB 的底线。**

#### 💡 拓展建议：如何实现“全局唯一回源”？

虽然框架出于轻量化考量未内置分布式锁，但**本缓存组件已经为您预留了完美的拓展点：`CacheLoader` 接口**。

如果您确实有极高的一致性要求或 DB 极为脆弱，完全可以在您实现的 `CacheLoader.load()` 逻辑中，自行包裹一层分布式锁（例如 Redisson Lock）。由于框架层已经做好了第一道防线（单机 SingleFlight），这会带来一个极大的架构优势：**分布式锁的竞争压力会被成百上千倍地削弱！**

在洪峰到来时，无论有多少并发请求，每个 JVM 节点最多只会有一个线程去竞争分布式锁。

**最佳实践伪代码：**
1. 框架单机 SingleFlight 拦截了 99.9% 的并发线程。
2. 少数代表各个节点的线程进入您的 `CacheLoader.load()`。
3. 尝试获取分布式锁。
4. **获取锁成功后，务必进行 Double-Check（再次直接查询一次 L2 缓存）**，因为在等待锁的期间，其他节点可能已经完成了查询并将数据回填到了 L2。
5. 若 L2 依然为空，再执行真正的 DB 回源查询。
