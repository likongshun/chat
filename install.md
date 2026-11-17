# Modbus 采集引擎安装与使用手册

> 适用仓库：[`likongshun/chat`](https://github.com/likongshun/chat)  
> 部署方式：Docker Compose（推荐）或 Go 本地运行  
> 文档状态：与当前仓库实现对齐；涉及生产可靠性、容量和设备兼容性的事项均以实际验收为准。

## 目录

- [1. 项目简介](#1-项目简介)
- [2. 部署前准备](#2-部署前准备)
- [3. 获取工程](#3-获取工程)
- [4. Docker Compose 安装](#4-docker-compose-安装)
- [5. 首次启动与运行检查](#5-首次启动与运行检查)
- [6. 配置 Modbus 设备](#6-配置-modbus-设备)
- [7. 配置导入、版本与热切换](#7-配置导入版本与热切换)
- [8. 数据采集与下游存储](#8-数据采集与下游存储)
- [9. 监控与运行维护](#9-监控与运行维护)
- [10. 数据备份与恢复](#10-数据备份与恢复)
- [11. 常见故障排查](#11-常见故障排查)
- [12. 本地开发与测试](#12-本地开发与测试)
- [13. 生产部署检查清单](#13-生产部署检查清单)
- [14. 当前实现边界](#14-当前实现边界)

---

## 1. 项目简介

本项目是基于 Go 的 Modbus TCP 数据采集引擎。设备型号、寄存器地址、数据类型和处理规则由 Excel 工作簿定义，Modbus 通信核心不需要因为增加一个设备型号而修改。

当前工程包含以下主要能力：

1. Excel 五 Sheet 配置加载、校验和配置快照管理。
2. Modbus TCP FC01、FC02、FC03、FC04 读取，批量读取和数据解码。
3. 按设备独立调度、全局并发限制、超时、退避和有界输出队列。
4. 本地分段 WAL，独立记录 MQTT 与 InfluxDB 下游 ACK。
5. MQTT QoS 1 投递、InfluxDB v2 写入及失败重试。
6. PostgreSQL 配置版本存储、版本激活和运行时活动版本轮询。
7. Prometheus 指标、健康检查和 Docker Compose 部署。

### 1.1 数据链路

```text
Excel 设备配置
      |
      v
Go 配置校验 / PostgreSQL 配置版本库
      |
      v
设备调度器 -> Modbus TCP 设备
      |
      v
寄存器解码 / 数据处理 / 统一采样记录
      |
      v
本地分段 WAL
      |
      +------> MQTT（QoS 1）独立 ACK
      |
      +------> InfluxDB v2 独立 ACK
```

WAL 用于在下游暂时不可用时保留待投递数据。每条记录的 MQTT、InfluxDB ACK 独立维护；已经确认的下游不会因为另一个下游失败而重复执行同一条待确认投递。

### 1.2 Compose 服务

| 服务 | 用途 | 默认容器端口 |
|---|---|---:|
| dashboard | 独立 Web 监控控制台 | 8080（默认仅绑定 127.0.0.1） |
| collector | Go 采集引擎 | 9090（健康检查与指标） |
| postgres | 配置版本数据库 | 5432 |
| mqtt | Eclipse Mosquitto 消息代理 | 1883 |
| influxdb | InfluxDB v2 时序数据库 | 8086 |

Compose 将这些服务放在内部 bridge 网络中，并将管理端口绑定到宿主机的 `127.0.0.1`。Modbus 设备必须能够被 collector 容器访问；容器内访问设备使用设备实际可路由的 IP 地址，不要填写宿主机上的 `localhost`。

---

## 2. 部署前准备

### 2.1 推荐环境

- Linux 服务器，建议使用受支持的 64 位发行版。
- Docker Engine 与 Docker Compose v2（命令为 `docker compose`）。
- 可访问设备所在网络的服务器网卡、路由和防火墙规则。
- 持久化磁盘空间。WAL、PostgreSQL 和 InfluxDB 数据均需保留。
- 能访问设备 Modbus TCP 端口（通常为 TCP 502；以设备实际设置为准）。

Windows 或 macOS 可以用于开发和功能验证；正式部署建议使用 Linux，以便验证 WAL 的文件锁、fsync、磁盘预留空间和故障恢复行为与目标文件系统一致。

### 2.2 资源规划

当前 Compose 默认参数是启动基线，不是 5,000 台设备的容量承诺：

- 全局采集并发：500。
- 单次采集超时：800ms。
- 调度输出缓冲：10,000 条。
- WAL Segment：64 MiB。
- WAL 总预算：10 GiB。
- ACK 预留空间：64 MiB。

实际 CPU、内存、磁盘容量和网络带宽应通过目标设备点数、采集周期、记录大小、下游中断时长及压测结果计算。请勿直接把默认参数视为生产容量规格。

### 2.3 网络要求

部署前确认：

1. collector 容器可以访问每台设备的 IP 和 Modbus TCP 端口。
2. PostgreSQL、MQTT、InfluxDB 服务在 Compose 内部网络可互通。
3. 如果设备位于 VLAN 或 VPN 后方，宿主机路由、防火墙和 Docker 网络策略允许转发。
4. 不要把 MQTT 1883、PostgreSQL 5432 或 InfluxDB 8086 直接暴露到公网。

---

## 3. 获取工程

在部署服务器执行：

```bash
git clone https://github.com/likongshun/chat.git
cd chat
git checkout main
```

更新代码：

```bash
git pull --ff-only origin main
```

建议生产环境固定到经过测试的 commit，而不是在未经验证时自动跟随最新 `main`。

---

## 4. Docker Compose 安装

### 4.1 创建环境文件

```bash
cp .env.example .env
```

编辑 `.env`，至少替换以下初始凭据：

```dotenv
POSTGRES_PASSWORD=替换为高强度随机密码

INFLUXDB_ADMIN_USER=admin
INFLUXDB_ADMIN_PASSWORD=替换为高强度随机密码
INFLUXDB_ADMIN_TOKEN=替换为足够长的随机令牌
```

要求：

- 不要保留任何 `CHANGE_ME` 值。
- 不要把真实 `.env` 提交到 Git。
- 限制 `.env` 文件权限：`chmod 600 .env`。
- 生产环境优先使用 Docker secrets 或外部密钥管理服务。
- `INFLUXDB_ADMIN_TOKEN` 同时作为当前 Compose 中 collector 的 InfluxDB 写入令牌；生产环境建议进一步拆分管理令牌与最小权限写入令牌。

### 4.2 常用环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `modbus` | Compose 项目名 |
| `POSTGRES_DB` | `modbus` | PostgreSQL 数据库 |
| `POSTGRES_USER` | `modbus` | PostgreSQL 用户 |
| `POSTGRES_PASSWORD` | 必填 | PostgreSQL 密码 |
| `POSTGRES_HOST` | `postgres` | PostgreSQL 服务地址 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 端口 |
| `POSTGRES_SSLMODE` | `disable` | PostgreSQL SSL 模式；跨主机部署应配置 TLS |
| `CONFIG_XLSX` | 镜像内配置文件 | Excel 配置文件路径 |
| `CONFIG_IMPORT_ON_START` | `false` | 启动时导入工作簿并激活新配置版本 |
| `CONFIG_RELOAD_INTERVAL` | `5s`（程序默认） | 活动配置版本轮询周期 |
| `POLL_WORKERS` | `500` | 全局采集并发上限 |
| `POLL_TIMEOUT` | `800ms` | 单次设备采集超时 |
| `SCHEDULER_OUTPUT_BUFFER` | `10000` | 采集结果缓冲容量 |
| `LATEST_CACHE_MAX_POINTS` | `1500000` | 最新点位缓存容量上限；按设备数×点数及内存预算调整 |
| `WAL_SEGMENT_BYTES` | `67108864` | WAL 分段阈值，单位字节 |
| `MAX_WAL_BYTES` | `10737418240` | WAL 数据容量预算，单位字节 |
| `ACK_RESERVE_BYTES` | `67108864` | ACK 紧急预留空间，单位字节 |
| `MQTT_BROKER` | `tcp://mqtt:1883` | MQTT Broker 地址 |
| `MQTT_CLIENT_ID` | `modbus-collector` | MQTT 客户端 ID |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | 空 | MQTT 认证信息（需 Broker 同步配置） |
| `MQTT_TOPIC` | `modbus/telemetry` | 遥测发布主题 |
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB 地址 |
| `INFLUXDB_ORG` | `modbus` | InfluxDB Organization |
| `INFLUXDB_BUCKET` | `telemetry` | InfluxDB Bucket |
| `INFLUXDB_TOKEN` | 默认回退到 Admin Token | InfluxDB 写入令牌 |
| `SINK_TIMEOUT` | `10s` | 下游请求超时 |
| `DELIVERY_BATCH_SIZE` | `500` | 单批投递记录上限 |
| `DELIVERY_POLL_INTERVAL` | `1s` | 投递轮询间隔 |
| `DELIVERY_RETRY_INTERVAL` | `1s` | 投递失败重试间隔 |
| `METRICS_ADDR` | `:9090` | 健康检查和 Prometheus HTTP 监听地址 |

如果设置 `POSTGRES_HOST`，程序会连接 PostgreSQL 配置库。Compose 已提供该变量。独立运行且不设置 PostgreSQL 时，程序从 Excel 加载配置。

### 4.3 校验 Compose 配置

```bash
docker compose config
```

该命令应成功展开配置。如果提示缺少密码、变量未设置或 YAML 错误，请先修正 `.env` 和 Compose 文件，不要直接启动。

### 4.4 构建并启动

```bash
docker compose up -d --build
```

查看服务：

```bash
docker compose ps
```

查看 collector 日志：

```bash
docker compose logs -f --tail=200 collector
```

查看所有服务日志：

```bash
docker compose logs -f --tail=100
```

首次启动时，PostgreSQL 会创建数据库；collector 执行配置库迁移。如果配置库还没有活动配置版本，collector 会校验并导入镜像中的 Excel 工作簿，建立初始版本。若活动版本已存在，默认不会用工作簿覆盖它。

### 4.5 数据卷

当前 Compose 使用以下命名卷：

| 卷 | 保存内容 |
|---|---|
| `collector_wal` | 本地 WAL、Checkpoint、Manifest、ACK 和 reserve 文件 |
| `postgres_data` | PostgreSQL 配置版本数据 |
| `mqtt_data` | Mosquitto 持久化数据 |
| `mqtt_log` | Mosquitto 日志 |
| `influxdb_data` | InfluxDB 数据 |
| `influxdb_config` | InfluxDB 配置 |

检查卷：

```bash
docker volume ls
```

**重要：** 日常停止或升级不要执行 `docker compose down -v`，该命令会删除上述持久化卷。除非明确要销毁所有运行数据，否则不要删除这些卷。

---

## 5. 首次启动与运行检查

### 5.1 服务状态

```bash
docker compose ps
```

重点检查：

- postgres：健康检查通过。
- influxdb：健康检查通过。
- mqtt：容器正常运行。
- collector：容器运行，日志中没有配置加载、WAL 打开或下游初始化的致命错误。

### 5.2 健康检查

Compose 将 collector 的 9090 端口绑定到宿主机 loopback：

```bash
curl -fsS http://127.0.0.1:9090/healthz
curl -fsS http://127.0.0.1:9090/readyz
```

- `/healthz`：进程健康检查。
- `/readyz`：服务就绪检查。

Prometheus 指标：

```bash
curl -fsS http://127.0.0.1:9090/metrics
```

如需从其他机器访问监控端点，应先配置反向代理、认证和网络访问控制，不要简单地将管理端口绑定到公网网卡。

### 5.3 检查 InfluxDB

浏览器访问：

`http://服务器IP:8086`

当前 Compose 将 InfluxDB 初始化为：

- Organization：`INFLUXDB_ORG`
- Bucket：`INFLUXDB_BUCKET`
- 管理用户：`INFLUXDB_ADMIN_USER`

使用 `.env` 中配置的凭据登录。确认初始化完成后，再检查采集数据是否持续写入。

### 5.4 检查 MQTT

本机可使用 MQTT 客户端订阅：

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'modbus/telemetry' -v
```

当前仓库附带的 Mosquitto 配置用于开发验证，允许匿名连接。生产环境必须先配置认证、ACL 和 TLS；若启用认证，collector 的 `MQTT_USERNAME`、`MQTT_PASSWORD` 必须与 Broker 用户配置匹配。

---

## 6. 配置 Modbus 设备

### 6.1 Excel 配置文件位置

默认文件：

`configs/modbus_device_config.xlsx`

容器内路径：

`/app/configs/modbus_device_config.xlsx`

Dockerfile 在构建镜像时将该文件复制进镜像。修改工作簿后，需要重新构建镜像；配置库已有活动版本时，还需要显式导入新工作簿，或通过配置版本管理激活新版本。

### 6.2 五个 Sheet

| Sheet | 用途 | 主要字段 |
|---|---|---|
| Devices | 设备实例 | `device_id`、`name`、`host`、`port`、`unit_id`、`model`、`poll_ms`、`enabled` |
| Points | 点位和寄存器定义 | `model`、`point_code`、`function`、`address`、`type`、`length/count`、`scale`、`offset`、`byte_order`、`word_order`、`unit`、`read_group`、`rw`、`enabled` |
| Processing | 解码后的处理规则 | `point_code`、`operation`、`param1`、`param2` |
| WriteRules | 写权限规则定义 | `model`、`point_code`、`enabled`、`min`、`max`、`require_confirm` |
| Enums | 枚举值和状态标签 | `point_code`、`value`、`label` |

Sheet 名称和必需列标题应保持稳定。列名按表头匹配，不要随意更改为同义词或删除必需列。

### 6.3 Devices：设备实例

每行定义一台设备实例。关键字段：

- `device_id`：设备唯一标识，不能重复。
- `name`：便于运维识别的设备名称。
- `host`、`port`：设备 Modbus TCP 地址和端口。
- `unit_id`：Modbus Unit Identifier，按网关或设备手册填写。
- `model`：关联 Points 中的设备型号。
- `poll_ms`：该设备采集周期，单位毫秒。
- `enabled`：是否参与采集。

设备 IP 必须是 collector 容器网络可达的地址。通过网关连接多个从站时，Unit ID 应按网关说明填写。

### 6.4 Points：寄存器点位

关键字段：

- `model`：所属设备型号。
- `point_code`：点位唯一代码。
- `function`：Modbus 功能码。
- `address`：寄存器或线圈地址。请根据设备手册确认地址是 0-based 还是文档显示的 1-based；必要时换算后再填写。
- `type`：解码数据类型。
- `length` / `count`：字符串或连续数据所需数量（按对应类型要求）。
- `scale`、`offset`：线性换算参数。
- `byte_order`、`word_order`：多字节数据排列方式。
- `read_group`：批量读取分组标识。
- `rw`：当前必须保持只读。
- `enabled`：是否采集该点位。

支持的功能码：

| 功能码 | 用途 | 约束 |
|---|---|---|
| FC01 | 读线圈 | BOOL |
| FC02 | 读离散输入 | BOOL |
| FC03 | 读保持寄存器 | 最多 125 个寄存器/请求 |
| FC04 | 读输入寄存器 | 最多 125 个寄存器/请求 |

位读取单次请求最多 2,000 bit。连续点位只有在功能码、型号和 read_group 兼容时才会合并读取。

支持的数据类型包括：

- `BOOL`
- `INT16`、`UINT16`
- `INT32`、`UINT32`
- `FLOAT32`
- `INT64`、`UINT64`
- `FLOAT64`
- `STRING`

常见字节序：`ABCD`、`BADC`、`CDAB`、`DCBA`。字序支持 `HIGH_LOW`、`LOW_HIGH`。必须以设备厂商寄存器手册和实际读数验证为准。

### 6.5 Processing：处理规则

当前处理器采用固定操作符白名单，不会执行 Excel 中任意脚本或表达式。

| 操作符 | 说明 | 参数 |
|---|---|---|
| `SCALE` | `value * param1 + param2` | 倍率、偏移 |
| `LINEAR` | 线性换算 | 斜率、截距 |
| `ABS` | 绝对值 | 无 |
| `CLAMP` | 限制到闭区间 | 最小值、最大值 |
| `BIT` | 提取指定 bit | bit 索引 0..63 |
| `ENUM` | 映射到 Enums 标签 | 对应点位枚举 |
| `ROUND` | 小数位舍入 | 精度 -9..9 |
| `DIVIDE` | 除以常数 | 非零除数 |

同一点位的处理规则按工作簿顺序执行。参数必须符合对应操作符的范围约束。

### 6.6 新增设备型号的建议流程

1. 取得设备厂商正式寄存器手册。
2. 确认 Modbus TCP 参数、Unit ID、寄存器地址基准、功能码。
3. 在 Devices 中增加设备实例，并关联型号。
4. 在 Points 中逐项录入寄存器、数据类型、字节序和缩放参数。
5. 按需添加 Processing 和 Enums。
6. 保持所有写权限关闭。
7. 在测试环境验证少量只读点位的原始寄存器值和工程值。
8. 检查超时、异常码、断线重连和恢复行为，再扩大采集范围。

不要根据相似型号猜测寄存器地址或字节序。

### 6.7 写操作安全说明

**当前版本不提供 Modbus 写执行能力。**

- Points 中的可写配置会被校验拒绝。
- 启用的 WriteRules 会被校验拒绝。
- Modbus 客户端只暴露读取功能，不包含写寄存器 API。

不得通过修改 Excel 绕过上述限制。任何未来写功能都必须单独进行代码审查、点位白名单、设备手册核验、安全联锁验证和现场负责人批准。

---

## 7. 配置导入、版本与热切换

### 7.1 首次导入

启用 PostgreSQL 后，collector 启动时会执行配置库迁移：

- 若尚无活动配置版本：从 `CONFIG_XLSX` 指向的工作簿导入并激活初始版本。
- 若已有活动版本：默认加载数据库中的活动版本，不会自动用镜像内工作簿覆盖。

配置版本包含校验后的配置快照、校验摘要和来源信息。

### 7.2 导入新的 Excel 配置

修改仓库中的工作簿后，重新构建镜像：

```bash
docker compose build collector
```

然后将 `.env` 中：

```dotenv
CONFIG_IMPORT_ON_START=true
```

重启 collector 触发导入：

```bash
docker compose up -d collector
docker compose logs -f --tail=200 collector
```

确认日志显示新配置版本已加载/激活后，将 `CONFIG_IMPORT_ON_START` 改回 `false`，避免后续每次重启都重复导入并创建新版本。

导入前应备份 PostgreSQL，并在测试环境先验证工作簿。

### 7.3 活动版本切换

配置仓库支持版本历史和显式激活。collector 会按配置轮询周期检查活动版本；检测到版本变化后，会停止当前调度、排空当前采集结果并关闭旧设备连接，再加载新配置启动采集。

注意：

- 配置必须通过完整校验才能生效。
- 新配置校验失败时，不应把无效配置作为运行配置。
- 切换期间可能出现短暂采集间隔。
- 该机制是活动版本轮询切换，不等于任意 Excel 文件的实时文件监控。
- 当前仓库尚未提供完整的面向操作员的配置管理 Web/API 控制台；版本管理需使用已实现的配置库接口或后续管理工具。

---

## 8. 数据采集与下游存储

### 8.1 采集调度

当前调度器主要行为：

- 每台设备独立调度，同一设备请求串行执行。
- 全局并发受 `POLL_WORKERS` 限制。
- 按设备周期执行，并对启动时间错峰。
- 设备超时或错误时退避重试，避免故障设备持续占满资源。
- 输出队列有界；下游持久化速度跟不上时会形成背压。
- 收到 SIGTERM/SIGINT 后执行有界停止和采集结果排空。

### 8.2 WAL 持久化

WAL 默认目录：

- Compose：命名卷 `collector_wal`，容器内挂载到 `/data/wal`。
- 本地运行：默认 `./data/wal`，可用 `WAL_DIR` 修改。

WAL 由 Manifest 管理的 Data Segment、ACK Segment、Checkpoint 和 ACK reserve 构成。运行时不要手工编辑、移动或删除其中的文件。

关键行为：

- Data 和 ACK 分开存储。
- MQTT、InfluxDB ACK 独立持久化。
- 只有下游成功并且 ACK 持久化后，才会更新对应确认状态。
- 磁盘压力达到停止水位或 WAL 预算边界时，新数据写入会被拒绝；ACK 和回收路径仍需继续工作。
- ACK reserve 耗尽且无法持久化 ACK 时，系统必须失败关闭，不得把未持久化 ACK 当作成功。

当前 WAL 设计的具体格式、恢复顺序和边界见 [WAL 格式与崩溃一致性说明](docs/WAL_FORMAT.md)。

### 8.3 MQTT

默认 Broker：

`tcp://mqtt:1883`

默认 Topic：

`modbus/telemetry`

发布采用 MQTT QoS 1，并等待 Broker 的发布确认。QoS 1 是至少一次语义；消费者应使用事件标识实现幂等处理，不要假设消息在所有故障场景下绝对只出现一次。

### 8.4 InfluxDB

默认地址：

`http://influxdb:8086`

collector 使用 InfluxDB v2 HTTP 写入接口。收到成功的 2xx 响应后记录该条记录的 InfluxDB ACK。请按当前代码和实际部署的 Bucket、Organization、Token 配置检查写入情况。

如果 InfluxDB 未正确配置，程序会禁用下游投递 worker，采集记录会留在 WAL 中；应尽快修复下游并监控 WAL 积压与磁盘使用量。

### 8.5 数据积压处理

下游中断期间，数据会留在本地 WAL。恢复后投递 worker 会继续处理未确认记录。运维人员应持续观察：

- WAL 总字节数与文件系统使用率。
- 待投递记录数量及积压增长速度。
- MQTT、InfluxDB 各自 ACK 和失败情况。
- fsync 延迟和错误。
- 容器重启次数及磁盘剩余空间。

不要通过手动删除 WAL Segment 来释放磁盘空间。应恢复下游、确认 ACK 继续推进，并让 Checkpoint/回收流程完成。

---

## 9. 监控与运行维护

### 9.1 Prometheus 指标端点

`http://127.0.0.1:9090/metrics`

指标覆盖采集调度、WAL、fsync 和投递状态。WAL 重点指标包括：

- `modbus_wal_fsync_duration_seconds`：fsync 延迟直方图。
- `modbus_wal_fsync_errors_total`：fsync 错误计数。
- `modbus_wal_fsync_p99_seconds`：近似 P99 指标。
- WAL 容量/文件系统压力相关指标。

当前实现的 WAL 压力分级阈值为 70% Warning、85% Critical、95% Stop；实际告警策略应结合目标磁盘和业务速率验证后配置。不要把该阈值直接当作所有文件系统的安全保证。

### 9.2 常用运维命令

```bash
# 服务状态
docker compose ps

# collector 最近日志
docker compose logs --tail=300 collector

# 持续跟踪 collector
docker compose logs -f collector

# 重启 collector
docker compose restart collector

# 重建并更新 collector
docker compose up -d --build collector

# 查看卷
docker volume ls

# 查看宿主机磁盘
df -h
df -i

# 查看 Docker 磁盘占用
docker system df
```

`docker system prune` 可能删除未使用的镜像、容器和构建缓存；执行前确认维护窗口和回滚镜像。不要误删持久化卷。

### 9.3 正常升级

建议顺序：

1. 备份数据库，并确认 WAL 所在卷可用。
2. 获取经过验证的代码版本或固定 commit。
3. 阅读变更记录和配置迁移说明。
4. 执行 `docker compose config`。
5. 构建新镜像：`docker compose build collector`。
6. 更新服务：`docker compose up -d`。
7. 检查日志、健康检查、采集数据和下游 ACK。
8. 保留上一个可用镜像和数据库备份，便于回滚。

升级时不要删除 WAL 卷。若涉及 WAL 格式变更，必须先阅读对应版本迁移说明并做好离线备份。

### 9.4 正常停止

```bash
docker compose stop
```

停止整个 Compose 项目但保留卷：

```bash
docker compose down
```

不要使用：

```bash
docker compose down -v
```

除非明确要销毁全部持久化数据。

---

## 10. 数据备份与恢复

### 10.1 备份原则

需要分别保护：

1. PostgreSQL 配置版本库。
2. InfluxDB 时序数据。
3. collector WAL 卷中的尚未完成投递记录。
4. Excel 配置源文件和部署环境文件（密钥应单独安全保存）。

WAL 目录内部存在文件锁、Manifest、Checkpoint、Data/ACK Segment 之间的崩溃一致性关系。**不要在 collector 正在运行时用普通文件复制方式备份 WAL 目录**，也不要只备份其中某几个文件。

### 10.2 PostgreSQL 逻辑备份示例

```bash
mkdir -p backups
docker compose exec -T postgres sh -c \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
  > backups/postgres-$(date +%Y%m%d-%H%M%S).dump
```

检查备份文件非空并妥善转移到受控存储。生产环境还应配置定期备份、异地保存和恢复演练。

### 10.3 WAL 备份建议

推荐在维护窗口中：

1. 停止 collector 并确认容器退出。
2. 使用文件系统快照或受控卷备份工具完整备份 `collector_wal` 卷。
3. 保留整个 WAL 目录，包括隐藏锁文件、Manifest、所有 Segment、Checkpoint 和 reserve 文件。
4. 记录对应的应用版本和备份时间。
5. 恢复时将完整卷恢复到原挂载路径，再启动 collector。

若无法确保卷备份的一致性，不要将不完整 WAL 备份用于恢复生产数据。

### 10.4 恢复演练

恢复前停止写入端，先在隔离环境验证：

- PostgreSQL 备份可以正常恢复并读取活动配置版本。
- InfluxDB 数据可恢复且 Bucket/Organization 配置一致。
- WAL 可通过启动恢复扫描，且无缺失已提交 Segment、校验错误或锁冲突。
- pending 记录可以继续向两个下游投递。
- 恢复后的事件没有因为人为操作导致序号或 ACK 状态丢失。

---

## 11. 常见故障排查

### 11.1 collector 启动失败

查看日志：

```bash
docker compose logs --tail=300 collector
```

重点检查：

- Excel 文件路径是否存在，工作簿是否损坏。
- 表名、必需表头、设备 ID、型号引用和点位定义是否通过校验。
- PostgreSQL 是否健康，密码和连接参数是否正确。
- WAL 卷是否可写、是否有足够空间。
- 是否有另一个进程占用同一个 WAL 目录。

### 11.2 设备连接超时或拒绝连接

检查：

1. 设备是否上电且 Modbus TCP 服务已启用。
2. IP、端口、Unit ID 是否正确。
3. 宿主机是否能访问设备网络。
4. Docker 容器网络是否有到设备 VLAN 的路由。
5. 防火墙是否放行 TCP 端口。
6. 设备是否限制并发连接或请求速率。

可在宿主机执行：

```bash
nc -vz 设备IP 502
```

该命令只检查 TCP 端口可达性，不代表 Modbus 寄存器地址正确。

### 11.3 读取值不正确

优先检查：

- 地址基准是否把手册的 1-based 地址误填为 0-based。
- FC03 与 FC04 是否选错。
- INT/UINT/FLOAT 类型是否正确。
- 多寄存器字节序和字序是否正确。
- Scale、Offset 是否重复换算。
- STRING 长度和编码是否符合设备手册。

应先用单个点位与设备厂商工具或已知参考值比对，再恢复批量采集。

### 11.4 PostgreSQL 连接失败

```bash
docker compose ps postgres
docker compose logs --tail=200 postgres
```

确认 `.env` 中数据库名、用户名、密码一致。修改 PostgreSQL 初始化密码变量不会自动修改已经初始化的数据目录中的既有数据库密码；如需改密，应通过 PostgreSQL 管理命令执行，而不是只改环境文件。

### 11.5 MQTT 投递失败

检查 Broker 日志和网络：

```bash
docker compose logs --tail=200 mqtt
```

确认 `MQTT_BROKER`、Topic、客户端 ID 及认证信息正确。当前默认 Broker 配置仅适合开发环境；若已开启认证，检查用户密码、ACL 和 TLS 配置。

### 11.6 InfluxDB 写入失败

检查：

```bash
docker compose logs --tail=200 influxdb
```

核对 URL、Organization、Bucket 和 Token。Token 必须对目标 Bucket 具有写权限。若 InfluxDB 不可用，WAL 会保留未确认数据；恢复服务后观察投递是否恢复、积压是否下降。

### 11.7 WAL 磁盘压力或 ENOSPC

```bash
df -h
df -i
docker system df
docker compose logs --tail=300 collector
```

处理步骤：

1. 确认宿主机文件系统是否已满，是否 inode 耗尽。
2. 确认 WAL 目录所在卷和挂载路径。
3. 优先恢复 MQTT/InfluxDB 下游连接，使 ACK、Checkpoint 和回收继续推进。
4. 按运维流程扩容磁盘或释放非业务临时文件。
5. 检查 reserve 是否恢复、WAL 压力指标是否下降。

不要手工删除 Segment、ACK、Checkpoint 或 Manifest。若 ACK reserve 耗尽，系统不能把未持久化的 ACK 视为成功；应先恢复可用磁盘空间，再观察自动补充和重试。

### 11.8 容器反复重启

```bash
docker compose ps
docker compose logs --tail=500 collector
docker inspect "$(docker compose ps -q collector)" --format '{{json .State}}'
```

检查退出码、OOM、健康检查、配置校验错误、数据库依赖和 WAL 打开失败原因。不要在未确认 WAL 状态前反复删除容器卷或重建数据目录。

---

## 12. 本地开发与测试

### 12.1 Go 环境

当前 Dockerfile 使用 Go 1.23 构建。安装兼容版本的 Go 后：

首次克隆后先生成并维护 Go 依赖校验文件 `go.sum`：

```bash
go mod tidy
go mod verify
go test ./...
go vet ./...
go test -race ./internal/...
go build ./cmd/...
```

请将生成的 `go.sum` 与 `go.mod` 一并提交到版本库。不要通过设置 `GOSUMDB=off` 关闭模块校验来掩盖缺失的校验和。Docker 构建阶段也会执行 `go mod tidy` 和 `go mod verify`，以兼容当前尚未包含 `go.sum` 的检出版本。

### 12.2 本地启动

不配置 PostgreSQL 时，程序从本地 Excel 加载配置：

```bash
export CONFIG_XLSX=./configs/modbus_device_config.xlsx
export WAL_DIR=./data/wal
export METRICS_ADDR=:9090

go run ./cmd/collector
```

本地运行时如需完整下游投递，应额外设置 MQTT 和 InfluxDB 连接参数。不要把测试环境的凭据复用到生产。

### 12.3 Compose 联调

```bash
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs -f collector
```

结束联调：

```bash
docker compose down
```

该命令保留命名卷；测试中若要彻底清理数据，应先确认没有需要保留的数据库和 WAL，再单独执行卷清理。

---

## 13. 生产部署检查清单

上线前逐项确认：

### 主机和容器

- [ ] Linux 主机、Docker Engine、Compose v2 版本已固定并记录。
- [ ] collector 到全部目标设备的网络路由和防火墙已验证。
- [ ] 容器以非 root 用户运行，权限和只读文件系统符合预期。
- [ ] 已配置 CPU、内存、磁盘容量监控和日志轮转策略。
- [ ] 已固定经过验证的镜像版本或 commit。

### 凭据和网络安全

- [ ] 所有默认密码和令牌均已替换。
- [ ] `.env` 未提交 Git，权限受限。
- [ ] MQTT 已启用认证、ACL 和 TLS；不再使用匿名开发配置。
- [ ] PostgreSQL、InfluxDB 管理端口不对公网开放。
- [ ] 数据库和下游凭据遵循最小权限原则。
- [ ] 密钥有轮换和吊销流程。

### 设备和配置

- [ ] 每种设备型号均有正式寄存器手册。
- [ ] 地址基准、功能码、数据类型、字节序和缩放已逐点验证。
- [ ] 设备采集周期和并发参数经现场网络验证。
- [ ] 所有写功能保持禁用。
- [ ] 配置导入和版本切换经过回滚演练。

### 数据可靠性

- [ ] PostgreSQL、InfluxDB 和 WAL 均有备份与恢复演练。
- [ ] WAL 所在文件系统的 fsync、锁、崩溃恢复和 ENOSPC 行为已实测。
- [ ] 已测试 inode、配额、磁盘满、异常断电和设备/下游中断。
- [ ] 已设置 WAL 容量、积压、fsync 延迟和错误告警。
- [ ] 已执行多小时或更长时间的目标环境 soak test。
- [ ] 已完成目标规模设备数和点数的容量压测。

### 设备安全

- [ ] 先进行只读联调。
- [ ] 已与现场负责人确认设备通信负载和维护窗口。
- [ ] 未通过配置文件尝试启用写操作。
- [ ] 任何未来写能力均须独立安全评审和现场授权。

---

## 14. 当前实现边界

本手册描述的是当前仓库代码，不代表所有规划能力已经完成。部署和验收时请特别注意：

1. Modbus TCP 读链路和基础调度能力已实现；物理设备互操作仍需按具体厂商型号验证。
2. PostgreSQL 配置版本存储、初始导入、活动版本读取和轮询切换已实现；完整的操作员管理控制台和设备租约/多实例分片尚未完成。
3. WAL 已有分段、Manifest、Checkpoint、独立 ACK、恢复和磁盘压力处理机制，并有崩溃注入及 ext4 ENOSPC CI 测试；这不等于已经在所有生产文件系统、存储控制器和断电场景下完成验收。
4. CI 中的 10,000 条积压记录测试、短时 soak 和 benchmark 不能替代多小时/24 小时生产磁盘测试，也不能证明 5,000 台设备的容量。
5. MQTT QoS 1 和 InfluxDB 写入有独立 ACK；消费者仍应按事件标识实现幂等。
6. 写执行保持关闭。不要将该版本用于未经验证的控制写入场景。

实施状态和 WAL 技术细节请继续参考：

- [实现状态](docs/IMPLEMENTATION_STATUS.md)
- [Docker Compose 部署说明](docs/DOCKER_COMPOSE.md)
- [Excel 配置说明](docs/CONFIGURATION.md)
- [WAL 格式与崩溃一致性](docs/WAL_FORMAT.md)

---

**项目地址：** https://github.com/likongshun/chat


## Web 数据监控控制台

项目提供独立的轻量级 Web 控制台，前端位于 `web/`，使用 Nginx 静态托管，并通过同源反向代理读取采集器监控接口。前端不连接数据库、不暴露 MQTT/InfluxDB/PostgreSQL 凭据。

启动或更新：

```bash
docker compose up -d --build dashboard
```

默认仅绑定服务器本机 `127.0.0.1:8080`，访问 `http://127.0.0.1:8080`。如需通过局域网访问，可在 `.env` 设置 `DASHBOARD_BIND=0.0.0.0`，并在防火墙/反向代理层配置访问控制与 HTTPS；不要直接将无认证控制台暴露到公网。

控制台除运行监控外，已接入设备与测点浏览功能：
- 设备列表来自当前生效配置快照，展示设备型号、地址、启用状态、测点数和最近采集时间。
- 测点列表来自当前型号配置，最新值由采集器内存有界缓存提供，显示数值、单位、质量和采集时间；缓存不是历史数据库，重启后会重新积累。
- 历史曲线从 InfluxDB v2 查询真实 `modbus_point` 测点数据，Token 仅保留在 Go 后端，不下发浏览器。
- 历史查询要求选择设备和测点，时间范围最多 7 天，单次最多 1,000 条记录。
- 运行监控仍每 5 秒刷新；设备列表每 15 秒刷新，点位详情在切换设备时刷新。

后端同源 API：
- `GET /api/v1/devices`：设备配置与最近采集时间。
- `GET /api/v1/devices/{device_id}/points`：设备型号测点定义和最新缓存值。
- `GET /api/v1/telemetry?device_id=...&point_id=...&from=RFC3339&to=RFC3339&limit=500`：InfluxDB 历史数据。

最新值缓存默认上限为 100,000 个设备/测点组合，可通过 `LATEST_CACHE_MAX_POINTS` 调整。对于 5,000 台设备、每台 300 点的规模，应结合内存预算将容量提高到覆盖目标点数，并实测缓存开销。当前缓存仅保留最新值，不能替代 InfluxDB 历史数据；InfluxDB 未配置或查询失败时，页面会显示明确错误状态，不会生成模拟历史值。
