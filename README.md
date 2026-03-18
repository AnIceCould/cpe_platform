# CPE Platform
### 基于流处理与 AI 预测的智能 CPE 丢包监测与配置平台

本平台是一个集成了边缘计算、分布式流处理与机器学习预测的实时监控系统，旨在解决大规模 CPE（Customer Premise Equipment）设备在复杂网络环境下的实时丢包检测与自动化配置问题。

### 核心技术栈
- **消息中间件**: MQTT (Mosquitto), Apache Kafka
- **流处理引擎**: Apache Flink, Node-RED
- **AI/计算**: XGBoost, gRPC
- **存储/缓存**: MySQL, Redis
- **实时通信**: WebSocket, gRPC
- **预测模型**: [Packet Loss Event Classification Models](https://github.com/AnIceCould/Proj-of-Polimi/tree/main/Network%20Measurement%20and%20Data%20Analysis%20Lab/H_Packet%20Loss%20Event%20Classification)

---

### 系统架构图
![系统架构](structure.png)

---

### 系统模块说明

#### 1. 数据采集与边缘计算 (Data Ingestion & Edge)
- **多端采集**: 模拟/真实 CPE 设备实时产生流量与状态数据，通过 **MQTT** 协议上报至 **Mosquitto** 网关。
- **边缘清洗**: 利用 **Node-RED** 在网关侧进行初步的数据过滤与特征提取，有效降低中心服务器的带宽压力。

#### 2. 消息流转与实时处理 (Message Orchestration & Streaming)
- **高可用缓冲**: 经清洗的数据由网关投递至 **Kafka 集群**，确保在高并发情况下的数据持久化与削峰填谷。
- **分布式流处理**: **Apache Flink** 订阅 Kafka 原始流，利用滑动窗口（Sliding Window）和多算子逻辑进行实时的延迟聚合与状态计算。

#### 3. AI 预测引擎 (AI Inference Engine)
- **异步推理**: Flink 处理后的数据触发异步服务，通过 **gRPC** 调用后端负载均衡的 **XGBoost** 模型服务器。
- **实时分类**: 模型实时预测丢包事件并回传至消息队列。

#### 4. 业务逻辑与数据持久化 (Business Logic & Storage)
- **实时推送**: 丢包预测结果通过 **WebSocket** 协议实时同步至前端监控面板。
- **多级存储策略**: 
    - **缓存**: 实时状态写入 **Redis** 缓存，提供极致的查询响应。
    - **入库**: 采用定时任务批量将数据持久化至 **MySQL**，保证数据的长期可追溯性。
- **指令下发**: 支持通过前端下发配置指令，结合 Redis/MySQL 实现高性能的状态管理与配置更新。
