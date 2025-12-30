# Ray集群

Ray集群是Ray分布式计算的基础。本章将介绍如何创建、管理和使用Ray集群来执行分布式任务。

## 集群架构

Ray集群由以下组件组成：

- **Head节点**：集群的控制节点，运行Ray的控制平面服务
- **Worker节点**：执行计算任务的工作节点
- **对象存储**：跨节点共享数据的分布式内存存储
- **调度器**：负责任务调度和资源管理

## 本地集群

在单台机器上启动Ray集群用于开发和测试：

```python
import ray

# 启动本地Ray集群
ray.init()

# 或指定特定端口
ray.init(port=6379)
```

## 集群启动

### 启动Head节点

```bash
# 启动head节点
ray start --head --port=6379 --dashboard-host=0.0.0.0

# 指定更多配置
ray start --head \
    --port=6379 \
    --dashboard-host=0.0.0.0 \
    --num-cpus=4 \
    --num-gpus=1 \
    --memory=8000000000 \
    --object-store-memory=2000000000
```

### 连接Worker节点

```bash
# 将worker节点连接到集群
ray start --address='<head-node-ip>:6379'

# 或使用密码连接
ray start --address='<head-node-ip>:6379' --redis-password='<password>'
```

## 使用集群

在客户端连接到集群：

```python
import ray

# 连接到远程集群
ray.init(address='ray://<head-node-ip>:10001')

# 执行分布式任务
@ray.remote
def hello():
    return f"Hello from {ray.get_runtime_context().get_node_id()}"

# 提交任务
result = hello.remote()
print(ray.get(result))
```

## 集群配置

### 配置文件

创建集群配置文件 `cluster.yaml`：

```yaml
# 集群配置示例
cluster_name: ray-cluster

min_workers: 2
max_workers: 10

initial_workers: 2

# 防止自动停止空闲节点
autoscaling_mode: default
idle_timeout_minutes: 5

# 节点配置
head_node:
  InstanceType: m5.xlarge
  ImageId: ami-0abcdef1234567890

worker_nodes:
  InstanceType: m5.large
  ImageId: ami-0abcdef1234567890

# 启动配置
setup_commands:
  - pip install ray[default]

head_start_ray_commands:
  - ray stop
  - ray start --head --port=6379 --dashboard-host=0.0.0.0 --num-cpus=4

worker_start_ray_commands:
  - ray stop
  - ray start --address=$RAY_HEAD_IP:6379
```

## 集群管理命令

### 启动集群

```bash
# 启动或更新集群
ray up cluster.yaml

# 不提示确认
ray up cluster.yaml --no-config-cache
```

### 连接到集群

```bash
# 连接到集群进行交互
ray attach cluster.yaml
```

### 监控集群

```bash
# 查看集群状态
ray status

# 查看集群资源使用情况
ray top
```

### 停止集群

```bash
# 停止集群
ray down cluster.yaml
```

### 执行远程命令

```bash
# 在集群上执行命令
ray exec cluster.yaml 'nvidia-smi'
```

## 自动缩放

Ray支持基于负载的自动缩放：

```yaml
# 自动缩放配置
min_workers: 0
max_workers: 10
initial_workers: 2

autoscaling_mode: default
idle_timeout_minutes: 5

# 资源利用率阈值
target_utilization_fraction: 0.8

# 扩展策略
upscaling_speed: 1.0
```

## 安全配置

### 认证和授权

```yaml
# 安全配置
auth:
  # 使用密码保护Redis
  redis_password: your-secure-password

# 网络安全组配置
provider:
  type: aws  # 或 gcp, azure
  region: us-west-2
  # 安全组配置
  cache_stopped_nodes: false
```

### TLS加密

```yaml
# 启用TLS加密
docker:
  image: "rayproject/ray:latest"
  container_name: "ray_container"
  run_options:
    - --tls-enabled
    - --tls-cert-file=/path/to/cert
    - --tls-key-file=/path/to/key
```

## 资源管理

### CPU和内存资源

```python
# 指定任务资源需求
@ray.remote(num_cpus=2, memory=1000000000)
def cpu_intensive_task():
    return "完成CPU密集型任务"

# 指定GPU资源
@ray.remote(num_gpus=1)
def gpu_task():
    return "完成GPU任务"
```

### 自定义资源

```python
# 定义自定义资源
ray.init(resources={'CustomResource': 4})

@ray.remote(resources={'CustomResource': 1})
def custom_resource_task():
    return "使用自定义资源"
```

## 故障排除

### 常见问题

1. **连接问题**：
   - 检查防火墙设置
   - 确保端口6379、8265、10001等已开放

2. **资源不足**：
   - 检查节点资源分配
   - 调整任务资源需求

3. **性能问题**：
   - 监控集群资源使用
   - 优化任务并行度

### 调试命令

```bash
# 查看Ray日志
ray logs --help

# 查看特定节点日志
ray logs --node-ip <node-ip>

# 检查集群健康状态
ray health-check
```

## 监控和可观测性

### Dashboard

Ray提供了一个Web仪表板来监控集群：

```bash
# 仪表板默认在端口8265上运行
# 访问 http://<head-node-ip>:8265
```

### 指标收集

```python
# 启用指标收集
import ray
ray.init(
    include_dashboard=True,
    dashboard_host="0.0.0.0",
    dashboard_port=8265
)
```

## 最佳实践

1. **合理分配资源**：根据工作负载特性分配适当的CPU、GPU和内存资源
2. **监控集群状态**：定期检查集群健康和资源使用情况
3. **使用自动缩放**：根据负载动态调整集群大小
4. **安全配置**：在生产环境中启用适当的安全措施
5. **备份和恢复**：定期备份集群配置和重要数据