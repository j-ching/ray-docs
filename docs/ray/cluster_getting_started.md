# Ray集群入门

Ray集群允许您在多台机器上分布式执行任务。本节介绍如何设置和管理Ray集群。

## 集群架构

Ray集群由以下组件组成：

- **头节点（Head Node）**: 集群的管理节点，运行GCS（Global Control Service）
- **工作节点（Worker Nodes）**: 执行任务和存储数据的节点
- **Raylet**: 每个节点上运行的进程，负责任务调度

## 使用Ray集群启动器

最简单的集群启动方式是使用Ray集群启动器：

```bash
# 启动头节点
ray up -y ray/python/ray/autoscaler/local/example-full.yaml

# 连接到集群
ray attach ray/python/ray/autoscaler/local/example-full.yaml
```

## 集群配置文件

集群配置使用YAML文件定义：

```yaml
# example-cluster.yaml
cluster_name: basic-ray-cluster

# 节点配置
min_workers: 1
max_workers: 3
initial_workers: 1

# 资源配置
available_node_types:
    ray.head.default:
        resources: {"CPU": 2}
        node_config:
            InstanceType: m5.large
    ray.worker.default:
        min_workers: 1
        max_workers: 3
        resources: {"CPU": 2}
        node_config:
            InstanceType: m5.large

# 提供商配置
provider:
    type: aws  # 或 gcp, azure
    region: us-west-1
```

## 连接到集群

有多种方式连接到Ray集群：

```python
import ray

# 连接到远程集群
ray.init(address='ray://<head-node-ip>:10001')

# 或使用集群URL
ray.init(address='anyscale://my-cluster-name')
```

## 集群管理命令

常用集群管理命令：

```bash
# 启动集群
ray up /path/to/cluster-config.yaml

# 连接到集群
ray attach /path/to/cluster-config.yaml

# 停止集群
ray down /path/to/cluster-config.yaml

# 监控集群
ray status /path/to/cluster-config.yaml
```

## 集群扩缩容

Ray支持自动扩缩容：

```yaml
# 自动扩缩容配置
min_workers: 0
max_workers: 10
idle_timeout_minutes: 5

# 资源利用率阈值
target_utilization_fraction: 0.8
```

## 集群安全

为集群配置安全设置：

```yaml
# 启用TLS加密
auth_config:
    rs256_public_key_file: /path/to/public/key
    rs256_private_key_file: /path/to/private/key

# 网络配置
docker:
    image: "rayproject/ray:latest"
    container_name: "ray_container"
    run_options:
        - --cap-add=SYS_PTRACE
        - --security-opt="apparmor=unconfined"
```

## 集群监控

监控集群状态：

```python
import ray

ray.init(address='ray://<head-node-ip>:10001')

# 获取集群资源信息
resources = ray.cluster_resources()
print(f"可用CPU: {resources.get('CPU', 0)}")
print(f"可用GPU: {resources.get('GPU', 0)}")

# 获取节点信息
nodes = ray.nodes()
for node in nodes:
    print(f"节点ID: {node['NodeID']}, 状态: {node['Alive']}")
```

## 故障排除

集群常见问题：

1. **连接问题**：检查网络和安全组设置
2. **资源不足**：验证云提供商的资源配额
3. **配置错误**：验证YAML配置文件语法
4. **权限问题**：确保有适当的云提供商权限