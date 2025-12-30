# 在本地启动Ray

在本地机器上启动Ray非常简单，本节介绍如何在单机环境中设置和运行Ray。

## 快速启动

最简单的启动方式是在Python代码中初始化Ray：

```python
import ray

# 启动Ray运行时
ray.init()

# 您的Ray代码
@ray.remote
def hello():
    return "Hello, Ray!"

result = ray.get(hello.remote())
print(result)

# 关闭Ray运行时
ray.shutdown()
```

## 命令行启动

您也可以使用命令行启动Ray集群：

```bash
# 启动Ray头节点
ray start --head --port=6379

# 连接到头节点
ray.init(address='localhost:6379')
```

## 本地模式

在开发和调试时，可以使用本地模式，所有任务将在单个进程中顺序执行：

```python
import ray

# 本地模式 - 用于调试
ray.init(local_mode=True)

# 在本地模式下，所有任务将顺序执行
@ray.remote
def task():
    return "执行中"

result = ray.get(task.remote())
print(result)

ray.shutdown()
```

## 配置选项

启动Ray时可以指定各种配置选项：

```python
import ray

# 使用自定义配置启动Ray
ray.init(
    num_cpus=4,                    # 指定CPU核心数
    num_gpus=1,                    # 指定GPU数量
    object_store_memory=200*1024*1024,  # 指定对象存储内存（200MB）
    dashboard_port=8265,           # 指定仪表板端口
    include_dashboard=True         # 启用仪表板
)
```

## 内存管理

在本地启动时，正确配置内存非常重要：

```python
import ray

# 限制Ray使用的内存量
ray.init(
    object_store_memory=10**9,      # 1GB对象存储
    memory_limit=4*10**9           # 4GB总内存限制
)
```

## 日志配置

可以配置Ray的日志级别：

```python
import ray
import logging

# 设置日志级别
logging.basicConfig(level=logging.INFO)

ray.init(
    logging_level=logging.INFO,
    log_to_driver=True
)
```

## 常见配置参数

- `num_cpus`: 指定可用的CPU核心数
- `num_gpus`: 指定可用的GPU数量
- `object_store_memory`: 对象存储内存限制
- `dashboard_host`: 仪表板主机地址
- `dashboard_port`: 仪表板端口
- `temp_dir`: 临时目录位置
- `include_dashboard`: 是否包含仪表板

## 停止Ray

正确停止Ray运行时很重要：

```python
import ray

ray.init()

# 您的Ray代码...

# 正确停止Ray
ray.shutdown()
```

或者使用命令行停止：

```bash
ray stop
```

## 故障排除

如果启动Ray时遇到问题，请检查：

1. 确保没有其他Ray实例正在运行
2. 检查端口是否被占用
3. 验证资源（CPU、内存、GPU）是否可用
4. 查看日志文件以获取更多信息