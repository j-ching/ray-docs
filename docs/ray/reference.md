# 参考

本节提供Ray的主要API和功能的完整参考。

## 核心Ray API

### ray.init()
初始化Ray运行时。

```python
import ray

# 基本初始化
ray.init()

# 带配置的初始化
ray.init(
    num_cpus=4,                    # 指定CPU核心数
    num_gpus=1,                    # 指定GPU数量
    object_store_memory=2*10**9,   # 对象存储内存限制(2GB)
    dashboard_host="0.0.0.0",      # Dashboard主机
    dashboard_port=8265,           # Dashboard端口
    include_dashboard=True,        # 启用Dashboard
    local_mode=False,              # 本地模式(调试用)
    logging_level=logging.INFO,    # 日志级别
    log_to_driver=True            # 将日志输出到驱动程序
)
```

### ray.remote()
装饰器，用于将函数或类转换为远程函数或actor。

```python
# 远程函数
@ray.remote
def my_function(x, y):
    return x + y

# 带资源需求的远程函数
@ray.remote(num_cpus=2, num_gpus=1, memory=100*1024*1024)
def resource_intensive_function():
    return "完成"

# Actor类
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
```

### ray.get()
获取一个或多个Ray对象引用的值。

```python
# 获取单个对象
ref = my_function.remote(1, 2)
result = ray.get(ref)

# 获取多个对象
refs = [my_function.remote(i, i+1) for i in range(5)]
results = ray.get(refs)
```

### ray.wait()
等待对象引用完成。

```python
# 等待至少1个对象完成
ready_refs, remaining_refs = ray.wait(refs, num_returns=1)

# 等待最多5秒
ready_refs, remaining_refs = ray.wait(refs, timeout=5.0)
```

## Ray集群API

### ray.cluster_resources()
获取集群资源信息。

```python
resources = ray.cluster_resources()
print(f"可用CPU: {resources.get('CPU', 0)}")
print(f"可用GPU: {resources.get('GPU', 0)}")
print(f"可用内存: {resources.get('memory', 0)}")
```

### ray.nodes()
获取集群节点信息。

```python
nodes = ray.nodes()
for node in nodes:
    print(f"节点ID: {node['NodeID']}")
    print(f"节点地址: {node['NodeManagerAddress']}")
    print(f"节点状态: {node['Alive']}")
```

## Ray Data API

### ray.data
用于大规模数据处理。

```python
import ray

# 创建数据集
ds = ray.data.range(10000)  # 从范围创建
ds = ray.data.from_items([1, 2, 3, 4, 5])  # 从列表创建
ds = ray.data.from_pandas(df)  # 从pandas DataFrame创建
ds = ray.data.from_numpy(array)  # 从numpy数组创建

# 读取数据文件
ds = ray.data.read_csv("file.csv")
ds = ray.data.read_json("file.json")
ds = ray.data.read_parquet("file.parquet")

# 数据转换
ds = ds.map(lambda x: x * 2)  # 映射
ds = ds.filter(lambda x: x > 5)  # 过滤
ds = ds.flat_map(lambda x: [x, x+1])  # 平铺映射

# 数据操作
ds.show(5)  # 显示前5行
ds.count()  # 计数
ds.take(10)  # 获取前10个元素
ds.iter_batches(batch_size=32)  # 批量迭代
```

## Ray Train API

### ray.train
用于分布式训练。

```python
from ray import train
import torch
import torch.nn as nn

def train_func():
    # 获取分片数据集
    dataset = train.get_dataset_shard("train")
    
    # 准备模型用于分布式训练
    model = nn.Linear(10, 1)
    model = train.torch.prepare_model(model)
    
    # 报告指标
    for i in range(10):
        # 训练逻辑
        loss = 0.1  # 模拟损失
        train.report({"epoch": i, "loss": loss})

# 使用内置Trainer
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    scaling_config=ScalingConfig(
        num_workers=2,
        use_gpu=False
    )
)

result = trainer.fit()
```

## Ray Tune API

### ray.tune
用于超参数调优。

```python
from ray import tune

def objective(config):
    # 目标函数
    for step in range(10):
        loss = (config["x"] - 2) ** 2
        tune.report(loss=loss, step=step)

# 定义搜索空间
search_space = {
    "x": tune.uniform(0, 10),
    "y": tune.choice([1, 2, 3])
}

# 创建Tuner
tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=10,
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
```

## Ray Serve API

### ray.serve
用于模型服务。

```python
from ray import serve

@serve.deployment
def my_service(request):
    return "Hello, World!"

# 运行服务
serve.run(my_service.bind())

# 带配置的部署
@serve.deployment(
    num_replicas=2,
    max_concurrent_queries=100,
    ray_actor_options={"num_cpus": 1}
)
class MyModel:
    def __call__(self, request):
        return {"prediction": "result"}

serve.run(MyModel.bind())
```

## Ray Workflows API

### ray.workflow
用于有状态的工作流。

```python
from ray import workflow

@workflow.step
def my_step(x):
    return x + 1

# 运行工作流
result = my_step.step(10).run("my_workflow")
```

## 异常类型

### 主要异常
```python
# Ray任务执行错误
ray.exceptions.RayTaskError

# 获取超时错误
ray.exceptions.GetTimeoutError

# Ray系统错误
ray.exceptions.RaySystemError

# 资源不足错误
ray.exceptions.RayOutOfMemoryError
```

## 资源规格

### 资源类型
- `CPU`: 逻辑CPU核心
- `GPU`: GPU设备
- `memory`: 系统内存
- `object_store_memory`: 对象存储内存
- `accelerator_type`: 特定加速器类型

### 资源分配示例
```python
@ray.remote(num_cpus=2, num_gpus=0.5, memory=100*1024*1024)
def resource_specific_task():
    return "完成"
```

## 常用配置选项

### ray.init() 配置
- `num_cpus`: 指定CPU数量
- `num_gpus`: 指定GPU数量
- `object_store_memory`: 对象存储内存限制
- `local_mode`: 本地模式（调试）
- `include_dashboard`: 是否包含仪表板
- `dashboard_host`: 仪表板主机地址
- `dashboard_port`: 仪表板端口

### @ray.remote 配置
- `num_cpus`: 需要的CPU数量
- `num_gpus`: 需要的GPU数量
- `memory`: 需要的内存（字节）
- `object_store_memory`: 需要的对象存储内存
- `resources`: 自定义资源需求
- `max_calls`: 最大调用次数（用于清理）
- `max_restarts`: 最大重启次数
- `max_task_retries`: 最大任务重试次数

## 性能监控

### 性能分析工具
```python
from ray.util import profile

@ray.remote
def profiled_task():
    with profile("关键部分", extra_data={"type": "computation"}):
        # 要分析的代码
        return "完成"
```

## 最佳实践参考

### 资源管理
- 准确指定资源需求以避免资源争用
- 使用适当的任务粒度以减少调度开销
- 监控内存使用以避免内存泄漏

### 错误处理
- 使用try-catch处理Ray任务错误
- 实现适当的重试逻辑
- 设置合理的超时值

### 性能优化
- 批处理小任务以提高效率
- 使用数据本地性优化网络传输
- 利用缓存避免重复计算

这个参考文档涵盖了Ray的主要API和功能，可作为开发时的快速参考。