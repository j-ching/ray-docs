# 任务

在Ray中，任务是基本的计算单元，表示一个可以远程执行的函数调用。任务是无状态的，每次调用都会创建一个新的执行实例。

## 任务基础

定义和执行任务非常简单：

```python
import ray

ray.init()

# 定义一个远程任务
@ray.remote
def simple_task(x):
    return x * 2

# 提交任务（异步执行）
task_ref = simple_task.remote(5)

# 获取任务结果
result = ray.get(task_ref)
print(result)  # 输出: 10

ray.shutdown()
```

## 任务选项

可以为任务指定各种选项，如资源需求：

```python
import ray

@ray.remote(num_cpus=2, num_gpus=1, memory=100 * 1024 * 1024)
def resource_intensive_task():
    return "完成"

# 任务将需要2个CPU核心、1个GPU和100MB内存
result = ray.get(resource_intensive_task.remote())
```

## 任务依赖

任务可以依赖于其他任务的结果：

```python
import ray

@ray.remote
def task_a():
    return 10

@ray.remote
def task_b(value):
    return value * 2

# task_b依赖于task_a的结果
a_result = task_a.remote()
b_result = task_b.remote(a_result)

final_result = ray.get(b_result)
print(final_result)  # 输出: 20
```

## 并行任务执行

可以并行执行多个任务：

```python
import ray

@ray.remote
def worker_task(worker_id):
    import time
    time.sleep(1)  # 模拟工作
    return f"Worker {worker_id} 完成"

# 并行执行多个任务
task_refs = [worker_task.remote(i) for i in range(5)]
results = ray.get(task_refs)

for result in results:
    print(result)
```

## 任务调度

Ray会自动调度任务到可用的资源上：

```python
import ray

# 在特定节点上运行任务（如果知道节点资源ID）
@ray.remote
def node_specific_task():
    return ray.get_runtime_context().get_node_id()

result = node_specific_task.remote()
node_id = ray.get(result)
print(f"任务在节点 {node_id} 上执行")
```

## 任务超时

可以为任务设置超时：

```python
import ray

@ray.remote
def long_running_task():
    import time
    time.sleep(10)
    return "完成"

# 提交任务并设置超时
try:
    result = ray.get(long_running_task.remote(), timeout=5.0)
except ray.exceptions.GetTimeoutError:
    print("任务超时")
```

## 批量任务执行

对于大量相似任务，可以使用批量执行：

```python
import ray

@ray.remote
def process_item(item):
    # 处理单个项目
    return item ** 2

# 批量处理
items = list(range(100))
task_refs = [process_item.remote(item) for item in items]

# 可以一次性获取所有结果
all_results = ray.get(task_refs)
```

## 任务命名

可以为任务指定名称以方便调试：

```python
import ray

@ray.remote
def named_task():
    return "Hello"

# 设置任务名称
result = ray.get(named_task.options(name="greeting_task").remote())
```

## 任务最佳实践

1. **合适的任务粒度**：避免任务太小导致调度开销过大
2. **资源管理**：准确指定任务的资源需求
3. **错误处理**：实现适当的错误处理和重试机制
4. **结果处理**：及时获取和处理任务结果
5. **监控**：使用Ray Dashboard监控任务执行情况