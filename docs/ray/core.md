# Ray核心API

Ray的核心API提供了一组基础构建块，用于创建分布式应用程序。这些API简单易用，同时支持复杂的分布式计算场景。

## 初始化Ray

在使用Ray之前，需要先初始化它：

```python
import ray

# 启动Ray
ray.init()

# 或者在集群模式下启动
ray.init(address=' ray://<head-node-ip>:10001')

# 关闭Ray
ray.shutdown()
```

## 远程函数 (Remote Functions)

远程函数是Ray的核心概念之一，允许您将普通Python函数转换为可以在集群中异步执行的函数。

### 定义远程函数

```python
@ray.remote
def my_function(x):
    return x * 2

# 异步执行函数
result_id = my_function.remote(42)

# 获取结果
result = ray.get(result_id)
print(result)  # 输出: 84
```

### 远程函数选项

可以为远程函数指定各种选项：

```python
@ray.remote(num_cpus=2, num_gpus=1, memory=100000000)
def resource_intensive_function(data):
    return process_data(data)
```

### 并行执行

远程函数可以并行执行：

```python
# 并行执行多个任务
results = [my_function.remote(i) for i in range(10)]

# 获取所有结果
all_results = ray.get(results)
```

## Actor

Actor是状态化的远程对象，可以维护状态并在其上执行方法。

### 定义Actor

```python
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
    
    def get_count(self):
        return self.count

# 创建Actor实例
counter = Counter.remote()

# 调用Actor方法
result = counter.increment.remote()
count = ray.get(result)
print(count)  # 输出: 1
```

### Actor选项

可以为Actor指定资源需求：

```python
@ray.remote(num_cpus=2, num_gpus=1)
class GPUActor:
    def __init__(self):
        # 初始化GPU资源
        pass
    
    def process(self, data):
        # 使用GPU处理数据
        return gpu_process(data)
```

## 对象存储 (Object Store)

Ray的对象存储允许在集群中高效地共享数据。

### 创建对象引用

```python
# 创建对象引用
obj_ref = ray.put("Hello, Ray!")

# 获取对象
value = ray.get(obj_ref)
print(value)  # 输出: Hello, Ray!
```

### 分布式共享

对象可以在任务和Actor之间共享：

```python
# 创建一个大的数据对象
large_data = ray.put(large_dataset)

# 在多个任务中使用同一个对象引用
@ray.remote
def process_data(data_ref):
    data = ray.get(data_ref)  # 从对象存储获取数据
    return process(data)

# 多个任务可以使用同一个数据引用
results = [process_data.remote(large_data) for _ in range(5)]
```

## Ray选项

Ray任务和Actor可以指定各种选项来控制资源分配和执行行为：

### 资源选项
- `num_cpus`: 需要的CPU核心数
- `num_gpus`: 需要的GPU数量
- `memory`: 需要的内存（字节）
- `object_store_memory`: 对象存储内存限制

### 调度选项
- `max_restarts`: 最大重启次数
- `max_task_retries`: 任务最大重试次数
- `lifetime`: Actor生命周期

```python
@ray.remote(
    num_cpus=2,
    num_gpus=1,
    memory=1000000000,
    max_restarts=5
)
class RobustActor:
    def __init__(self):
        pass
    
    def work(self):
        return "Working..."
```

## 等待条件

可以使用`ray.wait`来等待部分任务完成：

```python
# 创建多个任务
futures = [my_function.remote(i) for i in range(10)]

# 等待至少一个任务完成
ready, remaining = ray.wait(futures, num_returns=2)

# 获取已完成的结果
ready_results = ray.get(ready)
```

## 错误处理

Ray提供了错误处理机制：

```python
try:
    result = ray.get(failing_task.remote())
except ray.exceptions.RayTaskError as e:
    print(f"任务执行失败: {e}")
```

## 最佳实践

1. **避免频繁的对象传输**：尽量减少在任务之间传输大对象
2. **合理分配资源**：根据任务需求设置适当的资源限制
3. **使用批处理**：对小任务进行批处理以提高效率
4. **监控资源使用**：定期检查集群资源使用情况