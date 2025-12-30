# Ray中的并行化

Ray提供多种方式来实现并行计算，从简单的任务并行到复杂的分布式计算模式。本节介绍如何在Ray中有效地实现并行化。

## 任务并行

最简单的并行化形式是任务并行，多个任务可以同时执行：

```python
import ray

ray.init()

@ray.remote
def task(i):
    # 模拟一些计算工作
    import time
    time.sleep(1)
    return i * i

# 并行执行多个任务
futures = [task.remote(i) for i in range(10)]
results = ray.get(futures)

print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

ray.shutdown()
```

## 数据并行

Ray Data提供了高效的数据并行处理能力：

```python
import ray

# 创建一个数据集
ds = ray.data.range(10000)

# 并行映射操作
result = ds.map(lambda x: x * 2).take(10)
print(result)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

# 并行过滤操作
filtered = ds.filter(lambda x: x % 2 == 0).count()
print(f"偶数数量: {filtered}")
```

## Actor并行

Actor提供有状态的并行计算单元：

```python
import ray

@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
    
    def get_count(self):
        return self.count

# 创建多个计数器actor
counters = [Counter.remote() for _ in range(5)]

# 并行调用
results = ray.get([counter.increment.remote() for counter in counters])
print(results)  # [1, 1, 1, 1, 1]
```

## 异步迭代

Ray支持异步迭代模式，允许在任务执行时处理结果：

```python
import ray
from ray.util import async_iter

@ray.remote
def slow_task(i):
    import time
    time.sleep(i * 0.1)
    return i * i

# 提交多个任务
futures = [slow_task.remote(i) for i in range(5)]

# 异步获取结果
for i, result in enumerate(async_iter(futures)):
    print(f"结果 {i}: {result}")
```

## 批处理并行

对于I/O密集型任务，可以使用批处理来提高效率：

```python
import ray

@ray.remote
def process_batch(batch):
    # 处理一批数据
    return [x * 2 for x in batch]

# 将数据分批处理
data = list(range(100))
batch_size = 10
batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]

# 并行处理批次
batch_refs = [process_batch.remote(batch) for batch in batches]
results = ray.get(batch_refs)

# 合并结果
final_result = [item for batch_result in results for item in batch_result]
```

## 资源管理

在并行计算中，正确管理资源非常重要：

```python
import ray

# 限制CPU使用
@ray.remote(num_cpus=0.5)
def cpu_limited_task():
    # 这个任务只使用半个CPU核心
    return "完成"

# 限制内存使用
@ray.remote(memory=100 * 1024 * 1024)  # 100MB
def memory_limited_task():
    # 这个任务限制使用100MB内存
    return "完成"
```

## 动态并行

Ray支持动态创建和管理并行任务：

```python
import ray

@ray.remote
def dynamic_task(n):
    if n > 1:
        # 根据条件动态创建更多任务
        left = dynamic_task.remote(n - 1)
        right = dynamic_task.remote(n - 2)
        return ray.get(left) + ray.get(right)
    else:
        return n

# 计算斐波那契数列
result = dynamic_task.remote(10)
print(ray.get(result))
```

## 并行模式最佳实践

1. **任务粒度**：选择合适大小的任务，避免过细或过粗的粒度
2. **资源分配**：合理分配CPU、GPU和内存资源
3. **负载均衡**：确保任务在集群中均匀分布
4. **错误处理**：实现容错机制处理任务失败
5. **监控性能**：使用Ray Dashboard监控并行任务性能