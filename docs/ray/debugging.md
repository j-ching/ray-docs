# 调试

调试Ray应用程序可能比调试普通Python程序更具挑战性，因为代码在分布式环境中运行。本节介绍调试Ray应用程序的各种技术和工具。

## 调试策略

### 本地模式调试

最简单的调试方法是使用本地模式，它将在单个进程中顺序执行所有任务：

```python
import ray

# 使用本地模式进行调试
ray.init(local_mode=True)

@ray.remote
def problematic_task(x):
    # 这个函数将在同一个进程中执行
    # 可以使用常规的Python调试器
    result = x / (x - 5)  # 当x=5时会出现除零错误
    return result

try:
    result = ray.get(problematic_task.remote(5))
except Exception as e:
    print(f"捕获错误: {e}")

ray.shutdown()
```

### 日志记录

使用详细的日志记录来调试Ray应用程序：

```python
import ray
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

ray.init(
    logging_level=logging.DEBUG,
    log_to_driver=True
)

@ray.remote
def logging_task(value):
    logger.info(f"处理值: {value}")
    result = value * 2
    logger.info(f"结果: {result}")
    return result

result = ray.get(logging_task.remote(10))
print(f"最终结果: {result}")

ray.shutdown()
```

## Ray Dashboard调试

Ray Dashboard提供了一个可视化界面来监控和调试Ray应用程序：

```python
import ray

# 启动Ray时启用Dashboard
ray.init(
    include_dashboard=True,
    dashboard_host="0.0.0.0",
    dashboard_port=8265
)

# Dashboard将在 http://<your-host>:8265 可用
# 提供以下调试信息：
# - 任务执行时间线
# - 资源使用情况
# - 节点状态
# - 对象存储使用情况

# 您的Ray代码...
@ray.remote
def example_task():
    return "Hello from Ray!"

result = ray.get(example_task.remote())
print(result)

ray.shutdown()
```

## 错误处理和调试

### 捕获远程任务错误

```python
import ray
import traceback

@ray.remote
def error_task():
    raise ValueError("这是一个示例错误")

try:
    result = ray.get(error_task.remote())
except ray.exceptions.RayTaskError as e:
    print(f"任务执行错误: {e}")
    print(f"原始错误: {e.cause}")
    print(f"堆栈跟踪: {e.traceback}")
except Exception as e:
    print(f"其他错误: {e}")
```

### 调试Actor错误

```python
import ray

@ray.remote
class DebuggingActor:
    def __init__(self):
        self.state = 0
        print("Actor已初始化")
    
    def update(self, value):
        print(f"更新状态: {self.state} -> {value}")
        self.state = value
        return self.state
    
    def get_state(self):
        print(f"当前状态: {self.state}")
        return self.state

# 创建actor
actor = DebuggingActor.remote()

# 调试actor交互
new_state = ray.get(actor.update.remote(42))
print(f"新状态: {new_state}")

current_state = ray.get(actor.get_state.remote())
print(f"当前状态: {current_state}")
```

## 性能调试

### 任务时间分析

```python
import ray
import time

@ray.remote
def timing_task(task_id):
    start_time = time.time()
    # 模拟一些工作
    time.sleep(1)
    end_time = time.time()
    print(f"任务 {task_id} 执行时间: {end_time - start_time:.2f}秒")
    return task_id

# 提交多个任务
tasks = [timing_task.remote(i) for i in range(5)]
results = ray.get(tasks)
print(f"完成任务: {results}")
```

### 内存使用调试

```python
import ray

ray.init(object_store_memory=100 * 1024 * 1024)  # 100MB对象存储

@ray.remote
def memory_task():
    # 创建一些数据来监控内存使用
    import numpy as np
    data = np.random.random((1000, 1000))  # 约8MB
    return data.nbytes

# 检查集群资源
resources = ray.cluster_resources()
print(f"可用对象存储内存: {resources.get('object_store_memory', 0)}")

# 执行内存密集型任务
result = ray.get(memory_task.remote())
print(f"创建的数据大小: {result} 字节")

ray.shutdown()
```

## 调试工具

### 使用pdb调试器

虽然不能直接在远程任务中使用pdb，但可以在驱动程序中使用：

```python
import ray
import pdb

ray.init()

@ray.remote
def debug_with_pdb(x):
    return x * 2

# 在驱动程序中使用pdb
def main():
    pdb.set_trace()  # 设置断点
    result_ref = debug_with_pdb.remote(10)
    result = ray.get(result_ref)
    print(f"结果: {result}")

if __name__ == "__main__":
    main()
```

### 自定义调试装饰器

创建一个用于调试的装饰器：

```python
import ray
import functools
import time

def debug_remote(func):
    """装饰器用于调试远程函数"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"调用函数: {func.__name__}")
        print(f"参数: args={args}, kwargs={kwargs}")
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"函数 {func.__name__} 成功执行，耗时: {end_time - start_time:.2f}秒")
            return result
        except Exception as e:
            end_time = time.time()
            print(f"函数 {func.__name__} 执行失败，耗时: {end_time - start_time:.2f}秒")
            print(f"错误: {e}")
            raise
    return wrapper

@ray.remote
@debug_remote
def decorated_task(x):
    return x * x

result = ray.get(decorated_task.remote(5))
print(f"结果: {result}")
```

## 常见调试问题

1. **序列化错误**：确保传递给远程函数的对象可以被pickle序列化
2. **资源争用**：检查是否正确指定了任务资源需求
3. **网络问题**：在集群环境中验证节点间的网络连接
4. **内存泄漏**：监控对象存储使用情况，及时释放不需要的对象引用

## 调试最佳实践

1. **从本地模式开始**：在分布式部署前先在本地模式下测试
2. **使用详细日志**：在开发过程中启用详细日志记录
3. **监控资源使用**：定期检查CPU、内存和GPU使用情况
4. **逐步扩展**：从简单任务开始，逐步增加复杂性
5. **使用Dashboard**：利用Ray Dashboard进行可视化调试