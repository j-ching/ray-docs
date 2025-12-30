# Ray Actors

Ray Actors是Ray的核心概念之一，代表有状态的远程对象。Actor允许您在集群中维护状态，并在其上执行方法。它们是构建分布式应用程序的重要组件。

## 什么是Ray Actors？

Ray Actor是一个有状态的远程对象，可以在Ray集群的节点上执行。与无状态的远程函数不同，Actor在创建时初始化状态，并且可以在其生命周期内保持和更新该状态。每次调用Actor的方法都会更新或访问其内部状态。

## 基本用法

### 创建和使用Actor

```python
import ray

# 定义Actor类
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
    
    def get_count(self):
        return self.count

# 初始化Ray
ray.init()

# 创建Actor实例
counter = Counter.remote()

# 调用Actor方法
result = counter.increment.remote()
count = ray.get(result)
print(count)  # 输出: 1

# 再次调用方法，状态保持
result = counter.increment.remote()
count = ray.get(result)
print(count)  # 输出: 2

# 获取当前状态
result = counter.get_count.remote()
count = ray.get(result)
print(count)  # 输出: 2
```

### Actor方法的异步调用

```python
@ray.remote
class AsyncCounter:
    def __init__(self):
        self.count = 0
    
    async def async_increment(self):
        import asyncio
        await asyncio.sleep(0.1)  # 模拟异步操作
        self.count += 1
        return self.count
    
    def get_count(self):
        return self.count

# 创建异步Actor
async_counter = AsyncCounter.remote()

# 异步调用方法
async_result = async_counter.async_increment.remote()
result = ray.get(async_result)
print(result)  # 输出: 1
```

## Actor配置选项

### 资源分配

```python
@ray.remote(num_cpus=2, num_gpus=1, memory=1000000000)
class ResourceIntensiveActor:
    def __init__(self):
        # 初始化需要大量资源的操作
        self.data = [0] * 1000000  # 占用内存
    
    def process(self, data):
        # 需要CPU和GPU资源的处理
        return f"Processed {len(data)} items"

# 创建需要特定资源的Actor
gpu_actor = ResourceIntensiveActor.remote()
```

### Actor命名

```python
@ray.remote
class NamedActor:
    def __init__(self, name):
        self.name = name
        self.data = []
    
    def add_data(self, item):
        self.data.append(item)
        return f"Added {item} to {self.name}"
    
    def get_data(self):
        return self.data

# 使用命名Actor（可用于共享）
named_actor = NamedActor.options(name="global_actor").remote("MyActor")
```

## 高级Actor功能

### Actor生命周期管理

```python
@ray.remote
class LifecycleActor:
    def __init__(self):
        print("Actor正在初始化")
        self.initialized = True
        self.data = []
    
    def __ray_terminate__(self, actor_id):
        """Actor终止时调用"""
        print(f"Actor {actor_id} 正在终止")
        # 清理资源
        self.data = None
    
    def add_item(self, item):
        if not self.initialized:
            raise RuntimeError("Actor未初始化")
        self.data.append(item)
        return len(self.data)
    
    def cleanup(self):
        """手动清理方法"""
        self.data.clear()
        return "清理完成"

# 创建和使用Actor
actor = LifecycleActor.remote()
result = actor.add_item.remote("test")
length = ray.get(result)
print(f"数据长度: {length}")

# 清理Actor
cleanup_result = actor.cleanup.remote()
message = ray.get(cleanup_result)
print(message)
```

### Actor方法批处理

```python
@ray.remote
class BatchProcessor:
    def __init__(self):
        self.buffer = []
        self.max_batch_size = 10
    
    def add_to_batch(self, item):
        self.buffer.append(item)
        
        if len(self.buffer) >= self.max_batch_size:
            return self.process_batch()
        return None
    
    def process_batch(self):
        batch_data = self.buffer.copy()
        self.buffer.clear()
        
        # 批处理逻辑
        processed = [f"processed_{item}" for item in batch_data]
        return processed
    
    def force_process(self):
        return self.process_batch()

# 使用批处理Actor
processor = BatchProcessor.remote()

# 添加多个项目
for i in range(15):
    result = processor.add_to_batch.remote(f"item_{i}")
    batch_result = ray.get(result)
    if batch_result:
        print(f"批处理结果: {batch_result}")

# 强制处理剩余项目
final_result = processor.force_process.remote()
remaining = ray.get(final_result)
print(f"剩余批处理: {remaining}")
```

## 并发和同步

### Actor方法的串行执行

```python
@ray.remote
class SequentialActor:
    def __init__(self):
        self.state = 0
        self.history = []
    
    def update_state(self, value):
        # Actor方法是串行执行的，保证线程安全
        old_state = self.state
        self.state = value
        self.history.append((old_state, value))
        return self.state
    
    def get_history(self):
        return self.history

# 创建Actor
seq_actor = SequentialActor.remote()

# 并发调用，但Actor内部方法串行执行
futures = []
for i in range(5):
    future = seq_actor.update_state.remote(i * 10)
    futures.append(future)

# 获取所有结果
results = ray.get(futures)
print(f"结果: {results}")  # [0, 10, 20, 30, 40]

# 获取历史记录
history = ray.get(seq_actor.get_history.remote())
print(f"历史: {history}")
```

### 使用Ray内部Actor进行同步

```python
@ray.remote
class StatefulProcessor:
    def __init__(self):
        self.processed_items = 0
        self.pending_items = []
        self.is_processing = False
    
    def submit_item(self, item):
        self.pending_items.append(item)
        return f"Submitted item {item}, queue size: {len(self.pending_items)}"
    
    def process_next(self):
        if not self.pending_items:
            return "No items to process"
        
        item = self.pending_items.pop(0)
        self.processed_items += 1
        
        # 模拟处理
        processed_item = f"processed_{item}"
        return f"Processed {processed_item}, total: {self.processed_items}"
    
    def get_status(self):
        return {
            "processed": self.processed_items,
            "pending": len(self.pending_items),
            "processing": self.is_processing
        }

processor = StatefulProcessor.remote()

# 提交多个项目
for i in range(5):
    result = processor.submit_item.remote(f"data_{i}")
    print(ray.get(result))

# 处理项目
for i in range(6):  # 多处理一个以测试空队列
    result = processor.process_next.remote()
    print(ray.get(result))
```

## Actor通信模式

### Actor之间的通信

```python
@ray.remote
class MessageQueue:
    def __init__(self):
        self.messages = []
    
    def send_message(self, message):
        self.messages.append(message)
        return f"Message sent: {message}"
    
    def get_messages(self):
        msgs = self.messages.copy()
        self.messages.clear()
        return msgs

@ray.remote
class MessageProcessor:
    def __init__(self, queue_handle):
        self.queue = queue_handle
        self.processed_count = 0
    
    def process_messages(self):
        messages = ray.get(self.queue.get_messages.remote())
        self.processed_count += len(messages)
        
        processed = []
        for msg in messages:
            processed_msg = f"PROCESSED: {msg}"
            processed.append(processed_msg)
        
        return {
            "processed": processed,
            "count": self.processed_count
        }

# 创建消息队列
queue = MessageQueue.remote()

# 发送消息
ray.get(queue.send_message.remote("Hello"))
ray.get(queue.send_message.remote("World"))

# 创建处理器并连接到队列
processor = MessageProcessor.remote(queue)

# 处理消息
result = ray.get(processor.process_messages.remote())
print(result)
```

### Actor与远程函数的交互

```python
@ray.remote
class DataAggregator:
    def __init__(self):
        self.data = []
    
    def add_data(self, data):
        self.data.append(data)
        return len(self.data)
    
    def get_aggregated_data(self):
        return self.data

@ray.remote
def process_aggregated_data(aggregator_handle):
    # 从Actor获取数据
    data = ray.get(aggregator_handle.get_aggregated_data.remote())
    
    # 处理数据
    result = sum(data) if data else 0
    return result

# 创建聚合器
aggregator = DataAggregator.remote()

# 添加数据
for i in range(5):
    ray.get(aggregator.add_data.remote(i + 1))

# 通过远程函数处理Actor的数据
result = process_aggregated_data.remote(aggregator)
final_result = ray.get(result)
print(f"聚合结果: {final_result}")  # 输出: 15 (1+2+3+4+5)
```

## Actor容错性

### Actor重启策略

```python
@ray.remote(max_restarts=5, max_task_retries=3)
class FaultTolerantActor:
    def __init__(self):
        self.restarts = 0
        self.data = []
        print("FaultTolerantActor 初始化")
    
    def __ray_terminate__(self, actor_id):
        print(f"Actor {actor_id} 终止，重启次数: {self.restarts}")
    
    def risky_operation(self, should_fail=False):
        if should_fail:
            raise RuntimeError("模拟故障")
        return "成功执行"
    
    def add_data(self, item):
        self.data.append(item)
        return f"添加项目: {item}, 总数: {len(self.data)}"

# 创建容错Actor
ft_actor = FaultTolerantActor.remote()

# 成功操作
result = ft_actor.add_data.remote("item1")
print(ray.get(result))

# 尝试失败操作，会触发重启
try:
    result = ft_actor.risky_operation.remote(should_fail=True)
    ray.get(result)
except ray.exceptions.RayTaskError:
    print("任务失败，但Actor会自动重启")
```

## Actor性能优化

### Actor池模式

```python
@ray.remote
class WorkerActor:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.task_count = 0
    
    def process(self, task_data):
        self.task_count += 1
        # 模拟处理任务
        result = f"Worker {self.worker_id} processed: {task_data}"
        return result

class ActorPool:
    def __init__(self, actor_class, num_actors, *args, **kwargs):
        self.actors = [
            actor_class.remote(*args, **kwargs, worker_id=i) 
            for i in range(num_actors)
        ]
        self.free_actors = self.actors.copy()
        self.task_queue = []
    
    def submit(self, method_name, *args, **kwargs):
        if self.free_actors:
            actor = self.free_actors.pop()
            task = getattr(actor, method_name).remote(*args, **kwargs)
            return task
        else:
            # 如果没有空闲Actor，可以排队或返回错误
            raise RuntimeError("没有空闲的Actor")
    
    def return_actor(self, actor):
        if actor not in self.free_actors:
            self.free_actors.append(actor)

# 使用Actor池
pool = ActorPool(WorkerActor, 3)

# 提交多个任务
tasks = []
for i in range(5):
    task = pool.submit("process", f"task_{i}")
    tasks.append(task)

# 获取结果
results = ray.get(tasks)
for result in results:
    print(result)
```

## 实际应用场景

### 分布式缓存

```python
@ray.remote
class DistributedCache:
    def __init__(self, max_size=1000):
        self.cache = {}
        self.max_size = max_size
        self.access_count = {}
    
    def put(self, key, value):
        if len(self.cache) >= self.max_size:
            # 简单的LRU：删除最少访问的项目
            if self.access_count:
                least_accessed = min(self.access_count, key=self.access_count.get)
                del self.cache[least_accessed]
                del self.access_count[least_accessed]
        
        self.cache[key] = value
        self.access_count[key] = 0
        return f"Put {key}: {value}"
    
    def get(self, key):
        if key in self.cache:
            self.access_count[key] += 1
            return self.cache[key]
        return None
    
    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            del self.access_count[key]
            return True
        return False
    
    def stats(self):
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "keys": list(self.cache.keys())
        }

# 创建分布式缓存
cache = DistributedCache.remote()

# 使用缓存
ray.get(cache.put.remote("key1", "value1"))
ray.get(cache.put.remote("key2", "value2"))

value = ray.get(cache.get.remote("key1"))
print(f"获取的值: {value}")

stats = ray.get(cache.stats.remote())
print(f"缓存统计: {stats}")
```

### 状态机实现

```python
@ray.remote
class StateMachine:
    def __init__(self):
        self.state = "INIT"
        self.transitions = {
            "INIT": ["STARTING", "ERROR"],
            "STARTING": ["RUNNING", "ERROR"],
            "RUNNING": ["STOPPING", "ERROR"],
            "STOPPING": ["STOPPED", "ERROR"],
            "STOPPED": ["STARTING", "ERROR"],
            "ERROR": ["INIT"]
        }
        self.history = []
    
    def transition(self, new_state):
        if new_state in self.transitions.get(self.state, []):
            old_state = self.state
            self.state = new_state
            self.history.append((old_state, new_state))
            return f"从 {old_state} 过渡到 {new_state}"
        else:
            return f"无法从 {self.state} 过渡到 {new_state}"
    
    def get_state(self):
        return self.state
    
    def get_history(self):
        return self.history

# 使用状态机
sm = StateMachine.remote()

print(ray.get(sm.get_state.remote()))  # INIT

print(ray.get(sm.transition.remote("STARTING")))  # 状态转换
print(ray.get(sm.transition.remote("RUNNING")))   # 状态转换
print(ray.get(sm.transition.remote("STOPPING")))  # 状态转换
print(ray.get(sm.transition.remote("STOPPED")))   # 状态转换

print(ray.get(sm.get_state.remote()))  # STOPPED
print(ray.get(sm.get_history.remote()))  # 显示历史转换
```

## 最佳实践

### Actor设计原则

1. **状态最小化**: 尽量减少Actor的内部状态
2. **方法简单**: 保持Actor方法简单、专注
3. **错误处理**: 实现适当的错误处理和恢复机制
4. **资源管理**: 正确管理内存和其他资源

### 性能考虑

1. **批处理**: 对于小操作，考虑批处理以减少开销
2. **状态更新**: 避免频繁的状态更新
3. **并发模式**: 根据需要选择合适的并发模式

```python
# 示例：遵循最佳实践的Actor
@ray.remote
class BestPracticeActor:
    def __init__(self):
        self.simple_state = 0
        self.event_log = []  # 限制日志大小
        self.max_log_size = 1000
    
    def update(self, value):
        """简单、专注的方法"""
        self.simple_state = value
        self._add_to_log(f"update: {value}")
        return self.simple_state
    
    def _add_to_log(self, event):
        """内部方法，管理日志大小"""
        self.event_log.append(event)
        if len(self.event_log) > self.max_log_size:
            self.event_log = self.event_log[-500:]  # 保留最近500个事件
    
    def get_state(self):
        """只返回必要的状态"""
        return {
            "current": self.simple_state,
            "log_size": len(self.event_log)
        }

# 使用最佳实践Actor
actor = BestPracticeActor.remote()
result = actor.update.remote(42)
state = ray.get(result)
print(state)
```

Ray Actors通过提供有状态的远程对象，使构建复杂的分布式应用程序成为可能。它们在保持状态的同时提供并行处理能力，是Ray生态系统的重要组成部分。