# 日志记录

Ray提供了灵活的日志记录系统，帮助开发者调试和监控分布式应用程序。本节介绍如何配置和使用Ray的日志系统。

## 日志系统概述

Ray的日志系统包含多个组件：

- **驱动程序日志**：来自主程序的日志
- **工作进程日志**：来自Ray工作进程的日志
- **系统日志**：来自Ray运行时系统的日志
- **应用日志**：来自用户应用程序的日志

## 基本日志配置

### 初始化时配置日志

```python
import ray
import logging

# 配置Python日志记录器
logging.basicConfig(level=logging.INFO)

# 初始化Ray时配置日志
ray.init(
    logging_level=logging.DEBUG,      # 设置Ray日志级别
    log_to_driver=True,              # 将Ray日志输出到驱动程序
    num_cpus=2
)

# 在远程函数中使用日志
@ray.remote
def logged_task():
    logging.info("执行任务中...")
    logging.debug("调试信息")
    return "完成"

result = ray.get(logged_task.remote())
ray.shutdown()
```

### 日志级别

```python
import ray
import logging

# 不同的日志级别
ray.init(
    logging_level=logging.WARNING,    # 只记录警告及以上级别
    log_to_driver=True
)

@ray.remote
def task_with_logging():
    logging.debug("调试信息 - 不会显示")      # 级别低于WARNING
    logging.info("信息消息 - 不会显示")       # 级别低于WARNING
    logging.warning("警告消息 - 会显示")      # 级别等于WARNING
    logging.error("错误消息 - 会显示")        # 级别高于WARNING
    return "完成"

result = ray.get(task_with_logging.remote())
```

## 自定义日志格式

### 配置日志格式

```python
import ray
import logging

# 配置自定义日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

ray.init(log_to_driver=True)

@ray.remote
def formatted_logging_task():
    logger = logging.getLogger("ray.custom")
    logger.info("自定义格式的日志消息")
    logger.error("错误日志")
    return "日志完成"

result = ray.get(formatted_logging_task.remote())
```

## Ray特定日志配置

### 配置Ray组件日志

```python
import ray
import logging

# 为不同的Ray组件设置不同的日志级别
ray.init(
    logging_level=logging.INFO,
    log_to_driver=True
)

# 获取Ray特定的日志记录器
ray_logger = logging.getLogger("ray")
raylet_logger = logging.getLogger("ray.raylet")
core_worker_logger = logging.getLogger("ray.worker")

# 设置特定组件的日志级别
raylet_logger.setLevel(logging.WARNING)
core_worker_logger.setLevel(logging.INFO)
```

## 日志输出位置

### 日志文件输出

```python
import ray
import logging
import os

# 配置日志到文件
log_dir = "/tmp/ray_logs"
os.makedirs(log_dir, exist_ok=True)

# 创建文件处理器
file_handler = logging.FileHandler(os.path.join(log_dir, "ray_app.log"))
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(file_formatter)

# 配置日志记录器
logger = logging.getLogger("ray_app")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

ray.init(log_to_driver=True)

@ray.remote
def file_logging_task():
    logger.info("写入日志文件的消息")
    return "完成"

result = ray.get(file_logging_task.remote())
```

## 远程函数中的日志

### 在远程任务中使用日志

```python
import ray
import logging

ray.init(log_to_driver=True)

@ray.remote
def logging_task(task_id):
    logger = logging.getLogger(f"task_{task_id}")
    
    logger.info(f"任务 {task_id} 开始执行")
    
    # 模拟一些工作
    import time
    time.sleep(1)
    
    logger.info(f"任务 {task_id} 完成")
    
    return f"Task {task_id} result"

# 运行多个带日志的任务
task_ids = [1, 2, 3]
results = ray.get([logging_task.remote(tid) for tid in task_ids])
```

### 在Actor中使用日志

```python
import ray
import logging

@ray.remote
class LoggingActor:
    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.logger = logging.getLogger(f"actor_{actor_id}")
        self.logger.info(f"Actor {actor_id} 初始化")
    
    def do_work(self, work_id):
        self.logger.info(f"Actor {self.actor_id} 执行工作 {work_id}")
        
        # 模拟工作
        import time
        time.sleep(0.5)
        
        self.logger.info(f"工作 {work_id} 完成")
        return f"Actor {self.actor_id} 完成工作 {work_id}"
    
    def get_actor_id(self):
        return self.actor_id

ray.init(log_to_driver=True)

# 创建带日志的Actor
actor = LoggingActor.remote(1)
result = ray.get(actor.do_work.remote("work_1"))
print(result)
```

## 日志过滤和管理

### 自定义日志过滤器

```python
import ray
import logging

class TaskFilter(logging.Filter):
    """自定义日志过滤器"""
    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id
    
    def filter(self, record):
        # 只允许包含特定任务ID的消息通过
        return self.task_id in record.getMessage()

ray.init(log_to_driver=True)

@ray.remote
def filtered_logging_task(task_id):
    logger = logging.getLogger("filtered_logger")
    
    # 添加过滤器
    filter_handler = logging.StreamHandler()
    filter_handler.addFilter(TaskFilter(str(task_id)))
    logger.addHandler(filter_handler)
    logger.setLevel(logging.INFO)
    
    logger.info(f"Task {task_id} 开始")
    logger.info(f"Task X 不应该显示")  # 这条会被过滤
    logger.info(f"Task {task_id} 结束")
    
    return f"Task {task_id} 完成"

result = ray.get(filtered_logging_task.remote(42))
```

## 高级日志配置

### 结构化日志

```python
import ray
import logging
import json

class JSONFormatter(logging.Formatter):
    """JSON格式的日志格式化器"""
    def format(self, record):
        log_entry = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 添加额外字段
        if hasattr(record, 'task_id'):
            log_entry['task_id'] = record.task_id
            
        return json.dumps(log_entry)

ray.init(log_to_driver=True)

@ray.remote
def structured_logging_task(task_id):
    logger = logging.getLogger("structured_logger")
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    # 添加额外属性到日志记录
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.task_id = task_id
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    logger.info(f"结构化日志消息", extra={'task_id': task_id})
    
    # 恢复原来的工厂
    logging.setLogRecordFactory(old_factory)
    
    return "结构化日志完成"

result = ray.get(structured_logging_task.remote("task_123"))
```

## 日志监控和分析

### 集中式日志收集

```python
import ray
import logging
from datetime import datetime

# 模拟集中式日志记录
class CentralizedLogger:
    def __init__(self):
        self.logs = []
    
    def log(self, level, message, **kwargs):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            'extra': kwargs
        }
        self.logs.append(log_entry)
        print(f"[{level}] {message} - {kwargs}")

central_logger = CentralizedLogger()

@ray.remote
def centralized_logging_task(task_id):
    central_logger.log(
        "INFO", 
        f"Task {task_id} started", 
        task_id=task_id, 
        node="worker_node"
    )
    
    # 模拟工作
    import time
    time.sleep(0.1)
    
    central_logger.log(
        "INFO", 
        f"Task {task_id} completed", 
        task_id=task_id,
        duration=0.1
    )
    
    return f"Task {task_id} result"

ray.init(log_to_driver=True)
result = ray.get(centralized_logging_task.remote("task_456"))
```

## 最佳实践

### 日志最佳实践

```python
import ray
import logging
from functools import wraps

def log_execution(func):
    """装饰器：记录函数执行日志"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.info(f"开始执行 {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            logger.info(f"完成执行 {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"执行 {func.__name__} 时出错: {str(e)}")
            raise
    
    return wrapper

@ray.remote
class BestPracticeActor:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Actor初始化完成")
    
    @log_execution
    def process_data(self, data):
        """处理数据的方法"""
        self.logger.info(f"开始处理 {len(data) if hasattr(data, '__len__') else 'unknown'} 项数据")
        
        # 模拟数据处理
        processed = [x * 2 for x in data] if isinstance(data, list) else data
        
        self.logger.info("数据处理完成")
        return processed

ray.init(log_to_driver=True)

# 使用最佳实践的Actor
actor = BestPracticeActor.remote()
result = ray.get(actor.process_data.remote([1, 2, 3, 4, 5]))
print(f"结果: {result}")
```

## 日志性能考虑

### 高效日志记录

```python
import ray
import logging
import time

# 避免在热路径中进行昂贵的日志操作
@ray.remote
def efficient_logging_task(data_size):
    logger = logging.getLogger("efficient")
    
    # 使用条件日志记录避免不必要的字符串格式化
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"处理 {data_size} 大小的数据")
    
    # 模拟处理
    start_time = time.time()
    result = sum(range(data_size))
    end_time = time.time()
    
    # 只在必要时记录性能信息
    if end_time - start_time > 1.0:  # 如果处理时间超过1秒
        logger.warning(f"处理时间过长: {end_time - start_time:.2f}秒")
    
    return result

ray.init(log_to_driver=True)

# 运行高效日志记录任务
result = ray.get(efficient_logging_task.remote(1000000))
```

## 日志配置总结

Ray日志记录的关键配置选项：

1. **logging_level**: 控制Ray系统日志级别
2. **log_to_driver**: 是否将日志输出到驱动程序
3. **自定义日志格式**: 使用标准Python logging模块配置
4. **日志过滤**: 使用过滤器控制日志输出
5. **结构化日志**: 使用JSON等格式便于日志分析

通过合理配置日志系统，可以有效地监控和调试Ray分布式应用程序。