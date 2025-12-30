# Ray监控

Ray提供了全面的监控功能，帮助您了解集群状态、资源使用情况、任务执行情况和性能指标。本章将详细介绍Ray的监控工具和最佳实践。

## 监控概述

Ray的监控系统包括：
- **内置仪表板**：提供实时的集群和任务可视化
- **指标收集**：收集各种性能和资源指标
- **日志系统**：详细的日志记录功能
- **API接口**：程序化访问监控数据

## Ray Dashboard

Ray Dashboard是内置的Web界面，提供集群的实时可视化监控。

### 启动Dashboard

```python
import ray

# 启动Ray时启用Dashboard
ray.init(
    dashboard_host="0.0.0.0",  # 使Dashboard可以从外部访问
    dashboard_port=8265        # Dashboard端口
)

# 或者在集群模式下启动
# ray start --head --dashboard-host 0.0.0.0 --dashboard-port 8265
```

Dashboard默认在端口8265上运行，访问 `http://<your-server-ip>:8265` 查看。

### Dashboard功能

Dashboard提供以下功能：
- 集群状态概览
- 节点资源使用情况
- 任务和Actor监控
- 性能指标图表
- 日志查看

## 指标收集

### 启用指标收集

```python
import ray
from ray import serve

# 启动Ray时启用指标
ray.init(
    include_dashboard=True,
    dashboard_host="0.0.0.0"
)

# Ray会自动收集指标并提供Prometheus端点
# 指标可以在 http://<dashboard-host>:<dashboard-port>/metrics 访问
```

### 自定义指标

```python
import ray
from ray.util.metrics import Counter, Gauge, Histogram

@ray.remote
class InstrumentedActor:
    def __init__(self):
        # 创建自定义指标
        self.requests_counter = Counter(
            "my_app_requests_total",
            description="Total number of requests",
            tag_keys=("endpoint", "status")
        )
        self.active_requests = Gauge(
            "my_app_active_requests",
            description="Number of active requests",
            tag_keys=("endpoint",)
        )
        self.request_latency = Histogram(
            "my_app_request_latency_seconds",
            description="Request latency in seconds",
            boundaries=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
            tag_keys=("endpoint",)
        )
    
    def process_request(self, endpoint):
        import time
        start_time = time.time()
        
        # 增加活跃请求数
        self.active_requests.labels(endpoint=endpoint).inc()
        
        try:
            # 模拟处理时间
            time.sleep(0.1)
            
            # 记录延迟
            latency = time.time() - start_time
            self.request_latency.labels(endpoint=endpoint).observe(latency)
            
            # 增加请求计数
            self.requests_counter.labels(endpoint=endpoint, status="success").inc()
            
            return f"Processed {endpoint} in {latency:.3f}s"
        except Exception as e:
            self.requests_counter.labels(endpoint=endpoint, status="error").inc()
            raise
        finally:
            # 减少活跃请求数
            self.active_requests.labels(endpoint=endpoint).dec()

# 使用带指标的Actor
actor = InstrumentedActor.remote()
result = ray.get(actor.process_request.remote("api/v1/users"))
print(result)
```

## 程序化监控

### 获取集群状态

```python
import ray
from ray.experimental.state.api import get_node_info, get_task_info

# 获取节点信息
def get_cluster_status():
    if not ray.is_initialized():
        print("Ray未初始化")
        return
    
    # 获取节点列表
    nodes = ray.nodes()
    print(f"集群节点数: {len(nodes)}")
    
    for node in nodes:
        print(f"节点ID: {node['NodeID']}")
        print(f"节点地址: {node['NodeManagerAddress']}")
        print(f"节点状态: {node['alive']}")
        print(f"资源: {node['Resources']}")
        print("---")

# 获取任务信息
def get_task_status():
    # 获取任务列表（需要Ray的状态API）
    try:
        tasks = ray.tasks()  # 这个API可能需要特定版本
        print(f"运行中的任务数: {len(tasks)}")
    except:
        print("无法获取任务信息")

# 运行监控函数
get_cluster_status()
```

### 监控Actor状态

```python
@ray.remote
class MonitorableActor:
    def __init__(self, name):
        self.name = name
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
    
    def process(self, data):
        try:
            # 模拟处理
            import time
            time.sleep(0.01)
            self.processed_count += 1
            return f"Processed: {data}"
        except Exception as e:
            self.error_count += 1
            raise
    
    def get_stats(self):
        import time
        return {
            "name": self.name,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "uptime": time.time() - self.start_time,
            "success_rate": self.processed_count / max(1, self.processed_count + self.error_count)
        }

# 创建可监控的Actor
actors = [MonitorableActor.remote(f"worker_{i}") for i in range(3)]

# 提交一些任务
import time
for i in range(10):
    actor = actors[i % len(actors)]
    actor.process.remote(f"data_{i}")

# 检查统计信息
for i, actor in enumerate(actors):
    stats = ray.get(actor.get_stats.remote())
    print(f"Actor {i} 统计: {stats}")
```

## 日志记录

### Ray日志系统

```python
import ray
import logging

# 配置Python日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class LoggableActor:
    def __init__(self):
        self.logger = logging.getLogger(f"LoggableActor.{id(self)}")
    
    def do_work(self, task_id):
        self.logger.info(f"开始处理任务 {task_id}")
        
        try:
            # 模拟工作
            import time
            time.sleep(0.1)
            
            self.logger.info(f"任务 {task_id} 完成")
            return f"Task {task_id} completed"
        except Exception as e:
            self.logger.error(f"任务 {task_id} 失败: {e}")
            raise

# 使用带日志的Actor
actor = LoggableActor.remote()
result = ray.get(actor.do_work.remote("test_task"))
print(result)
```

### 结构化日志

```python
import json
import ray
from datetime import datetime

@ray.remote
class StructuredLogger:
    def __init__(self):
        self.logs = []
    
    def log_event(self, event_type, data, level="INFO"):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "event_type": event_type,
            "data": data,
            "node_id": ray.get_runtime_context().get_node_id()
        }
        
        # 添加到内存日志
        self.logs.append(log_entry)
        
        # 输出结构化日志
        print(json.dumps(log_entry))
        
        return log_entry
    
    def get_logs(self, limit=100):
        return self.logs[-limit:]

# 使用结构化日志记录器
logger_actor = StructuredLogger.remote()

# 记录不同类型的事件
ray.get(logger_actor.log_event.remote("task_start", {"task_id": "123", "worker": "A"}))
ray.get(logger_actor.log_event.remote("task_end", {"task_id": "123", "duration": 0.5}, "INFO"))
ray.get(logger_actor.log_event.remote("error", {"task_id": "123", "error": "timeout"}, "ERROR"))

# 获取日志
recent_logs = ray.get(logger_actor.get_logs.remote(5))
for log in recent_logs:
    print(json.dumps(log, indent=2))
```

## 性能监控

### 任务性能分析

```python
import time
import ray

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {}
    
    def time_function(self, func_name):
        """装饰器：测量函数执行时间"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # 记录指标
                    if func_name not in self.metrics:
                        self.metrics[func_name] = []
                    self.metrics[func_name].append(duration)
                    
                    print(f"{func_name} 执行时间: {duration:.4f}s")
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    print(f"{func_name} 执行失败，耗时: {duration:.4f}s, 错误: {e}")
                    raise
            return wrapper
        return decorator

# 性能监控装饰器
perf_monitor = PerformanceMonitor()

@ray.remote
class PerformanceTestActor:
    @perf_monitor.time_function("heavy_computation")
    def heavy_computation(self, size):
        """执行计算密集型任务"""
        import numpy as np
        data = np.random.random((size, size))
        result = np.linalg.svd(data)
        return len(result[0])
    
    @perf_monitor.time_function("data_processing")
    def data_processing(self, data):
        """数据处理任务"""
        processed = [x * 2 for x in data]
        return sum(processed)
    
    def get_performance_metrics(self):
        return self.perf_monitor.metrics

# 测试性能监控
actor = PerformanceTestActor.remote()

# 执行性能测试
for i in range(5):
    comp_result = actor.heavy_computation.remote(100)
    proc_result = actor.data_processing.remote(list(range(1000)))
    
    ray.get([comp_result, proc_result])

# 查看性能指标
print("性能指标:", perf_monitor.metrics)
```

### 资源使用监控

```python
@ray.remote
class ResourceMonitor:
    def __init__(self):
        import psutil
        self.psutil = psutil
        self.start_time = time.time()
    
    def get_system_resources(self):
        """获取系统资源使用情况"""
        return {
            "cpu_percent": self.psutil.cpu_percent(interval=1),
            "memory_percent": self.psutil.virtual_memory().percent,
            "memory_available_gb": self.psutil.virtual_memory().available / (1024**3),
            "disk_percent": self.psutil.disk_usage("/").percent,
            "uptime": time.time() - self.start_time
        }
    
    def get_ray_resources(self):
        """获取Ray资源使用情况"""
        return ray.available_resources()
    
    def get_combined_status(self):
        """获取综合状态"""
        system = self.get_system_resources()
        ray_resources = self.get_ray_resources()
        
        return {
            "system": system,
            "ray_resources": ray_resources,
            "timestamp": time.time()
        }

# 使用资源监控器
resource_monitor = ResourceMonitor.remote()

# 获取资源状态
status = ray.get(resource_monitor.get_combined_status.remote())
print("系统资源状态:")
for key, value in status.items():
    print(f"  {key}: {value}")
```

## 集群监控

### 节点健康检查

```python
@ray.remote
class NodeHealthChecker:
    def __init__(self):
        import psutil
        self.psutil = psutil
        self.thresholds = {
            "cpu_percent": 80,      # CPU使用率阈值
            "memory_percent": 85,   # 内存使用率阈值
            "disk_percent": 90      # 磁盘使用率阈值
        }
    
    def check_health(self):
        """检查节点健康状况"""
        health_status = {
            "cpu_healthy": self.psutil.cpu_percent() < self.thresholds["cpu_percent"],
            "memory_healthy": self.psutil.virtual_memory().percent < self.thresholds["memory_percent"],
            "disk_healthy": self.psutil.disk_usage("/").percent < self.thresholds["disk_percent"],
            "system_load": self.psutil.getloadavg()[0] if hasattr(self.psutil, 'getloadavg') else 0
        }
        
        health_status["overall_healthy"] = all([
            health_status["cpu_healthy"],
            health_status["memory_healthy"],
            health_status["disk_healthy"]
        ])
        
        return health_status
    
    def get_detailed_metrics(self):
        """获取详细指标"""
        return {
            "cpu_percent": self.psutil.cpu_percent(),
            "memory_used_gb": self.psutil.virtual_memory().used / (1024**3),
            "memory_total_gb": self.psutil.virtual_memory().total / (1024**3),
            "disk_used_gb": self.psutil.disk_usage("/").used / (1024**3),
            "disk_total_gb": self.psutil.disk_usage("/").total / (1024**3),
            "process_count": len(self.psutil.pids())
        }

# 健康检查
health_checker = NodeHealthChecker.remote()

health = ray.get(health_checker.check_health.remote())
detailed_metrics = ray.get(health_checker.get_detailed_metrics.remote())

print("健康状况:", health)
print("详细指标:", detailed_metrics)
```

## 监控最佳实践

### 监控策略

```python
import ray
from ray.util.state import list_actors, list_tasks

class ComprehensiveMonitor:
    def __init__(self):
        self.alert_thresholds = {
            "error_rate": 0.05,      # 错误率阈值
            "latency_ms": 1000,      # 延迟阈值
            "cpu_usage": 80,         # CPU使用率阈值
            "memory_usage": 85       # 内存使用率阈值
        }
    
    def check_cluster_health(self):
        """检查集群健康状况"""
        health_report = {
            "timestamp": time.time(),
            "alerts": [],
            "summary": {}
        }
        
        # 检查节点
        nodes = ray.nodes()
        alive_nodes = [n for n in nodes if n.get('alive', False)]
        health_report["summary"]["alive_nodes"] = len(alive_nodes)
        health_report["summary"]["total_nodes"] = len(nodes)
        
        if len(alive_nodes) < len(nodes):
            health_report["alerts"].append({
                "type": "node_failure",
                "message": f"{len(nodes) - len(alive_nodes)} 个节点离线"
            })
        
        # 检查资源使用
        for node in alive_nodes:
            resources = node.get('Resources', {})
            for resource, value in resources.items():
                if 'CPU' in resource and value < 0.1:  # CPU资源几乎耗尽
                    health_report["alerts"].append({
                        "type": "resource_exhausted",
                        "message": f"节点 {node['NodeID']} CPU资源不足"
                    })
        
        return health_report
    
    def generate_metrics_report(self):
        """生成指标报告"""
        report = {
            "timestamp": time.time(),
            "ray_version": ray.__version__,
            "total_objects": len(ray.objects()),
            "total_tasks": len(ray.tasks()) if hasattr(ray, 'tasks') else 0,
            "total_actors": len(list_actors()) if 'list_actors' in globals() else 0
        }
        
        return report

# 使用综合监控器
monitor = ComprehensiveMonitor()

# 生成健康报告
health_report = monitor.check_cluster_health()
print("健康报告:", json.dumps(health_report, indent=2))

# 生成指标报告
metrics_report = monitor.generate_metrics_report()
print("指标报告:", json.dumps(metrics_report, indent=2))
```

### 告警系统

```python
@ray.remote
class AlertSystem:
    def __init__(self):
        self.alerts = []
        self.alert_callbacks = []
        self.severity_levels = ["INFO", "WARNING", "ERROR", "CRITICAL"]
    
    def register_alert_callback(self, callback):
        """注册告警回调函数"""
        self.alert_callbacks.append(callback)
    
    def trigger_alert(self, severity, message, details=None):
        """触发告警"""
        alert = {
            "timestamp": time.time(),
            "severity": severity,
            "message": message,
            "details": details or {}
        }
        
        self.alerts.append(alert)
        
        # 执行回调函数
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                print(f"告警回调执行失败: {e}")
        
        # 根据严重性打印消息
        print(f"[{severity}] {message}")
        if details:
            print(f"  详细信息: {details}")
        
        return alert
    
    def get_alerts(self, since=None, severity_threshold="INFO"):
        """获取告警"""
        if since:
            alerts = [a for a in self.alerts if a["timestamp"] >= since]
        else:
            alerts = self.alerts[:]
        
        severity_idx = self.severity_levels.index(severity_threshold)
        filtered_alerts = [
            a for a in alerts 
            if self.severity_levels.index(a["severity"]) >= severity_idx
        ]
        
        return filtered_alerts

# 创建告警系统
alert_system = AlertSystem.remote()

# 注册告警回调（示例）
def email_alert_callback(alert):
    print(f"发送邮件告警: {alert['message']}")

# 这里可以实际注册回调函数
# ray.get(alert_system.register_alert_callback.remote(email_alert_callback))

# 触发一些告警
ray.get(alert_system.trigger_alert.remote("WARNING", "CPU使用率过高", {"cpu_percent": 95}))
ray.get(alert_system.trigger_alert.remote("ERROR", "任务执行失败", {"task_id": "task_123"}))

# 获取告警
recent_alerts = ray.get(alert_system.get_alerts.remote(severity_threshold="WARNING"))
print(f"最近告警数: {len(recent_alerts)}")
```

## 与外部监控系统集成

### Prometheus集成

```python
# 示例：如何将Ray指标导出到Prometheus
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import threading
import time

class PrometheusExporter:
    def __init__(self, port=8000):
        self.port = port
        self.setup_metrics()
        
        # 在后台启动Prometheus服务器
        self.server_thread = threading.Thread(
            target=start_http_server, 
            args=(self.port,), 
            daemon=True
        )
        self.server_thread.start()
    
    def setup_metrics(self):
        """设置Prometheus指标"""
        self.ray_tasks_total = Counter(
            'ray_tasks_total', 
            'Total number of Ray tasks', 
            ['status']
        )
        self.ray_task_duration = Histogram(
            'ray_task_duration_seconds',
            'Ray task duration in seconds',
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, float('inf')]
        )
        self.ray_active_actors = Gauge(
            'ray_active_actors',
            'Number of active Ray actors'
        )
    
    def record_task_completion(self, status, duration):
        """记录任务完成"""
        self.ray_tasks_total.labels(status=status).inc()
        self.ray_task_duration.observe(duration)
    
    def update_actor_count(self, count):
        """更新Actor计数"""
        self.ray_active_actors.set(count)

# 启动Prometheus导出器（在实际应用中）
# prometheus_exporter = PrometheusExporter(port=8000)
```

## 监控最佳实践

### 性能考虑

```python
@ray.remote
class EfficientMonitor:
    """
    高效监控器
    - 最小化监控开销
    - 批量收集指标
    - 异步报告
    """
    def __init__(self):
        self.metrics_buffer = []
        self.max_buffer_size = 100
        self.report_interval = 10  # 每10个指标报告一次
        self.last_report_time = time.time()
    
    def collect_metric(self, metric_name, value, tags=None):
        """收集指标"""
        metric = {
            "name": metric_name,
            "value": value,
            "timestamp": time.time(),
            "tags": tags or {}
        }
        
        self.metrics_buffer.append(metric)
        
        # 当缓冲区满或达到报告间隔时报告
        if (len(self.metrics_buffer) >= self.max_buffer_size or 
            time.time() - self.last_report_time > self.report_interval):
            self.flush_metrics()
    
    def flush_metrics(self):
        """刷新指标"""
        if not self.metrics_buffer:
            return
        
        # 在实际应用中，这里会将指标发送到监控系统
        batch = self.metrics_buffer[:]
        self.metrics_buffer = []
        self.last_report_time = time.time()
        
        print(f"报告 {len(batch)} 个指标")
        # 这里可以实现实际的指标发送逻辑
        # 例如发送到Prometheus、InfluxDB或其他监控系统

# 使用高效监控器
efficient_monitor = EfficientMonitor.remote()

# 模拟收集一些指标
for i in range(150):
    ray.get(efficient_monitor.collect_metric.remote(f"metric_{i%10}", i, {"tag": f"value_{i%3}"}))
```

Ray的监控功能为分布式应用程序提供了全面的可观测性。通过合理使用内置的Dashboard、自定义指标、日志记录和告警系统，您可以确保应用程序的健康运行并快速诊断问题。