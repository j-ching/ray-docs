# Ray配置

Ray提供了丰富的配置选项，允许您根据特定需求调整运行时行为、资源分配、性能参数等。本章将详细介绍Ray的各种配置选项和最佳实践。

## 配置概述

Ray配置主要分为以下几个方面：
- **启动配置**：启动Ray时的全局配置
- **任务配置**：远程函数和Actor的特定配置
- **集群配置**：多节点集群的配置
- **性能配置**：性能相关的调优参数

## 启动配置

### 基本启动配置

```python
import ray

# 基本启动配置
ray.init(
    num_cpus=8,                    # CPU核心数
    num_gpus=2,                    # GPU数量
    memory=10 * 1024 * 1024 * 1024,  # 10GB系统内存
    object_store_memory=2 * 1024 * 1024 * 1024,  # 2GB对象存储内存
    dashboard_host="0.0.0.0",      # Dashboard主机地址
    dashboard_port=8265,           # Dashboard端口
    include_dashboard=True,        # 包含Dashboard
    temp_dir="/tmp/ray",           # 临时目录
    logging_level="info",          # 日志级别
    log_to_driver=True             # 是否将日志输出到driver
)

# 关闭Ray
ray.shutdown()
```

### 高级启动配置

```python
import ray

# 高级启动配置
ray.init(
    # 资源配置
    resources={
        "custom_resource": 4,       # 自定义资源
        "special_accelerator": 2    # 特殊加速器
    },
    
    # 内存配置
    object_store_memory=4 * 1024 * 1024 * 1024,  # 4GB对象存储
    plasma_directory="/dev/shm",   # Plasma存储目录
    
    # 网络配置
    address="ray://head-node:10001",  # 连接到现有集群
    
    # 性能配置
    _system_config={
        "max_task_args_memory_fraction": 0.5,  # 任务参数内存限制
        "max_direct_call_object_size": 100 * 1024,  # 直接调用对象大小限制
        "worker_lease_timeout_milliseconds": 1000,  # 工作进程租约超时
        "ping_gcs_rpc_server_max_retries": 5,  # GCS RPC服务器ping重试次数
        "maximum_gcs_destroyed_actor_cached_count": 100000,  # 缓存销毁的Actor数量
        "maximum_gcs_destroyed_task_cached_count": 100000,  # 缓存销毁的任务数量
        "raylet_max_active_tasks": 1000000,  # 最大活跃任务数
        "max_task_queue_size": 1000000  # 最大任务队列大小
    },
    
    # 其他配置
    runtime_env={  # 运行时环境
        "pip": ["numpy", "pandas"],
        "conda": "environment.yml"
    }
)
```

### 系统配置参数详解

```python
# 重要的系统配置参数
system_config = {
    # 任务调度相关
    "max_task_args_memory_fraction": 0.5,           # 任务参数使用的对象存储内存比例
    "max_direct_call_object_size": 100 * 1024,     # 直接调用时对象的最大大小(字节)
    "worker_lease_timeout_milliseconds": 1000,     # 工作进程租约超时时间(毫秒)
    
    # GCS相关
    "ping_gcs_rpc_server_max_retries": 5,          # GCS RPC服务器ping最大重试次数
    "maximum_gcs_destroyed_actor_cached_count": 100000,   # GCS缓存销毁的Actor数量
    "maximum_gcs_destroyed_task_cached_count": 100000,    # GCS缓存销毁的任务数量
    "gcs_server_request_timeout_seconds": 5,       # GCS服务器请求超时时间
    
    # 调度器相关
    "raylet_max_active_tasks": 1000000,            # Raylet最大活跃任务数
    "max_task_queue_size": 1000000,                # 任务队列最大大小
    "max_waiting_tasks": 100000,                   # 最大等待任务数
    
    # 内存管理
    "object_spill_threshold": 0.8,                 # 对象溢出阈值
    "min_spilling_size": 1024 * 1024,             # 最小溢出大小(1MB)
    
    # 超时配置
    "task_retry_delay_ms": 1000,                   # 任务重试延迟(毫秒)
    "node_manager_forward_task_retry_timeout_ms": 10000,  # 节点管理器转发任务重试超时
    
    # 资源管理
    "max_resource_shapes_per_load_report": 10000,  # 每次负载报告的最大资源形状数
    "cpu_profiling_interval_ms": 100,              # CPU分析间隔(毫秒)
}
```

## 任务配置

### 远程函数配置

```python
import ray

@ray.remote(
    num_cpus=2,                    # 使用的CPU核心数
    num_gpus=0.5,                  # 使用的GPU数量（可以是小数）
    memory=1000000000,             # 内存需求（字节）
    object_store_memory=500000000, # 对象存储内存需求
    resources={"custom_resource": 1},  # 自定义资源需求
    max_calls=1000,                # 每个工作进程处理的最大调用次数
    max_retries=3,                 # 最大重试次数
    lifetime="detached",           # Actor生命周期（仅对Actor有效）
    name="my_remote_func",         # 任务名称
    namespace="my_namespace"       # 命名空间
)
def configured_remote_function(data):
    """带配置的远程函数"""
    return f"Processed: {data}"

# 使用配置化的远程函数
result = configured_remote_function.remote("test_data")
output = ray.get(result)
print(output)
```

### Actor配置

```python
@ray.remote(
    num_cpus=1,
    num_gpus=1,
    memory=2000000000,
    resources={"special_hardware": 1},
    max_concurrency=10,            # 最大并发数
    max_restarts=5,                # 最大重启次数
    max_task_retries=3,            # 最大任务重试次数
    lifetime="detached",           # 永久运行
    name="my_persistent_actor",    # Actor名称
    namespace="production"         # 命名空间
)
class ConfiguredActor:
    def __init__(self):
        self.data = []
    
    def process(self, item):
        self.data.append(item)
        return f"Processed {item}"

# 创建配置化的Actor
actor = ConfiguredActor.remote()
result = actor.process.remote("test_item")
output = ray.get(result)
print(output)
```

## 集群配置

### 集群配置文件

```yaml
# cluster.yaml - Ray集群配置示例
cluster_name: ray-cluster-prod

min_workers: 2
max_workers: 20
initial_workers: 5

# 防止自动停止空闲节点
autoscaling_mode: default
idle_timeout_minutes: 5

# 节点配置
head_node:
  InstanceType: m5.2xlarge
  ImageId: ami-0abcdef1234567890
  KeyName: my-key-pair
  SecurityGroupIds: [sg-xxxxxxxx]

worker_nodes:
  InstanceType: m5.large
  ImageId: ami-0abcdef1234567890
  KeyName: my-key-pair
  SecurityGroupIds: [sg-xxxxxxxx]

# 启动配置
setup_commands:
  - pip install ray[default]
  - pip install numpy pandas scikit-learn

# Ray启动命令
head_start_ray_commands:
  - ray stop
  - ulimit -n 65536; ray start --head --port=6379 --dashboard-host=0.0.0.0 --num-cpus=8 --num-gpus=1

worker_start_ray_commands:
  - ray stop
  - ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6379 --num-cpus=4 --num-gpus=0

# 文件挂载
file_mounts: {
    "/tmp/data": "/local/path/to/data"
}

# 高级资源管理
provider:
  type: aws
  region: us-west-2
  availability_zone: us-west-2a
  cache_stopped_nodes: false  # 设置为false以确保节点被实际终止
```

### 自动缩放配置

```python
# 自动缩放配置参数
autoscaling_config = {
    "upscaling_speed": 1.0,              # 扩展速度
    "idle_timeout_minutes": 5,            # 空闲超时（分钟）
    "initial_workers": 2,                 # 初始工作节点数
    "min_workers": 1,                     # 最小工作节点数
    "max_workers": 50,                    # 最大工作节点数
    "target_utilization_fraction": 0.8,   # 目标资源利用率
    "waiting_bundle_timeout_seconds": 300, # 等待bundle超时（秒）
    "pending_launch_timeout": 600,        # 挂起启动超时（秒）
}

# 资源需求配置
resource_demand_config = {
    "worker_nodes": {
        "InstanceType": "m5.large",
        "Resources": {
            "CPU": 2,
            "GPU": 0,
            "memory": 8 * 1024 * 1024 * 1024  # 8GB
        }
    }
}
```

## 环境配置

### 运行时环境

```python
# 运行时环境配置
runtime_env = {
    "pip": [
        "numpy>=1.18.0",
        "pandas>=1.2.0",
        "scikit-learn>=0.24.0"
    ],
    "conda": {
        "dependencies": [
            "python=3.8",
            "numpy",
            "pip",
            {
                "pip": [
                    "ray[default]",
                    "torch"
                ]
            }
        ]
    },
    "env_vars": {
        "OMP_NUM_THREADS": "1",
        "PYTHONPATH": "/path/to/my/module"
    },
    "working_dir": "/path/to/working/dir",  # 工作目录
    "excludes": ["/path/to/large/files"]    # 排除的文件/目录
}

# 使用运行时环境
@ray.remote(runtime_env=runtime_env)
def env_specific_task():
    import numpy as np
    import pandas as pd
    return f"Using numpy {np.__version__} and pandas {pd.__version__}"

result = env_specific_task.remote()
output = ray.get(result)
print(output)
```

### 环境变量配置

```python
# Ray相关的环境变量
environment_variables = {
    # 日志配置
    "RAY_LOG_LEVEL": "info",           # 日志级别
    "RAY_DISABLE_IMPORT_WARNING": "1", # 禁用导入警告
    
    # 内存配置
    "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE": "1",  # 允许慢速存储
    "RAY_memory_monitor_refresh_ms": "200",      # 内存监控刷新间隔
    
    # 网络配置
    "RAY_grpc_socket_options": "SO_REUSEPORT=1", # gRPC套接字选项
    
    # 调试配置
    "RAY_DEBUG_DISABLE_PUSH_METRICS": "1",       # 禁用推送指标
    "RAY_USAGE_STATS_ENABLED": "0",              # 禁用使用统计
}
```

## 性能调优配置

### 内存优化配置

```python
# 内存优化配置
memory_config = {
    # 对象存储配置
    "object_store_memory": 4 * 1024 * 1024 * 1024,  # 4GB对象存储
    "plasma_directory": "/dev/shm",                  # 使用内存文件系统
    
    # 溢出配置
    "object_spill_threshold": 0.9,                   # 溢出阈值
    "min_spilling_size": 10 * 1024 * 1024,         # 最小溢出大小(10MB)
    
    # 缓存配置
    "max_call_grpc_timeout_seconds": 120,            # GRPC调用超时
    "raylet_max_active_tasks": 500000,               # 最大活跃任务数
}

# 启动时应用内存配置
ray.init(
    object_store_memory=4 * 1024 * 1024 * 1024,
    _system_config={
        "object_spill_threshold": 0.9,
        "min_spilling_size": 10 * 1024 * 1024,
    }
)
```

### 并发优化配置

```python
# 并发优化配置
concurrency_config = {
    # 任务队列配置
    "max_task_queue_size": 500000,                   # 最大任务队列大小
    "max_waiting_tasks": 50000,                      # 最大等待任务数
    
    # 工作进程配置
    "worker_register_timeout_seconds": 60,           # 工作进程注册超时
    "max_io_workers": 4,                             # 最大IO工作进程数
    "min_worker_port": 10000,                        # 最小工作端口
    "max_worker_port": 65535,                        # 最大工作端口
    
    # 调度配置
    "scheduler_queue_size": 10000,                   # 调度器队列大小
    "max_tasks_per_tick": 1000,                      # 每tick最大任务数
}

# 应用并发配置
ray.init(_system_config=concurrency_config)
```

## 配置最佳实践

### 生产环境配置模板

```python
def get_production_ray_config():
    """生产环境Ray配置"""
    return {
        "num_cpus": None,  # 自动检测
        "num_gpus": None,  # 自动检测
        "memory": None,  # 自动检测
        "object_store_memory": None,  # 自动检测
        
        # 安全配置
        "include_dashboard": True,
        "dashboard_host": "0.0.0.0",
        "dashboard_port": 8265,
        
        # 性能配置
        "_system_config": {
            # 稳定性配置
            "ping_gcs_rpc_server_max_retries": 10,
            "gcs_server_request_timeout_seconds": 10,
            
            # 性能配置
            "max_task_args_memory_fraction": 0.5,
            "max_direct_call_object_size": 100 * 1024,
            "worker_lease_timeout_milliseconds": 1000,
            
            # 内存管理
            "object_spill_threshold": 0.9,
            "min_spilling_size": 10 * 1024 * 1024,
        },
        
        # 运行时环境
        "runtime_env": {
            "pip": ["ray[default]"],
            "env_vars": {
                "OMP_NUM_THREADS": "1",
                "RAY_DISABLE_IMPORT_WARNING": "1"
            }
        }
    }

# 使用生产配置
def initialize_ray_production():
    """初始化生产环境Ray"""
    config = get_production_ray_config()
    ray.init(**config)
    return ray.is_initialized()

# 初始化
is_prod = initialize_ray_production()
print(f"Ray生产环境初始化: {is_prod}")
```

### 配置验证工具

```python
def validate_ray_config(config):
    """验证Ray配置"""
    errors = []
    
    # 检查基本配置
    if "num_cpus" in config and config["num_cpus"] < 0:
        errors.append("num_cpus 不能为负数")
    
    if "num_gpus" in config and config["num_gpus"] < 0:
        errors.append("num_gpus 不能为负数")
    
    # 检查系统配置
    if "_system_config" in config:
        sys_config = config["_system_config"]
        if "object_spill_threshold" in sys_config:
            threshold = sys_config["object_spill_threshold"]
            if not (0 < threshold <= 1):
                errors.append("object_spill_threshold 必须在(0, 1]范围内")
    
    # 检查资源配置
    if "resources" in config:
        for resource_name, resource_value in config["resources"].items():
            if resource_value < 0:
                errors.append(f"资源 {resource_name} 的值不能为负数")
    
    return errors

# 验证配置示例
test_config = {
    "num_cpus": 4,
    "num_gpus": 1,
    "_system_config": {
        "object_spill_threshold": 0.8
    },
    "resources": {
        "custom_resource": 2
    }
}

validation_errors = validate_ray_config(test_config)
if validation_errors:
    print("配置验证错误:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("配置验证通过")
```

### 配置管理类

```python
class RayConfigManager:
    """Ray配置管理器"""
    
    def __init__(self):
        self.default_config = {
            "num_cpus": None,
            "num_gpus": None,
            "memory": None,
            "object_store_memory": None,
            "include_dashboard": True,
            "dashboard_host": "localhost",
            "dashboard_port": 8265,
        }
        
        self.system_config_presets = {
            "development": {
                "max_task_args_memory_fraction": 0.3,
                "max_direct_call_object_size": 50 * 1024,
                "worker_lease_timeout_milliseconds": 5000,
            },
            "production": {
                "max_task_args_memory_fraction": 0.5,
                "max_direct_call_object_size": 100 * 1024,
                "worker_lease_timeout_milliseconds": 1000,
            },
            "high_performance": {
                "max_task_args_memory_fraction": 0.7,
                "max_direct_call_object_size": 200 * 1024,
                "worker_lease_timeout_milliseconds": 100,
            }
        }
    
    def get_config(self, environment="development", custom_config=None):
        """获取配置"""
        config = self.default_config.copy()
        
        # 应用环境特定配置
        if environment in self.system_config_presets:
            config["_system_config"] = self.system_config_presets[environment].copy()
        
        # 应用自定义配置
        if custom_config:
            config.update(custom_config)
        
        return config
    
    def apply_config(self, config):
        """应用配置"""
        if ray.is_initialized():
            print("警告: Ray已经初始化，配置更改将被忽略")
            return False
        
        ray.init(**config)
        return True

# 使用配置管理器
config_manager = RayConfigManager()

# 获取开发环境配置
dev_config = config_manager.get_config("development")
print("开发环境配置:", dev_config)

# 获取生产环境配置
prod_config = config_manager.get_config("production", {
    "num_cpus": 16,
    "num_gpus": 4
})
print("生产环境配置:", prod_config)
```

Ray的配置系统提供了灵活的方式来调整运行时行为以满足不同场景的需求。正确配置Ray可以显著提高性能和稳定性。