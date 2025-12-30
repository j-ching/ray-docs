# Workflows快速入门

Ray Workflows是Ray的分布式工作流引擎，用于构建、运行和管理有状态的长运行分布式应用程序。本节介绍如何使用Ray Workflows。

## 安装Ray Workflows

```bash
pip install "ray[workflows]"
```

## 基本概念

Ray Workflows的主要组件：

- **Workflow**：有状态的分布式应用程序
- **Step**：工作流中的单个执行步骤
- **Checkpoint**：工作流状态的持久化
- **Resume**：从检查点恢复工作流执行

## 简单工作流示例

```python
import ray
from ray import workflow

# 初始化Ray和Workflows
ray.init()
workflow.init()

# 定义一个简单的步骤
@workflow.step
def simple_step(x):
    return x + 1

# 运行工作流
result = simple_step.step(10).run("simple_example")
print(result)  # 输出: 11
```

## 有状态工作流

```python
@workflow.step
def counter_step(current_count: int) -> int:
    new_count = current_count + 1
    print(f"计数器: {current_count} -> {new_count}")
    return new_count

# 运行计数器工作流
initial_count = 0
final_count = counter_step.step(initial_count).run("counter_workflow")
print(f"最终计数: {final_count}")
```

## 复杂工作流模式

### 顺序工作流

```python
@workflow.step
def step_one(x):
    result = x * 2
    print(f"步骤1: {x} -> {result}")
    return result

@workflow.step
def step_two(x):
    result = x + 10
    print(f"步骤2: {x} -> {result}")
    return result

@workflow.step
def step_three(x):
    result = x ** 2
    print(f"步骤3: {x} -> {result}")
    return result

# 创建顺序工作流
workflow_result = (
    step_one.step(5)
    .then(lambda x: step_two.step(x))
    .then(lambda x: step_three.step(x))
    .run("sequential_workflow")
)

print(f"顺序工作流结果: {workflow_result}")  # 输出: 400 (5*2+10=20, 20^2=400)
```

### 并行工作流

```python
@workflow.step
def parallel_task(x):
    import time
    time.sleep(1)  # 模拟工作
    return x * x

@workflow.step
def combine_results(results):
    return sum(results)

# 并行执行多个任务
tasks = [parallel_task.step(i) for i in range(1, 5)]
combined = combine_results.step(tasks).run("parallel_workflow")
print(f"并行工作流结果: {combined}")  # 输出: 30 (1^2 + 2^2 + 3^2 + 4^2 = 30)
```

### 条件工作流

```python
@workflow.step
def check_condition(value):
    return value > 10

@workflow.step
def process_large_value(value):
    return f"大值: {value}"

@workflow.step
def process_small_value(value):
    return f"小值: {value}"

@workflow.step
def conditional_workflow(value):
    is_large = check_condition.step(value)
    if is_large:
        return process_large_value.step(value)
    else:
        return process_small_value.step(value)

# 运行条件工作流
result = conditional_workflow.step(15).run("conditional_workflow")
print(result)  # 输出: "大值: 15"
```

## 工作流持久化和恢复

```python
@workflow.step
def long_running_step(x):
    import time
    # 模拟长时间运行的任务
    time.sleep(2)
    return x * 2

# 运行工作流
workflow_id = "long_running_example"
result = long_running_step.step(21).run(workflow_id)
print(f"结果: {result}")

# 恢复工作流（如果需要）
try:
    # 尝试恢复工作流
    restored_result = workflow.resume(workflow_id)
    print(f"恢复结果: {restored_result}")
except Exception:
    print("工作流已完成或不存在")
```

## 工作流嵌套

```python
@workflow.step
def inner_workflow(x):
    @workflow.step
    def inner_step(y):
        return y + 100
    
    return inner_step.step(x).run()

@workflow.step
def outer_workflow(x):
    inner_result = inner_workflow.step(x)
    return inner_result * 2

# 运行嵌套工作流
result = outer_workflow.step(5).run("nested_workflow")
print(f"嵌套工作流结果: {result}")  # 输出: 210 ((5+100)*2)
```

## 错误处理和重试

```python
@workflow.step
def unreliable_step(x, attempt_count=0):
    import random
    
    # 模拟偶尔失败的任务
    if random.random() < 0.5 and attempt_count < 3:
        print(f"尝试 {attempt_count + 1}: 步骤失败")
        raise ValueError(f"模拟错误，尝试次数: {attempt_count + 1}")
    
    print(f"尝试 {attempt_count + 1}: 步骤成功")
    return x * 2

# 运行可能失败的工作流步骤
try:
    result = unreliable_step.step(10).run("reliable_workflow")
    print(f"结果: {result}")
except Exception as e:
    print(f"工作流执行失败: {e}")
```

## 工作流管理

```python
# 列出所有工作流
all_workflows = workflow.list_all()
print(f"所有工作流: {all_workflows}")

# 获取特定工作流状态
try:
    status = workflow.get_status("simple_example")
    print(f"工作流状态: {status}")
except Exception:
    print("工作流不存在")

# 获取工作流输出
try:
    output = workflow.get_output("simple_example")
    print(f"工作流输出: {output}")
except Exception:
    print("无法获取工作流输出")
```

## 实际应用示例：数据处理管道

```python
@workflow.step
def extract_data(source):
    """从源提取数据"""
    print(f"从 {source} 提取数据")
    return [1, 2, 3, 4, 5]

@workflow.step
def transform_data(data, multiplier):
    """转换数据"""
    print(f"转换数据，乘数: {multiplier}")
    return [x * multiplier for x in data]

@workflow.step
def load_data(data, destination):
    """加载数据到目标"""
    print(f"将数据加载到 {destination}")
    print(f"数据: {data}")
    return f"已加载 {len(data)} 项数据到 {destination}"

@workflow.step
def data_pipeline(source, destination, multiplier):
    """完整的数据处理管道"""
    raw_data = extract_data.step(source)
    processed_data = transform_data.step(raw_data, multiplier)
    result = load_data.step(processed_data, destination)
    return result

# 运行数据处理管道
pipeline_result = data_pipeline.step(
    "source_db", 
    "target_db", 
    10
).run("data_pipeline_example")

print(f"管道结果: {pipeline_result}")
```

## 工作流监控

```python
@workflow.step
def monitored_step(x):
    # 报告指标
    print(f"处理值: {x}")
    result = x ** 2
    print(f"结果: {result}")
    return result

# 运行工作流并监控
result = monitored_step.step(7).run("monitored_workflow")
print(f"监控结果: {result}")
```

## 最佳实践

1. **幂等性**：确保工作流步骤是幂等的，以便安全重试
2. **检查点**：利用Ray Workflows的自动检查点功能
3. **错误处理**：实现适当的错误处理和恢复逻辑
4. **资源管理**：合理分配资源以避免资源争用
5. **监控**：监控工作流执行状态和性能
6. **清理**：定期清理完成的工作流以节省存储空间

## 高级特性

### 工作流事件监听

```python
def workflow_callback(event):
    print(f"工作流事件: {event}")

# 注册事件回调（如果支持）
# workflow.on_event("completed", workflow_callback)
```

### 动态工作流构建

```python
@workflow.step
def dynamic_workflow(steps_config):
    """根据配置动态构建工作流"""
    result = 0
    for config in steps_config:
        operation = config["operation"]
        value = config["value"]
        
        if operation == "add":
            result += value
        elif operation == "multiply":
            result *= value
    
    return result

# 动态配置
steps = [
    {"operation": "add", "value": 10},
    {"operation": "multiply", "value": 2},
    {"operation": "add", "value": 5}
]

dynamic_result = dynamic_workflow.step(steps).run("dynamic_workflow")
print(f"动态工作流结果: {dynamic_result}")  # 输出: 25 ((0+10)*2+5)
```

Ray Workflows提供了构建容错、可恢复的分布式应用程序的强大功能，特别适用于需要长时间运行和状态管理的场景。