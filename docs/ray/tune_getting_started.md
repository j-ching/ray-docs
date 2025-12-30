# Tune快速入门

Ray Tune是Ray的超参数调优库，提供了一个简单而强大的接口来进行分布式超参数搜索。本节介绍如何使用Ray Tune进行超参数优化。

## 安装Ray Tune

```bash
pip install "ray[tune]"
```

## 基本概念

Ray Tune的主要组件：

- **Tuner**：管理整个调优过程
- **Trainable**：定义要优化的函数或类
- **Search Algorithm**：定义如何搜索超参数空间
- **Scheduler**：定义如何调度和终止试验
- **Trial**：单个超参数配置的执行

## 简单示例

```python
from ray import tune
import ray

def objective(config):
    # 模拟训练过程
    for step in range(10):
        # 模拟损失函数，基于配置的参数
        loss = (config["x1"] - 2) ** 2 + (config["x2"] - 4) ** 2
        # 添加一些噪声
        loss += config["noise_level"] * tune.utils.util.random()
        
        # 报告当前结果
        tune.report(loss=loss, step=step)

# 定义搜索空间
search_space = {
    "x1": tune.uniform(-5, 5),
    "x2": tune.uniform(-5, 5),
    "noise_level": tune.uniform(0.0, 1.0)
}

# 创建Tuner
tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=10  # 运行10个试验
    )
)

# 运行调优
results = tuner.fit()

# 获取最佳结果
best_result = results.get_best_result()
print(f"最佳配置: {best_result.config}")
print(f"最佳结果: {best_result.metrics}")
```

## 使用搜索算法

```python
from ray.tune.search import ConcurrencyLimiter
from ray.tune.search.hyperopt import HyperOptSearch

# 使用HyperOpt搜索算法
hyperopt_search = HyperOptSearch(
    metric="loss",
    mode="min"
)

tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        search_alg=hyperopt_search,
        num_samples=20
    )
)

results = tuner.fit()
```

## 使用调度器

```python
from ray.tune.schedulers import ASHAScheduler

# 使用ASHA（Asynchronous Successive Halving Algorithm）调度器
asha_scheduler = ASHAScheduler(
    metric="loss",
    mode="min",
    max_t=10,      # 最大训练步数
    grace_period=1, # 最小资源分配
    reduction_factor=2
)

tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        scheduler=asha_scheduler,
        num_samples=50
    )
)

results = tuner.fit()
```

## 与机器学习框架集成

```python
from ray import tune
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

def train_mnist(config):
    # 创建简单的神经网络
    model = nn.Sequential(
        nn.Linear(784, config["hidden_size"]),
        nn.ReLU(),
        nn.Linear(config["hidden_size"], 10)
    )
    
    optimizer = optim.SGD(
        model.parameters(),
        lr=config["lr"],
        momentum=config["momentum"]
    )
    
    criterion = nn.CrossEntropyLoss()
    
    # 模拟训练过程
    for epoch in range(10):
        # 模拟损失计算
        loss = 1.0 / (epoch + 1) + config["lr"] + config["momentum"]
        
        # 报告指标
        tune.report(loss=loss, accuracy=1 - loss)

# 定义搜索空间
search_space = {
    "lr": tune.loguniform(0.001, 0.1),
    "momentum": tune.uniform(0.1, 0.9),
    "hidden_size": tune.choice([32, 64, 128, 256])
}

tuner = tune.Tuner(
    train_mnist,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=20,
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
```

## 条件搜索空间

```python
def conditional_objective(config):
    # 根据条件选择不同的参数
    if config["model_type"] == "cnn":
        # CNN特定的参数
        result = config["conv_filters"] * config["learning_rate"]
    else:
        # 其他模型类型的参数
        result = config["hidden_units"] * config["learning_rate"]
    
    tune.report(score=result)

# 条件搜索空间
conditional_search_space = {
    "model_type": tune.choice(["cnn", "transformer"]),
    "learning_rate": tune.loguniform(0.0001, 0.1),
    "conv_filters": tune.choice([32, 64, 128]),  # 仅CNN使用
    "hidden_units": tune.choice([64, 128, 256])  # 非CNN使用
}

tuner = tune.Tuner(
    conditional_objective,
    param_space=conditional_search_space,
    tune_config=tune.TuneConfig(
        num_samples=10
    )
)

results = tuner.fit()
```

## 网格搜索

```python
def grid_search_objective(config):
    # 简单的目标函数
    result = config["a"] * config["b"] + config["c"]
    tune.report(result=result)

# 网格搜索空间
grid_search_space = {
    "a": tune.grid_search([1, 2, 3]),
    "b": tune.grid_search([10, 20]),
    "c": tune.grid_search([0.1, 0.2])
}

# 这将运行 3 * 2 * 2 = 12 个试验
tuner = tune.Tuner(
    grid_search_objective,
    param_space=grid_search_space
)

results = tuner.fit()
```

## 实验配置

```python
from ray.air.config import RunConfig
from ray.air.checkpoint import CheckpointConfig

# 配置实验运行
run_config = RunConfig(
    name="my_experiment",  # 实验名称
    local_dir="./results",  # 结果保存目录
    stop={"training_iteration": 100},  # 停止条件
    checkpoint_config=CheckpointConfig(
        checkpoint_frequency=2,  # 每2次迭代保存一次检查点
        checkpoint_at_end=True   # 训练结束时保存检查点
    )
)

tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=10,
        metric="loss",
        mode="min"
    ),
    run_config=run_config
)

results = tuner.fit()
```

## 分布式调优

```python
import ray

# 初始化Ray集群
ray.init(address="auto")  # 连接到现有集群

def distributed_objective(config):
    # 在分布式环境中运行的函数
    import time
    import random
    
    # 模拟训练时间
    time.sleep(random.uniform(1, 5))
    
    # 计算结果
    result = (config["x"] - 2) ** 2 + (config["y"] - 3) ** 2
    tune.report(loss=result)

search_space = {
    "x": tune.uniform(-5, 5),
    "y": tune.uniform(-5, 5)
}

tuner = tune.Tuner(
    distributed_objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=50,
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
```

## 结果分析

```python
# 分析调优结果
def analyze_results(results):
    # 获取最佳结果
    best_result = results.get_best_result()
    print(f"最佳配置: {best_result.config}")
    print(f"最佳指标: {best_result.metrics}")
    
    # 获取所有结果
    all_results = results
    print(f"总共运行了 {len(all_results)} 个试验")
    
    # 打印前5个最佳结果
    best_results = results.get_best_result(
        metric="loss",
        mode="min",
        scope="all"
    )
    
    # 访问特定试验的结果
    for i, trial in enumerate(results):
        print(f"试验 {i}: 配置={trial.config}, 损失={trial.metrics.get('loss', 'N/A')}")

# 运行调优并分析结果
results = tuner.fit()
analyze_results(results)
```

## 自定义搜索算法

```python
from ray.tune.search import Searcher

class CustomSearcher(Searcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.points = [(1, 1), (2, 2), (3, 3), (4, 4)]
        self.index = 0
    
    def suggest(self, trial_id):
        if self.index < len(self.points):
            x, y = self.points[self.index]
            self.index += 1
            return {"x": x, "y": y}
        else:
            return None

# 使用自定义搜索器
custom_searcher = CustomSearcher()

tuner = tune.Tuner(
    objective,
    tune_config=tune.TuneConfig(
        search_alg=custom_searcher,
        num_samples=4
    )
)

results = tuner.fit()
```

## 最佳实践

1. **选择合适的搜索算法**：对于小搜索空间使用网格搜索，对于大空间使用贝叶斯优化
2. **使用调度器**：使用ASHA等调度器提前终止表现不佳的试验
3. **设置合理的资源限制**：避免过度使用计算资源
4. **定义明确的停止条件**：设置适当的停止条件避免无限运行
5. **监控实验**：使用Ray Dashboard监控实验进度
6. **保存检查点**：定期保存实验结果以防止数据丢失

## 常见调优策略

- **随机搜索**：在大搜索空间中通常比网格搜索更有效
- **贝叶斯优化**：适用于昂贵的评估函数
- **进化算法**：适用于复杂的搜索空间
- **人口基础训练**：适用于强化学习等场景