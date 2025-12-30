# Ray Tune

Ray Tune是Ray的超参数调优库，用于自动化机器学习模型的超参数搜索。它支持多种搜索算法和调度策略，可扩展到大型集群。

## 什么是Ray Tune？

Ray Tune是一个用于超参数调优的库，旨在高效地搜索机器学习模型的最佳超参数组合。它提供了多种搜索算法、调度策略和分析工具，支持从简单的网格搜索到复杂的贝叶斯优化。

## 快速开始

### 基本超参数调优

```python
from ray import tune
import ray

def train_func(config):
    # 模拟训练过程
    for step in range(10):
        # 模拟损失计算
        loss = config["lr"] * step / (step + 1)
        # 报告当前指标
        tune.report(loss=loss, step=step)

# 定义搜索空间
search_space = {
    "lr": tune.loguniform(0.001, 1.0),
    "batch_size": tune.choice([16, 32, 64, 128])
}

# 运行调优
tuner = tune.Tuner(
    train_func,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=10,  # 运行10个试验
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
best_result = results.get_best_result()
print(f"最佳参数: {best_result.config}")
print(f"最佳指标: {best_result.metrics}")
```

### 与机器学习框架集成

```python
from ray import tune
from ray.tune.schedulers import ASHAScheduler
from ray.air import session
import torch
import torch.nn as nn
import torch.optim as optim

def train_model(config):
    # 模型定义
    model = nn.Sequential(
        nn.Linear(784, config["hidden_size"]),
        nn.ReLU(),
        nn.Linear(config["hidden_size"], 10)
    )
    
    optimizer = optim.SGD(model.parameters(), lr=config["lr"])
    criterion = nn.CrossEntropyLoss()
    
    for epoch in range(10):
        # 模拟训练步骤
        loss = config["lr"] * (10 - epoch) / 10  # 模拟损失下降
        accuracy = 1.0 - loss  # 模拟准确率提升
        
        # 报告指标
        session.report({
            "loss": loss,
            "accuracy": accuracy,
            "epoch": epoch
        })

# 配置搜索空间
config = {
    "lr": tune.loguniform(0.001, 0.1),
    "hidden_size": tune.choice([32, 64, 128, 256]),
    "batch_size": tune.choice([16, 32, 64])
}

# 使用ASHA调度器
scheduler = ASHAScheduler(
    metric="loss",
    mode="min",
    max_t=10,      # 最大训练步数
    grace_period=2, # 最小运行步数
    reduction_factor=2
)

tuner = tune.Tuner(
    train_model,
    param_space=config,
    tune_config=tune.TuneConfig(
        num_samples=20,
        scheduler=scheduler,
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
```

## 搜索算法

### 随机搜索

```python
from ray.tune.search.basic_variant import BasicVariantGenerator

# 简单的随机搜索
tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.uniform(0.001, 0.1),
        "momentum": tune.uniform(0.1, 0.9)
    },
    tune_config=tune.TuneConfig(
        search_alg=BasicVariantGenerator(),
        num_samples=50
    )
)
```

### 网格搜索

```python
# 网格搜索
tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.grid_search([0.001, 0.01, 0.1]),
        "batch_size": tune.grid_search([16, 32, 64])
    },
    tune_config=tune.TuneConfig(
        num_samples=1  # 网格搜索会尝试所有组合
    )
)
```

### 贝叶斯优化

```python
from ray.tune.search.hyperopt import HyperOptSearch

# 使用HyperOpt进行贝叶斯优化
search_alg = HyperOptSearch(
    metric="loss",
    mode="min"
)

tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.loguniform(0.001, 0.1),
        "hidden_size": tune.quniform(32, 256, q=32)
    },
    tune_config=tune.TuneConfig(
        search_alg=search_alg,
        num_samples=20
    )
)
```

### 进化算法

```python
from ray.tune.search.nevergrad import NevergradSearch
import nevergrad as ng

# 使用Nevergrad的进化算法
search_alg = NevergradSearch(
    optimizer=ng.optimizers.OnePlusOne,
    metric="loss",
    mode="min"
)

tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.loguniform(0.001, 0.1),
        "reg": tune.uniform(0.01, 1.0)
    },
    tune_config=tune.TuneConfig(
        search_alg=search_alg,
        num_samples=30
    )
)
```

## 调度策略

### ASHA (Asynchronous Successive Halving Algorithm)

```python
from ray.tune.schedulers import ASHAScheduler

asha_scheduler = ASHAScheduler(
    metric="loss",
    mode="min",
    max_t=100,        # 最大训练时间
    grace_period=10,  # 最小运行时间
    reduction_factor=4,  # 淘汰比例
    brackets=1        # 档次数
)

tuner = tune.Tuner(
    train_func,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        scheduler=asha_scheduler,
        num_samples=50
    )
)
```

### Population Based Training (PBT)

```python
from ray.tune.schedulers import PopulationBasedTraining

pbt_scheduler = PopulationBasedTraining(
    metric="accuracy",
    mode="max",
    perturbation_interval=5,  # 每5步进行一次扰动
    hyperparam_mutations={
        "lr": tune.loguniform(0.0001, 0.1),
        "momentum": [0.8, 0.9, 0.95, 0.99]
    }
)

tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.choice([0.001, 0.01, 0.1]),
        "momentum": tune.choice([0.9, 0.95])
    },
    tune_config=tune.TuneConfig(
        scheduler=pbt_scheduler,
        num_samples=8
    )
)
```

## 与Ray AIR集成

### 使用Ray AIR的Tuner

```python
from ray import tune
from ray.tune.tuner import Tuner
from ray.air import RunConfig
from ray.air.config import CheckpointConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    # 训练逻辑
    for step in range(config["num_epochs"]):
        # 模拟训练
        loss = config["lr"] * (config["num_epochs"] - step) / config["num_epochs"]
        accuracy = 1.0 - loss
        
        # 使用Ray AIR的session报告指标
        session.report({"loss": loss, "accuracy": accuracy, "epoch": step})

# 定义搜索空间
tune_config = {
    "lr": tune.loguniform(0.001, 0.1),
    "batch_size": tune.choice([16, 32, 64]),
    "num_epochs": 10
}

# 创建Tuner
tuner = tune.Tuner(
    train_func,
    param_space=tune_config,
    tune_config=tune.TuneConfig(
        num_samples=20,
        metric="accuracy",
        mode="max"
    ),
    run_config=RunConfig(
        name="tune_experiment",
        checkpoint_config=CheckpointConfig(
            checkpoint_score_attribute="accuracy",
            checkpoint_score_order="max"
        )
    )
)

results = tuner.fit()
```

## 检查点和恢复

### 自动检查点

```python
def train_with_checkpoint(config):
    step = 0
    
    # 检查是否从检查点恢复
    if session.get_checkpoint():
        checkpoint = session.get_checkpoint()
        step = checkpoint.to_dict()["step"]
    
    for epoch in range(step, config["num_epochs"]):
        # 训练逻辑
        loss = calculate_loss(config, epoch)
        
        # 定期保存检查点
        if epoch % 5 == 0:
            session.report(
                metrics={"loss": loss, "epoch": epoch},
                checkpoint=Checkpoint.from_dict({
                    "step": epoch,
                    "config": config,
                    "model_state": "model_state_dict"
                })
            )
        else:
            session.report({"loss": loss, "epoch": epoch})
```

### 实验恢复

```python
# 从现有实验恢复
restored_tuner = tune.Tuner.restore(
    path="/path/to/experiment",
    trainable=train_func
)

results = restored_tuner.fit()
```

## 分析结果

### 结果分析

```python
import pandas as pd

# 获取结果分析
results = tuner.fit()

# 转换为DataFrame进行分析
df = results.get_dataframe()

# 显示最佳结果
best_result = results.get_best_result()
print(f"最佳配置: {best_result.config}")
print(f"最佳指标: {best_result.metrics}")

# 分析超参数重要性
print(df[["config/lr", "config/batch_size", "loss"]].describe())

# 可视化结果（需要安装相关库）
# tune.Analysis(results).dataframe()
```

## 高级功能

### 条件搜索空间

```python
def conditional_search(config):
    # 条件参数空间
    if config["model_type"] == "deep":
        config["hidden_layers"] = tune.randint(3, 10)
        config["hidden_size"] = tune.choice([128, 256, 512])
    else:
        config["hidden_layers"] = tune.randint(1, 3)
        config["hidden_size"] = tune.choice([64, 128])

search_space = {
    "model_type": tune.choice(["shallow", "deep"]),
    "lr": tune.loguniform(0.001, 0.1)
}

# 使用函数定义的搜索空间
tuner = tune.Tuner(
    train_with_conditional_space,
    param_space=lambda: {
        "model_type": tune.choice(["shallow", "deep"]),
        **({"hidden_layers": tune.randint(3, 10)} if tune.sample_from(lambda spec: spec.config.model_type == "deep") else {"hidden_layers": tune.randint(1, 3)})
    }
)
```

### 自定义搜索算法

```python
from ray.tune.search import Searcher

class CustomSearcher(Searcher):
    def __init__(self, metric="loss", mode="min", **kwargs):
        super().__init__(metric=metric, mode=mode)
        self.count = 0
    
    def suggest(self, trial_id):
        # 自定义参数建议逻辑
        self.count += 1
        return {
            "lr": 0.1 / (self.count + 1),
            "batch_size": [16, 32, 64][self.count % 3]
        }
    
    def on_trial_complete(self, trial_id, result=None, error=False):
        # 处理试验完成事件
        pass

# 使用自定义搜索算法
custom_searcher = CustomSearcher()
tuner = tune.Tuner(
    train_func,
    param_space={
        "lr": tune.uniform(0.001, 0.1),
        "batch_size": tune.choice([16, 32, 64])
    },
    tune_config=tune.TuneConfig(
        search_alg=custom_searcher,
        num_samples=10
    )
)
```

## 分布式调优

### 集群上的分布式调优

```python
# 在Ray集群上运行
ray.init(address="ray://<head-node-ip>:10001")

# Ray Tune会自动利用集群资源
tuner = tune.Tuner(
    train_func,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=100,
        metric="loss",
        mode="min"
    )
)

results = tuner.fit()
```

## 实验管理

### 命名和组织实验

```python
from ray.air import RunConfig

tuner = tune.Tuner(
    train_func,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=20,
        metric="loss",
        mode="min"
    ),
    run_config=RunConfig(
        name="my_tune_experiment",  # 实验名称
        local_dir="/path/to/results",  # 结果保存目录
        stop={"training_iteration": 100}  # 停止条件
    )
)
```

## 最佳实践

### 高效调优策略

1. **选择合适的搜索算法**：
   - 随机搜索：简单有效，适合初步探索
   - 贝叶斯优化：适合昂贵的评估函数
   - 网格搜索：适合离散参数的彻底搜索

2. **使用早期停止**：
   - ASHA调度器可以显著减少调优时间
   - 设置合理的grace period避免过早停止

3. **合理的资源分配**：
   - 根据搜索空间大小分配计算资源
   - 考虑并发试验数量

4. **监控和调试**：
   - 定期检查调优进度
   - 分析超参数与性能的关系

```python
# 示例：综合调优配置
def comprehensive_tune():
    config = {
        "lr": tune.loguniform(0.0001, 0.1),
        "batch_size": tune.choice([16, 32, 64, 128]),
        "momentum": tune.uniform(0.8, 0.99),
        "weight_decay": tune.loguniform(1e-6, 1e-2)
    }
    
    scheduler = ASHAScheduler(
        metric="validation_loss",
        mode="min",
        max_t=100,
        grace_period=10,
        reduction_factor=3
    )
    
    search_alg = HyperOptSearch(
        metric="validation_loss",
        mode="min",
        n_initial_points=10
    )
    
    tuner = tune.Tuner(
        train_model,
        param_space=config,
        tune_config=tune.TuneConfig(
            search_alg=search_alg,
            scheduler=scheduler,
            num_samples=50,
            metric="validation_loss",
            mode="min"
        ),
        run_config=RunConfig(
            name="comprehensive_tune",
            checkpoint_config=CheckpointConfig(
                checkpoint_score_attribute="validation_loss",
                checkpoint_score_order="min",
                num_to_keep=3
            )
        )
    )
    
    results = tuner.fit()
    return results
```

Ray Tune通过提供灵活的搜索算法、高效的调度策略和完整的实验管理功能，使超参数调优变得简单而高效。