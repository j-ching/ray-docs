# AIR快速入门

Ray AIR（Artificial Intelligence Runtime）是统一的机器学习工具包，用于在Ray上构建、训练和部署可扩展的ML应用程序。本节介绍如何快速开始使用Ray AIR。

## 安装Ray AIR

```bash
pip install "ray[air]"
```

## 基本概念

Ray AIR统一了机器学习管道的各个组件：

- **数据处理**：使用Ray Data进行大规模数据预处理
- **模型训练**：使用Ray Train进行分布式训练
- **超参数调优**：使用Ray Tune进行自动化调优
- **模型服务**：使用Ray Serve进行模型部署
- **批处理推理**：使用Ray AIR进行大规模推理

## 简单示例

以下是一个使用Ray AIR的基本示例：

```python
import ray
from ray import air, tune
from ray.air import session
from ray.air.examples.xgboost_example import XGBoostTrainer
from ray.air.config import ScalingConfig

# 初始化Ray
ray.init()

# 定义训练配置
trainer = XGBoostTrainer(
    label_column="labels",
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(
        num_workers=2,  # 使用2个工作节点
        use_gpu=False
    ),
)

# 训练模型
result = trainer.fit()
print(f"训练结果: {result}")

ray.shutdown()
```

## 使用预处理器

```python
from ray.air.preprocessors import StandardScaler, LabelEncoder

# 定义预处理器
preprocessor = StandardScaler(columns=["feature1", "feature2"])

# 应用预处理器到训练器
trainer = XGBoostTrainer(
    label_column="labels",
    preprocessor=preprocessor,
    params={"objective": "binary:logistic"},
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=2),
)

result = trainer.fit()
```

## 超参数调优

```python
from ray import tune

# 定义搜索空间
param_space = {
    "params": {
        "max_depth": tune.randint(3, 10),
        "learning_rate": tune.loguniform(0.1, 1),
        "subsample": tune.uniform(0.5, 1.0),
        "colsample_bytree": tune.uniform(0.5, 1.0),
    }
}

# 使用Tuner进行超参数调优
tuner = tune.Tuner(
    XGBoostTrainer,
    param_space=param_space,
    tune_config=tune.TuneConfig(num_samples=10),
    run_config=air.RunConfig(stop={"training_iteration": 5}),
)

results = tuner.fit()
best_result = results.get_best_result()
print(f"最佳参数: {best_result.config}")
```

## 模型部署

训练完成后，可以使用Ray AIR进行模型部署：

```python
from ray import serve
from ray.serve import PredictorDeployment
from ray.air.predictor import Predictor

# 获取最佳模型
best_model = best_result.checkpoint.to_model()

# 部署模型
@serve.deployment
class ModelDeployment:
    def __init__(self, predictor: Predictor):
        self.predictor = predictor
    
    async def __call__(self, request):
        data = request.json()
        predictions = self.predictor.predict(data)
        return predictions

# 部署服务
deployment = ModelDeployment.bind(best_model.get_preprocessor(), best_model.get_model())
serve.run(deployment)
```

## 完整的ML管道示例

```python
import ray
from ray import air, tune
from ray.air import Result, ScalingConfig
from ray.air.preprocessors import StandardScaler
from ray.air.config import RunConfig
from ray.train.xgboost import XGBoostTrainer

def ml_pipeline_example():
    # 初始化Ray
    if not ray.is_initialized():
        ray.init()
    
    # 创建示例数据集
    import pandas as pd
    import numpy as np
    from ray.data import from_pandas
    
    data = pd.DataFrame({
        "feature1": np.random.random(1000),
        "feature2": np.random.random(1000),
        "labels": np.random.randint(0, 2, 1000)
    })
    dataset = from_pandas(data)
    
    # 定义预处理器
    preprocessor = StandardScaler(columns=["feature1", "feature2"])
    
    # 创建训练器
    trainer = XGBoostTrainer(
        label_column="labels",
        preprocessor=preprocessor,
        params={"objective": "binary:logistic"},
        datasets={"train": dataset},
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            checkpoint_config=air.CheckpointConfig(
                checkpoint_score_attribute="accuracy",
                checkpoint_score_order="max"
            )
        )
    )
    
    # 训练模型
    result = trainer.fit()
    print(f"训练完成，结果: {result.metrics}")
    
    return result

# 运行示例
if __name__ == "__main__":
    result = ml_pipeline_example()
    ray.shutdown()
```

## AIR组件集成

Ray AIR的强项在于其组件之间的无缝集成：

```python
# 数据 -> 预处理 -> 训练 -> 评估 -> 部署
from ray.air import Pipeline

# 构建端到端管道
pipeline = Pipeline(
    preprocessor=StandardScaler(columns=["feature1", "feature2"]),
    model=XGBoostModel()
)

# 使用管道进行预测
predictions = pipeline.predict(test_dataset)
```

## 最佳实践

1. **数据预处理**：在训练前使用Ray Data进行高效的数据预处理
2. **资源管理**：合理配置训练资源以优化性能
3. **检查点管理**：启用检查点以支持容错和恢复
4. **指标监控**：使用session.report()报告训练指标
5. **模型版本控制**：使用Ray AIR的检查点系统管理模型版本