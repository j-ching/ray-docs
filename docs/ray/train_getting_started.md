# Train快速入门

Ray Train是Ray的分布式训练库，简化了在多个设备和节点上训练机器学习模型的过程。本节介绍如何使用Ray Train进行分布式训练。

## 安装Ray Train

```bash
pip install "ray[train]"
```

## 基本概念

Ray Train的主要组件：

- **Trainer**：封装训练逻辑的类
- **DataLoader**：分布式数据加载器
- **Strategy**：分布式训练策略（如数据并行、模型并行）
- **Checkpoint**：训练状态的保存和恢复机制

## 简单PyTorch示例

```python
import ray
from ray import train
from ray.train import Trainer
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# 定义简单的神经网络
class SimpleNet(nn.Module):
    def __init__(self):
        super(SimpleNet, self).__init__()
        self.fc1 = nn.Linear(10, 5)
        self.fc2 = nn.Linear(5, 1)
        self.relu = nn.ReLU()
    
    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.fc2(x)
        return x

def train_func(config):
    # 获取训练配置
    lr = config.get("lr", 0.001)
    epochs = config.get("epochs", 5)
    
    # 获取数据
    train_dataset = train.get_dataset_shard("train")
    
    # 创建模型
    model = SimpleNet()
    
    # 设置分布式训练
    model = train.torch.prepare_model(model)
    
    # 创建优化器
    optimizer = optim.SGD(model.parameters(), lr=lr)
    
    # 训练循环
    for epoch in range(epochs):
        for batch_idx, batch in enumerate(train_dataset.iter_torch_batches(batch_size=32)):
            inputs, targets = batch["x"], batch["y"]
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = nn.MSELoss()(outputs, targets)
            loss.backward()
            optimizer.step()
        
        # 报告指标
        train.report({"epoch": epoch, "loss": loss.item()})

# 初始化Ray
ray.init()

# 准备数据
import numpy as np
X = np.random.random((1000, 10))
y = np.random.random((1000, 1))
train_dataset = ray.data.from_pandas(pd.DataFrame({"x": list(X), "y": list(y)}))

# 创建训练器
trainer = Trainer(
    train_func,
    train_dataset,
    scaling_config={
        "num_workers": 2,      # 使用2个worker
        "use_gpu": False       # 不使用GPU
    },
    params={"lr": 0.01, "epochs": 10}
)

# 开始训练
results = trainer.fit()
print(f"训练完成: {results}")

ray.shutdown()
```

## 使用内置Trainer

Ray Train提供了一些内置的Trainer类：

```python
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
import torch
from torch import nn, optim

# 使用内置的TorchTrainer
def train_loop_per_worker():
    # 获取数据
    dataset = train.get_dataset_shard("train")
    
    # 创建模型
    model = nn.Sequential(
        nn.Linear(10, 10),
        nn.ReLU(),
        nn.Linear(10, 1)
    )
    model = train.torch.prepare_model(model)
    
    # 创建优化器
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    
    # 训练循环
    for epoch in range(5):
        for batch in dataset.iter_torch_batches(batch_size=16):
            inputs, targets = batch["x"], batch["y"]
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = nn.MSELoss()(outputs, targets)
            loss.backward()
            optimizer.step()
        
        train.report({"epoch": epoch, "loss": loss.item()})

# 配置扩展设置
scaling_config = ScalingConfig(
    num_workers=2,
    use_gpu=False
)

# 创建TorchTrainer
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config,
    datasets={"train": train_dataset}
)

# 训练
results = trainer.fit()
```

## TensorFlow示例

```python
from ray.train.tensorflow import TensorflowTrainer
import tensorflow as tf

def tensorflow_train_func():
    # 获取数据
    dataset = train.get_dataset_shard("train")
    
    # 准备TensorFlow模型
    model = tf.keras.Sequential([
        tf.keras.layers.Dense(10, activation='relu', input_shape=(10,)),
        tf.keras.layers.Dense(1)
    ])
    
    # 准备模型用于分布式训练
    model = train.tensorflow.prepare_model(model)
    
    model.compile(
        optimizer='adam',
        loss='mse',
        metrics=['accuracy']
    )
    
    # 训练
    for epoch in range(5):
        # 将Ray数据集转换为TensorFlow数据集
        tf_dataset = dataset.to_tf("x", "y", batch_size=32)
        history = model.fit(tf_dataset, epochs=1, verbose=0)
        
        train.report({"epoch": epoch, "loss": history.history["loss"][0]})

# 创建TensorFlow训练器
tf_trainer = TensorflowTrainer(
    train_loop_per_worker=tensorflow_train_func,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": train_dataset}
)

tf_results = tf_trainer.fit()
```

## 检查点和恢复

```python
from ray.air.checkpoint import Checkpoint

def train_with_checkpoint():
    # 模拟训练逻辑
    for epoch in range(10):
        # 训练代码...
        
        if epoch % 2 == 0:  # 每2个epoch保存一次检查点
            # 创建检查点
            checkpoint = Checkpoint.from_dict({
                "epoch": epoch,
                "model_state": "model_state_dict_here"
            })
            train.report({"epoch": epoch}, checkpoint=checkpoint)

# 恢复训练
def resume_training():
    # 获取最新检查点
    latest_checkpoint = train.get_checkpoint()
    if latest_checkpoint:
        state = latest_checkpoint.to_dict()
        start_epoch = state["epoch"] + 1
        print(f"从epoch {start_epoch}恢复训练")
    else:
        start_epoch = 0
        print("从头开始训练")
    
    # 继续训练逻辑...
```

## 超参数调优集成

```python
from ray import tune
from ray.tune.schedulers import ASHAScheduler

# 与Ray Tune集成进行超参数调优
def train_with_config(config):
    # 使用配置中的超参数
    lr = config["lr"]
    hidden_size = config["hidden_size"]
    
    # 训练逻辑...
    for epoch in range(5):
        # 模拟训练
        loss = 1.0 / (epoch + 1) + config["lr"]  # 模拟损失
        train.report({"epoch": epoch, "loss": loss})

# 定义搜索空间
param_space = {
    "lr": tune.loguniform(0.001, 0.1),
    "hidden_size": tune.choice([16, 32, 64, 128])
}

# 使用Tuner进行超参数搜索
tuner = tune.Tuner(
    TorchTrainer,
    param_space=param_space,
    train_loop_per_worker=train_with_config,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": train_dataset}
)

results = tuner.fit()
best_result = results.get_best_result()
print(f"最佳配置: {best_result.config}")
```

## 分布式策略

```python
from ray.air.config import DatasetConfig

# 配置数据集分片
dataset_config = {
    "train": DatasetConfig(
        split_ratio=0.8,
        transform=lambda x: x.batch(32)
    )
}

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=ScalingConfig(
        num_workers=4,
        use_gpu=True,
        resources_per_worker={"CPU": 2, "GPU": 0.5}
    ),
    datasets={"train": train_dataset},
    dataset_config=dataset_config
)

results = trainer.fit()
```

## 性能优化

```python
# 启用混合精度训练
def train_with_amp():
    import torch.cuda.amp as amp
    
    model = train.torch.prepare_model(nn.Linear(10, 1))
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    
    # 使用自动混合精度
    scaler = amp.GradScaler()
    
    for epoch in range(5):
        for batch in train.get_dataset_shard("train").iter_torch_batches():
            inputs, targets = batch["x"], batch["y"]
            
            optimizer.zero_grad()
            with amp.autocast():  # 启用自动混合精度
                outputs = model(inputs)
                loss = nn.MSELoss()(outputs, targets)
            
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
        
        train.report({"epoch": epoch, "loss": loss.item()})
```

## 最佳实践

1. **数据预处理**：在训练前使用Ray Data进行高效的数据预处理
2. **资源分配**：合理分配CPU、GPU资源给每个worker
3. **检查点管理**：定期保存检查点以支持容错
4. **性能监控**：使用train.report()报告训练指标
5. **批处理大小**：根据可用内存调整批处理大小
6. **梯度同步**：理解不同分布式策略的梯度同步机制

## 常见训练模式

Ray Train支持多种分布式训练模式：

- **数据并行**：每个worker拥有模型的完整副本，处理不同的数据批次
- **模型并行**：模型的不同部分在不同的worker上
- **混合并行**：结合数据并行和模型并行

这些模式通过ScalingConfig和特定的准备函数来实现。