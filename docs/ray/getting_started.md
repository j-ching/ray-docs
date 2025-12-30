# 快速入门

Ray是一个用于分布式计算的开源框架，它使开发人员能够轻松地构建可扩展的机器学习和数据处理应用程序。本指南将帮助您快速开始使用Ray。

## 安装Ray

```bash
pip install ray
```

## 基本用法

最简单的Ray程序包含一个远程函数，它可以并行执行：

```python
import ray

# 初始化Ray运行时
ray.init()

# 定义一个远程函数
@ray.remote
def hello_world():
    return "Hello, Ray!"

# 调用远程函数并获取结果
result = ray.get(hello_world.remote())
print(result)

# 关闭Ray运行时
ray.shutdown()
```

## Ray集群入门

Ray可以轻松地在单机或集群环境中运行。在单机上运行时，Ray会自动管理所有资源。在集群环境中，Ray提供了简单的方法来管理多个节点。

### 启动Ray

```bash
ray start --head
```

### 连接到Ray集群

```python
import ray

ray.init("ray://<head-node-ip>:10001")
```

## 核心概念

- **远程函数**：可以并行执行的函数
- **Actor**：有状态的远程对象
- **对象存储**：用于在任务和actor之间共享数据
- **任务**：执行远程函数的计算单元

## Ray生态系统

Ray提供了多个高级库，用于特定领域：

- **Ray AIR**：统一的机器学习工具包
- **Ray Serve**：模型服务
- **Ray Data**：分布式数据处理
- **Ray Train**：分布式训练
- **Ray Tune**：超参数调优
- **Ray Workflows**：工作流编排

这些库构建在Ray的核心功能之上，为特定用例提供了高级抽象。