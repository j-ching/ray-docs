# Ray中文文档

欢迎来到Ray分布式计算框架的中文文档！本项目旨在为中文用户提供Ray框架的完整文档翻译和说明。

## 什么是Ray？

Ray是一个用于分布式计算的开源框架，它使开发人员能够轻松地构建可扩展的机器学习和数据处理应用程序。Ray提供了一个简单而强大的API，用于在单机或集群环境中执行并行和分布式计算。

## 主要特性

- **简单易用**：通过简单的API实现分布式计算
- **高性能**：优化的调度器和对象存储系统
- **可扩展**：从单机到大规模集群的无缝扩展
- **灵活性**：支持多种编程模型和框架

## Ray生态系统

Ray生态系统包含多个高级库：

- **Ray Core**：基础分布式计算原语
- **Ray Data**：分布式数据处理
- **Ray Train**：分布式机器学习训练
- **Ray Tune**：超参数调优
- **Ray Serve**：模型服务
- **Ray AIR**：统一的机器学习工具包
- **Ray Workflows**：分布式工作流

## 文档结构

本中文文档按照以下结构组织：

1. [快速入门](docs/ray/getting_started.md) - Ray的快速入门指南
2. [安装](docs/ray/installation.md) - 如何安装Ray及其组件
3. [核心功能](docs/ray/core.md) - Ray核心概念和API
   - [远程函数](docs/ray/remote_functions.md)
   - [Actors](docs/ray/actors.md)
   - [GPU支持](docs/ray/gpu.md)
   - [共享内存对象存储](docs/ray/object_store.md)
   - [Ray中的并行化](docs/ray/parallel.md)
4. [任务](docs/ray/tasks.md) - 任务管理和调度
5. [Ray集群](docs/ray/clusters.md) - 集群管理和部署
   - [在本地启动Ray](docs/ray/starting_ray.md)
   - [Ray集群入门](docs/ray/cluster_getting_started.md)
   - [在云上部署Ray](docs/ray/cluster_deploy.md)
   - [集群配置](docs/ray/cluster_config.md)
   - [集群CLI](docs/ray/cluster_cli.md)
   - [在现有集群上运行Ray](docs/ray/cluster_running.md)
   - [集群故障排除](docs/ray/cluster_troubleshooting.md)
   - [集群安全](docs/ray/cluster_security.md)
   - [使用Ray客户端](docs/ray/cluster_ray_client.md)
6. [开发](docs/ray/development.md) - 开发和调试
   - [调试](docs/ray/debugging.md)
   - [日志记录](docs/ray/logging.md)
   - [监控](docs/ray/monitoring.md)
   - [故障排除](docs/ray/troubleshooting.md)
7. [Ray AIR](docs/ray/air.md) - 统一机器学习工具包
   - [AIR快速入门](docs/ray/air_getting_started.md)
   - [数据](docs/ray/air_data.md)
   - [预处理器](docs/ray/air_preprocessor.md)
   - [训练器](docs/ray/air_trainer.md)
   - [Tuner](docs/ray/air_tuner.md)
   - [预测器](docs/ray/air_predictor.md)
   - [工作流](docs/ray/air_workflow.md)
   - [结果](docs/ray/air_results.md)
   - [AIR CLI](docs/ray/air_cli.md)
   - [AIR故障排除](docs/ray/air_troubleshooting.md)
8. [Ray Data](docs/ray/data.md) - 分布式数据处理
   - [数据加载和保存](docs/ray/data_loading.md)
   - [数据转换](docs/ray/data_transform.md)
   - [数据迭代](docs/ray/data_iterating.md)
   - [数据批处理](docs/ray/data_batch_inference.md)
   - [数据连接](docs/ray/data_join.md)
   - [数据分片](docs/ray/data_shuffling.md)
   - [数据实现](docs/ray/data_internals.md)
9. [Ray Serve](docs/ray/serve.md) - 模型服务
   - [Serve快速入门](docs/ray/serve_getting_started.md)
   - [编写Serve程序](docs/ray/serve_tutorial.md)
   - [部署管理](docs/ray/serve_deployment.md)
   - [请求处理](docs/ray/serve_requests.md)
   - [扩展和资源](docs/ray/serve_scaling_and_resources.md)
   - [故障排除](docs/ray/serve_troubleshooting.md)
10. [Ray Train](docs/ray/train.md) - 分布式训练
    - [Train快速入门](docs/ray/train_getting_started.md)
    - [PyTorch训练](docs/ray/train_pytorch.md)
    - [TensorFlow训练](docs/ray/train_tensorflow.md)
    - [Horovod训练](docs/ray/train_horovod.md)
    - [XGBoost训练](docs/ray/train_xgboost.md)
    - [LightGBM训练](docs/ray/train_lightgbm.md)
    - [故障排除](docs/ray/train_troubleshooting.md)
11. [Ray Tune](docs/ray/tune.md) - 超参数调优
    - [Tune快速入门](docs/ray/tune_getting_started.md)
    - [定义搜索空间](docs/ray/tune_search_space.md)
    - [运行实验](docs/ray/tune_run.md)
    - [搜索算法](docs/ray/tune_search_algorithms.md)
    - [调度算法](docs/ray/tune_schedulers.md)
    - [可视化](docs/ray/tune_visualization.md)
    - [故障排除](docs/ray/tune_troubleshooting.md)
12. [Ray Workflows](docs/ray/workflows.md) - 分布式工作流
    - [Workflows快速入门](docs/ray/workflows_getting_started.md)
    - [工作流模式](docs/ray/workflows_patterns.md)
    - [故障排除](docs/ray/workflows_troubleshooting.md)
13. [性能](docs/ray/performance.md) - 性能优化
    - [性能调优](docs/ray/performance_tuning.md)
    - [性能基准](docs/ray/performance_benchmarks.md)
14. [配置](docs/ray/configuration.md) - 配置和系统设置
    - [系统配置](docs/ray/system_config.md)
    - [内存管理](docs/ray/memory.md)
    - [环境变量](docs/ray/environment_variables.md)
15. [参考](docs/ray/reference.md) - API参考
16. [贡献](docs/ray/contributing.md) - 如何为Ray做贡献

## 快速开始

要开始使用Ray，请先安装：

```bash
pip install ray
```

然后尝试以下简单示例：

```python
import ray

# 初始化Ray
ray.init()

# 定义一个远程函数
@ray.remote
def hello_world():
    return "Hello, Ray!"

# 异步执行函数
result = hello_world.remote()

# 获取结果
print(ray.get(result))

# 关闭Ray
ray.shutdown()
```

## 社区和支持

- **官方文档**: [Ray Documentation](https://docs.ray.io/)
- **GitHub**: [Ray Project](https://github.com/ray-project/ray)
- **论坛**: [Ray Discussion](https://discuss.ray.io/)
- **Slack**: [Ray Community](https://ray-distributed.slack.com/)

## 贡献

本中文文档欢迎社区贡献。如果您发现任何错误或希望添加内容，请参考[贡献指南](docs/ray/contributing.md)。

## 许可证

本项目遵循与Ray项目相同的许可证。有关详细信息，请参阅原始Ray项目的许可证。