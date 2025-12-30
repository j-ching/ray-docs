# GPU支持

Ray提供了强大的GPU支持，使用户能够轻松地在分布式环境中使用GPU进行计算。本节介绍如何在Ray中配置和使用GPU资源。

## GPU资源分配

在Ray中，您可以为任务和actor指定GPU资源需求：

```python
import ray

@ray.remote(num_gpus=1)
def use_gpu():
    # 这个任务将使用1个GPU
    import torch
    device = torch.device("cuda")
    return f"Using device: {device}"

# 或者在actor上使用GPU
@ray.remote(num_gpus=2)
class GPUActor:
    def __init__(self):
        import torch
        self.device = torch.device("cuda:0")
    
    def get_device(self):
        return str(self.device)
```

## GPU可见性

Ray会自动管理GPU可见性，确保每个任务或actor只能访问分配给它的GPU。这通过设置`CUDA_VISIBLE_DEVICES`环境变量来实现。

## 多GPU配置

当有多个GPU可用时，Ray会根据任务需求自动分配：

```python
import ray

# 在有4个GPU的节点上
@ray.remote(num_gpus=0.5)
class FractionalGPUActor:
    def __init__(self):
        import os
        self.visible_gpus = os.environ.get("CUDA_VISIBLE_DEVICES", "none")
    
    def get_gpu_info(self):
        return f"Visible GPUs: {self.visible_gpus}"
```

## GPU监控

您可以监控GPU使用情况：

```python
# 获取节点的GPU资源信息
resources = ray.cluster_resources()
print(f"可用GPU数量: {resources.get('GPU', 0)}")
```

## 混合CPU/GPU工作负载

Ray允许您创建混合使用CPU和GPU资源的任务：

```python
@ray.remote(num_cpus=2, num_gpus=1)
def mixed_workload():
    # 使用2个CPU核心和1个GPU
    import torch
    x = torch.randn(1000, 1000).cuda()
    y = torch.randn(1000, 1000).cuda()
    result = torch.mm(x, y)
    return result.cpu()
```

## 最佳实践

1. **精确指定GPU需求**：只请求实际需要的GPU数量，以提高资源利用率
2. **GPU内存管理**：在GPU任务中注意内存管理，避免内存泄漏
3. **错误处理**：处理GPU不可用或内存不足的情况
4. **性能监控**：监控GPU利用率和性能指标

## 常见问题

- **GPU资源不足**：确保有足够的GPU资源可用
- **CUDA版本兼容性**：确保CUDA版本与PyTorch/TensorFlow兼容
- **驱动程序问题**：确保NVIDIA驱动程序已正确安装