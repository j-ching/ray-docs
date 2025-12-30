# 安装

本节介绍如何安装Ray及其相关组件。

## 基本安装

### 安装最新稳定版

```bash
pip install ray
```

### 安装特定版本

```bash
# 安装特定版本
pip install ray==2.9.3

# 升级到最新版本
pip install --upgrade ray
```

## 安装Ray生态系统组件

### 安装Ray Data

```bash
pip install "ray[data]"
```

### 安装Ray Train

```bash
pip install "ray[train]"
```

### 安装Ray Tune

```bash
pip install "ray[tune]"
```

### 安装Ray Serve

```bash
pip install "ray[serve]"
```

### 安装Ray Workflows

```bash
pip install "ray[workflows]"
```

### 安装完整版Ray

```bash
pip install "ray[default]"
# 等同于安装所有组件
```

## 从源码安装

### 克隆源码

```bash
git clone https://github.com/ray-project/ray.git
cd ray
```

### 安装开发版本

```bash
# 安装依赖
pip install -e python/ray[dev]
```

## Docker安装

### 使用官方Docker镜像

```bash
# 拉取最新镜像
docker pull rayproject/ray:latest

# 运行Ray head节点
docker run --shm-size=2g -d -p 8265:8265 -p 10001:10001 rayproject/ray:latest ray start --head --dashboard-host 0.0.0.0

# 连接到容器
docker exec -it <container-id> python
```

## Conda安装

```bash
# 使用conda安装
conda install -c conda-forge ray-core

# 安装完整版
conda install -c conda-forge ray-default
```

## 系统要求

### 支持的操作系统

- Linux (Ubuntu 18.04+, CentOS 7+)
- macOS (10.15+)
- Windows (实验性支持)

### 硬件要求

- **CPU**: 任何支持的架构
- **内存**: 最少2GB，推荐4GB+
- **存储**: 1GB可用空间
- **网络**: 用于集群部署

### Python版本支持

- Python 3.7+
- 推荐使用Python 3.8或更高版本

## 验证安装

### 基本验证

```python
import ray

# 初始化Ray
ray.init()

# 运行简单测试
@ray.remote
def hello():
    return "Hello, Ray!"

result = ray.get(hello.remote())
print(result)  # 输出: Hello, Ray!

# 关闭Ray
ray.shutdown()
```

### 验证特定组件

```python
# 验证Ray Data
import ray
ray.init()
ds = ray.data.range(10)
print(ds.take(5))  # [0, 1, 2, 3, 4]
ray.shutdown()

# 验证Ray Serve
from ray import serve
print(serve.__version__)

# 验证Ray Train
from ray.train import Trainer
print(Trainer.__module__)
```

## 常见安装问题

### Windows安装问题

```bash
# 如果在Windows上遇到问题，尝试
pip install ray[default] --find-links https://s3-us-west-2.amazonaws.com/ray-wheels/master/
```

### 权限问题

```bash
# 如果遇到权限问题，使用用户安装
pip install --user ray[default]
```

### 虚拟环境安装

```bash
# 创建虚拟环境
python -m venv ray_env
source ray_env/bin/activate  # Linux/macOS
# 或 ray_env\Scripts\activate  # Windows

# 安装Ray
pip install -U pip
pip install ray[default]
```

## 安装特定后端

### GPU支持

```bash
# 安装支持GPU的PyTorch和TensorFlow
pip install "ray[default]" torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install tensorflow[and-cuda]
```

### 特定依赖

```bash
# 仅安装Ray核心功能
pip install ray

# 安装带额外依赖的版本
pip install "ray[serve,train,tune,data,debug]"
```

## 集群安装

### 在集群节点上安装

在每个集群节点上执行相同的安装步骤：

```bash
# 在所有节点上安装
pip install ray[default]

# 或使用conda
conda install -c conda-forge ray-default
```

## 版本兼容性

### 与深度学习框架的兼容性

```bash
# 兼容PyTorch
pip install ray[train] torch torchvision

# 兼容TensorFlow
pip install ray[train] tensorflow

# 兼容JAX
pip install ray[train] jax jaxlib
```

### 依赖冲突解决

如果遇到依赖冲突：

```bash
# 创建新的虚拟环境
python -m venv fresh_ray_env
source fresh_ray_env/bin/activate
pip install --upgrade pip
pip install ray[default]
```

## 预发布版本

### 安装预发布版本

```bash
# 安装预发布版本（测试用）
pip install --pre -U ray
```

### 安装特定分支的wheel

```bash
# 从特定分支安装wheel
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/master/ray-2.9.0.dev0-cp38-cp38-manylinux2014_x86_64.whl
```

安装完成后，您就可以开始使用Ray的强大功能了。建议先从基本的Ray功能开始，然后根据需要安装额外的组件。