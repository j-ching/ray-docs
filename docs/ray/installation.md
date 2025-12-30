# 安装

Ray可以在多种环境中安装，包括本地机器、云平台和集群。本节将介绍如何安装Ray以及不同安装选项的说明。

## 基础安装

使用pip安装Ray的最简单方法：

```bash
pip install ray
```

## 安装特定组件

Ray提供了一些可选组件，可以根据需要安装：

### Ray AIR (AI Runtime)
```bash
pip install "ray[air]"
```

### Ray Serve (模型服务)
```bash
pip install "ray[serve]"
```

### Ray Data (数据处理)
```bash
pip install "ray[data]"
```

### Ray Train (分布式训练)
```bash
pip install "ray[train]"
```

### Ray Tune (超参数调优)
```bash
pip install "ray[tune]"
```

### 完整安装
```bash
pip install "ray[default]"
```

## 从源码安装

如果需要最新的开发版本，可以从源码安装：

```bash
git clone https://github.com/ray-project/ray.git
cd ray
pip install -e python
```

## Docker安装

Ray也提供了Docker镜像：

```bash
docker pull rayproject/ray
```

## 验证安装

安装完成后，可以通过以下命令验证安装：

```python
import ray

# 初始化Ray
ray.init()

# 运行一个简单的测试
@ray.remote
def hello():
    return "Hello, Ray!"

result = hello.remote()
print(ray.get(result))

# 关闭Ray
ray.shutdown()
```

## 平台支持

Ray支持以下平台：
- Linux (Ubuntu 18.04+, CentOS 7+)
- macOS (10.15+)
- Windows (实验性支持)

## 常见问题

### 权限问题
在某些系统上，可能需要使用`--user`标志安装：

```bash
pip install --user ray
```

### 依赖冲突
如果遇到依赖冲突，可以尝试创建虚拟环境：

```bash
python -m venv ray_env
source ray_env/bin/activate  # Linux/macOS
# 或
ray_env\\Scripts\\activate  # Windows
pip install ray
```

### CUDA支持
如果需要CUDA支持，请确保安装了兼容的PyTorch或TensorFlow版本。