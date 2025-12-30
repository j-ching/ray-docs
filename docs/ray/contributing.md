# 贡献

Ray是一个开源项目，欢迎社区贡献。本节介绍如何为Ray项目做贡献。

## 贡献方式

### 代码贡献

1. **报告问题**：在GitHub上提交bug报告或功能请求
2. **修复bug**：解决已知问题
3. **添加功能**：实现新功能或改进现有功能
4. **优化性能**：改进性能或效率
5. **完善文档**：改进文档或添加示例

### 非代码贡献

1. **文档改进**：修正错误、完善说明
2. **教程编写**：创建使用教程或示例
3. **社区支持**：在论坛或聊天室帮助其他用户
4. **测试反馈**：测试新功能并提供反馈

## 开发环境设置

### 克隆仓库

```bash
# 克隆Ray仓库
git clone https://github.com/ray-project/ray.git
cd ray

# 创建开发分支
git checkout -b feature/my-feature-branch
```

### 安装开发版本

```bash
# 安装开发依赖
pip install -e python/ray[dev]

# 或者安装所有可选依赖
pip install -e python/ray[all]
```

### 构建Ray

```bash
# 构建Ray（需要C++工具链）
cd ray
./build.sh
```

## 代码规范

### Python代码规范

Ray遵循PEP 8 Python编码规范：

```python
# 正确的导入顺序
import os
import sys

import numpy as np
import pandas as pd

from ray import ray
```

### 代码风格要求

```python
# 使用4个空格缩进
def example_function(param1, param2):
    """函数文档字符串示例。
    
    Args:
        param1: 第一个参数
        param2: 第二个参数
        
    Returns:
        处理结果
    """
    result = param1 + param2
    return result
```

### 类型注解

```python
from typing import List, Dict, Optional

def typed_function(data: List[int], mapping: Dict[str, int]) -> Optional[str]:
    """带类型注解的函数示例。"""
    if not data:
        return None
    return str(sum(data))
```

## 测试

### 运行测试

```bash
# 运行所有测试
python -m pytest python/ray/tests/

# 运行特定测试
python -m pytest python/ray/tests/test_basic.py

# 运行性能测试
python -m pytest python/ray/tests/test_performance.py
```

### 编写测试

```python
import pytest
import ray

def test_example():
    """示例测试函数。"""
    ray.init()
    
    @ray.remote
    def simple_task():
        return 42
    
    result = ray.get(simple_task.remote())
    assert result == 42
    
    ray.shutdown()
```

## 文档贡献

### 文档结构

Ray文档使用Sphinx生成：

```
ray/
├── doc/
│   ├── source/
│   │   ├── getting-started.rst
│   │   └── api/
│   └── conf.py
```

### 文档格式

```rst
标题
====

子标题
--------

.. code-block:: python

    def example():
        return "Hello, Ray!"

.. note::
    这是一个注意事项。

.. warning::
    这是一个警告。
```

## 提交PR

### PR准备

1. **确保测试通过**：
   ```bash
   python -m pytest python/ray/tests/test_my_feature.py
   ```

2. **代码检查**：
   ```bash
   # 运行linter
   flake8 python/ray/my_module.py
   
   # 运行格式化工具
   black python/ray/my_module.py
   ```

### PR描述模板

```markdown
## 问题描述
简要描述解决的问题。

## 解决方案
描述实现的解决方案。

## 测试
- [ ] 单元测试已通过
- [ ] 集成测试已通过
- [ ] 性能测试（如适用）

## 变更类型
- [ ] Bug修复
- [ ] 新功能
- [ ] 性能改进
- [ ] 文档更新
```

## 开发流程

### 本地开发

```bash
# 1. 创建分支
git checkout -b feature/my-feature

# 2. 编写代码
# 编辑相关文件

# 3. 编写测试
# 添加测试用例

# 4. 运行测试
python -m pytest python/ray/tests/test_my_feature.py

# 5. 提交代码
git add .
git commit -m "Add my feature"
git push origin feature/my-feature
```

### 代码审查

提交PR后，维护者会进行代码审查，可能包括：

- 代码质量检查
- 性能影响评估
- API设计审查
- 文档完整性检查

## 特定领域贡献

### Ray Core

```python
# Ray核心功能开发注意事项
# 1. 保持向后兼容性
# 2. 确保性能不下降
# 3. 添加充分的错误处理
# 4. 提供清晰的错误消息
```

### Ray Data

```python
# Ray Data开发注意事项
# 1. 关注数据处理性能
# 2. 确保数据一致性
# 3. 优化内存使用
# 4. 支持大数据集处理
```

### Ray Train

```python
# Ray Train开发注意事项
# 1. 分布式训练的正确性
# 2. 模型精度保证
# 3. 资源管理优化
# 4. 检查点机制可靠性
```

## 社区资源

### 联系方式

- **GitHub Issues**: https://github.com/ray-project/ray/issues
- **Discussion Forum**: https://discuss.ray.io/
- **Slack**: https://ray-distributed.slack.com/

### 贡献者指南

1. **从小处开始**：先修复小bug或改进文档
2. **参与讨论**：在GitHub Issues和PR中参与讨论
3. **遵循流程**：严格按照贡献流程操作
4. **保持沟通**：与维护者保持良好沟通

## 代码审查标准

### 代码质量

- 代码清晰易懂
- 适当的注释和文档
- 遵循编码规范
- 充分的测试覆盖

### 性能影响

- 不应显著降低性能
- 需要性能基准测试（如适用）
- 内存使用合理

### API设计

- API设计简洁明了
- 保持向后兼容
- 提供适当的错误处理

## 感谢贡献

Ray项目感谢所有贡献者，包括：

- 代码贡献者
- 文档改进者
- 测试反馈者
- 社区支持者

您的贡献帮助Ray变得更好，使更多用户受益。

通过遵循这些指南，您可以有效地为Ray项目做贡献。欢迎加入Ray社区，一起构建更好的分布式计算平台。