# Serve快速入门

Ray Serve是用于模型服务的可扩展、灵活的工具包。本节介绍如何使用Ray Serve快速构建和部署模型服务。

## 安装Ray Serve

```bash
pip install "ray[serve]"
```

## 基本概念

Ray Serve的主要组件：

- **Deployment**：封装您的模型或服务逻辑
- **Endpoint**：服务的HTTP端点
- **Router**：将请求路由到适当的Deployment
- **Application**：一个完整的Serve应用程序

## 简单示例

```python
from ray import serve
import requests

# 定义一个简单的服务
@serve.deployment
def hello_world(request):
    name = request.query_params.get("name", "world")
    return f"Hello {name}!"

# 运行服务
serve.run(hello_world.bind())

# 发送请求
response = requests.get("http://localhost:8000/hello_world?name=Ray")
print(response.text)  # 输出: Hello Ray!
```

## 有状态服务

```python
@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0
    
    def __call__(self, request):
        self.count += 1
        return {"count": self.count}

# 部署有状态服务
serve.run(Counter.bind())

# 发送请求
response = requests.get("http://localhost:8000/Counter")
print(response.json())  # 输出: {"count": 1}
```

## 多个服务

```python
@serve.deployment
def api_one(request):
    return "API One Response"

@serve.deployment
def api_two(request):
    return "API Two Response"

# 部署多个服务
serve.run(api_one.bind())
serve.run(api_two.bind())

# 访问不同的服务
# http://localhost:8000/api_one
# http://localhost:8000/api_two
```

## 模型服务示例

```python
import numpy as np
from ray import serve
from sklearn.ensemble import RandomForestClassifier
import pickle

@serve.deployment
class ModelDeployment:
    def __init__(self):
        # 加载预训练模型
        # 这里只是一个示例，实际应用中您会加载真实的模型
        self.model = RandomForestClassifier()
        # 通常从检查点加载模型
        # self.model = pickle.load(open("model.pkl", "rb"))
    
    def __call__(self, request):
        # 解析请求数据
        input_data = request.json()
        
        # 预处理输入数据
        features = np.array(input_data["features"])
        
        # 进行预测
        prediction = self.model.predict(features.reshape(1, -1))
        
        # 返回结果
        return {"prediction": int(prediction[0])}

# 部署模型服务
serve.run(ModelDeployment.bind())
```

## 配置选项

```python
@serve.deployment(
    num_replicas=2,  # 运行2个副本以实现负载均衡
    max_concurrent_queries=100,  # 最大并发查询数
    user_config={"model_path": "/path/to/model"},  # 用户配置
    ray_actor_options={"num_cpus": 1, "num_gpus": 0.5}  # Ray actor选项
)
class ConfigurableModel:
    def __init__(self):
        self.model = None
        self.reconfigure({"model_path": "/default/path"})
    
    def reconfigure(self, config):
        # 根据配置重新配置模型
        model_path = config["model_path"]
        # 加载模型
        print(f"重新配置模型路径: {model_path}")
    
    def __call__(self, request):
        return {"status": "model running"}

serve.run(ConfigurableModel.bind())
```

## 使用类作为服务

```python
@serve.deployment
class Adder:
    def __init__(self, increment: int = 1):
        self.increment = increment
    
    def __call__(self, request):
        input_number = request.json()["number"]
        result = input_number + self.increment
        return {"result": result}

# 部署服务，指定构造函数参数
serve.run(Adder.options(init_args=(5,)).bind())

# 发送请求
# POST {"number": 10} to http://localhost:8000/Adder
# 返回 {"result": 15}
```

## 组合服务

```python
@serve.deployment
class TextModel:
    def __call__(self, text: str):
        # 模拟文本处理
        return f"Processed: {text.upper()}"

@serve.deployment
class TextProcessor:
    def __init__(self):
        # 获取对其他部署的引用
        self.text_model = TextModel.get_handle()
    
    async def __call__(self, request):
        text = request.json()["text"]
        
        # 调用另一个服务
        result = await self.text_model.remote(text)
        
        return {"original": text, "processed": result}

# 部署组合服务
TextModel.deploy()
TextProcessor.deploy()

# 发送请求到TextProcessor
# 它会调用TextModel服务
```

## HTTP中间件

```python
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# 使用中间件
@serve.deployment
class APIService:
    async def __call__(self, request):
        return {"message": "Hello from API"}

# 部署时应用中间件
serve.run(
    APIService.bind(),
    route_prefix="/api",
    host="0.0.0.0",
    port=8000
)
```

## 服务配置

```python
from ray.serve.config import AutoscalingConfig

# 自动扩缩容配置
autoscaling_config = AutoscalingConfig(
    min_replicas=1,
    max_replicas=5,
    target_num_ongoing_requests_per_replica=3,
    upscale_smoothing_factor=1.0,
    downscale_smoothing_factor=1.0,
)

@serve.deployment(
    autoscaling_config=autoscaling_config,
    # 其他配置...
)
class AutoScalingModel:
    def __call__(self, request):
        return {"status": "auto-scaling model"}

serve.run(AutoScalingModel.bind())
```

## 错误处理

```python
@serve.deployment
class ErrorHandlingService:
    def __call__(self, request):
        try:
            # 处理请求
            data = request.json()
            result = 10 / data.get("divisor", 1)
            return {"result": result}
        except ZeroDivisionError:
            return {"error": "Cannot divide by zero"}, 400
        except Exception as e:
            return {"error": str(e)}, 500

serve.run(ErrorHandlingService.bind())
```

## 最佳实践

1. **资源管理**：为每个部署指定适当的资源需求
2. **并发控制**：设置合理的并发查询限制
3. **健康检查**：实现健康检查端点
4. **监控**：使用Ray Dashboard监控服务性能
5. **版本管理**：使用版本控制来管理模型更新
6. **负载均衡**：使用多个副本来处理高并发请求

## 启动Serve

您也可以通过命令行启动Serve：

```bash
ray start --head
serve run my_app:deployment
```

或者在Python脚本中：

```python
import ray
from ray import serve

# 初始化Ray
ray.init()

# 启动Serve
serve.start()

# 部署服务
@serve.deployment
def my_service(request):
    return "Hello from Ray Serve!"

# 运行服务
my_service.deploy()

# 稍后清理
serve.shutdown()
ray.shutdown()
```