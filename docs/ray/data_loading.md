# 数据加载和保存

Ray Data提供高效的数据加载和保存功能，支持多种数据格式和存储系统。本节介绍如何使用Ray Data进行大规模数据操作。

## 基本数据加载

Ray Data支持从多种数据源加载数据：

```python
import ray

# 从Python列表创建数据集
ds = ray.data.from_items([1, 2, 3, 4, 5])
print(ds.take(3))  # [1, 2, 3]

# 从Pandas DataFrame创建数据集
import pandas as pd
df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
ds = ray.data.from_pandas(df)

# 从NumPy数组创建数据集
import numpy as np
arr = np.array([[1, 2], [3, 4], [5, 6]])
ds = ray.data.from_numpy(arr)
```

## 从文件加载数据

### CSV文件

```python
# 从单个CSV文件加载
ds = ray.data.read_csv("path/to/file.csv")

# 从多个CSV文件加载
ds = ray.data.read_csv(["file1.csv", "file2.csv", "file3.csv"])

# 从目录中的所有CSV文件加载
ds = ray.data.read_csv("path/to/csv/directory/")

# 带选项的CSV加载
ds = ray.data.read_csv(
    "data.csv",
    parallelism=10,  # 并行度
    include_paths=True,  # 包含文件路径
    filesystem="pyarrow"  # 文件系统
)
```

### JSON文件

```python
# 读取JSON文件
ds = ray.data.read_json("data.json")

# 读取JSONL（每行一个JSON对象）文件
ds = ray.data.read_json("data.jsonl")

# 读取多个JSON文件
ds = ray.data.read_json(["file1.json", "file2.json"])
```

### Parquet文件

```python
# 读取Parquet文件
ds = ray.data.read_parquet("data.parquet")

# 读取目录中的Parquet文件
ds = ray.data.read_parquet("path/to/parquet/directory/")

# 使用读取选项
ds = ray.data.read_parquet(
    "data.parquet",
    columns=["col1", "col2"],  # 只读取特定列
    parallelism=5
)
```

### 二进制文件

```python
# 读取二进制文件（如图像）
ds = ray.data.read_binary_files("path/to/images/*.jpg")

# 读取文本文件
ds = ray.data.read_text("path/to/text/files/*.txt")
```

## 从数据库加载数据

```python
# 从SQL数据库加载数据
import sqlite3

def load_from_db():
    conn = sqlite3.connect("example.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name")
    rows = cursor.fetchall()
    conn.close()
    return ray.data.from_items(rows)

# 或者使用Ray Data的内置支持
ds = ray.data.from_items(load_from_db())
```

## 数据保存

### 保存为CSV

```python
# 将数据集保存为CSV文件
ds = ray.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))

# 保存为单个CSV文件
ds.write_csv("output.csv")

# 保存为多个分片的CSV文件
ds.write_csv("output_directory/")
```

### 保存为Parquet

```python
# 保存为Parquet格式
ds.write_parquet("output.parquet")

# 保存为分片的Parquet文件
ds.write_parquet("output_directory/")

# 使用保存选项
ds.write_parquet(
    "output.parquet",
    filesystem="pyarrow",  # 文件系统
    existing_data_behavior="delete_matching"  # 如何处理现有数据
)
```

### 保存为JSON

```python
# 保存为JSON
ds.write_json("output.json")

# 保存为JSONL
ds.write_json("output.jsonl")
```

## 云存储集成

Ray Data支持与各种云存储系统集成：

```python
# 从S3加载数据
ds = ray.data.read_csv("s3://my-bucket/path/to/data.csv")

# 保存到S3
ds.write_parquet("s3://my-bucket/output/")

# 使用S3选项
import boto3
s3_client = boto3.client('s3')
ds.write_parquet(
    "s3://my-bucket/output/",
    # 可以传递S3客户端或配置
)
```

## 自定义数据加载

```python
# 使用自定义函数加载数据
def custom_loader():
    # 自定义数据加载逻辑
    data = []
    for i in range(1000):
        data.append({"id": i, "value": i * 2})
    return data

ds = ray.data.from_items(custom_loader())

# 使用生成器加载大量数据
def data_generator():
    for i in range(100000):
        yield {"index": i, "data": f"item_{i}"}

ds = ray.data.from_items(list(data_generator()))
```

## 性能优化

### 并行加载

```python
# 控制加载的并行度
ds = ray.data.read_csv("large_file.csv", parallelism=20)

# 优化块大小
ds = ray.data.read_parquet(
    "data.parquet",
    ray_remote_args={"num_cpus": 1}  # 为每个读取任务分配资源
)
```

### 数据格式选择

```python
# 对于分析工作负载，Parquet通常是最佳选择
# 对于临时数据，CSV可能更合适
# 对于机器学习，考虑使用TFRecord或类似的格式

# 使用适当的块大小
ds = ray.data.read_parquet(
    "data.parquet",
    # 块大小影响内存使用和并行处理
)
```

## 错误处理

```python
# 处理加载错误
try:
    ds = ray.data.read_csv("potentially_corrupted_file.csv")
    # 检查数据集是否成功创建
    count = ds.count()
    print(f"成功加载 {count} 行数据")
except Exception as e:
    print(f"加载失败: {e}")
    
    # 尝试修复或使用替代方案
    ds = ray.data.from_pandas(pd.read_csv("potentially_corrupted_file.csv", error_bad_lines=False))
```

## 最佳实践

1. **选择合适的数据格式**：Parquet用于分析，CSV用于临时数据
2. **调整并行度**：根据集群大小和数据量调整
3. **使用分片**：将大文件分割成多个块以提高并行处理能力
4. **监控内存使用**：避免加载超过可用内存的数据
5. **利用云存储**：对于大文件，直接从云存储加载而不是下载到本地