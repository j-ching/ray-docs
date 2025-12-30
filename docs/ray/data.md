# Ray Data

Ray Data是Ray的分布式数据处理库，专门用于大规模数据加载和预处理。它提供了简单易用的API，用于处理机器学习工作流中的数据管道。

## 什么是Ray Data？

Ray Data是一个用于分布式数据处理的库，旨在高效处理机器学习工作流中的数据加载、转换和预处理任务。它提供了一个延迟执行的DataFrame API，支持多种数据格式和高效的分布式处理。

## 快速开始

### 创建数据集

```python
import ray

# 从Python列表创建数据集
ds = ray.data.from_items([1, 2, 3, 4, 5])
print(ds.take(3))  # 输出: [1, 2, 3]

# 从Pandas DataFrame创建
import pandas as pd
df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
ds = ray.data.from_pandas(df)

# 从文件创建
ds = ray.data.read_csv("s3://bucket/data.csv")
ds = ray.data.read_json("s3://bucket/data.json")
ds = ray.data.read_parquet("s3://bucket/data.parquet")
```

### 基本操作

```python
# 显示数据
ds.show(3)

# 获取数据集统计信息
print(ds.schema())
print(ds.count())

# 转换数据
ds = ds.map(lambda x: x * 2)
ds = ds.filter(lambda x: x > 10)
```

## 数据加载

### 从不同来源加载数据

```python
# 从CSV文件加载
csv_ds = ray.data.read_csv("path/to/file.csv")

# 从JSON文件加载
json_ds = ray.data.read_json("path/to/file.json")

# 从Parquet文件加载
parquet_ds = ray.data.read_parquet("path/to/file.parquet")

# 从S3加载
s3_ds = ray.data.read_csv("s3://bucket/data.csv")

# 从HDFS加载
hdfs_ds = ray.data.read_parquet("hdfs://namenode:port/path/data.parquet")
```

### 自定义数据加载

```python
# 从自定义函数创建数据集
def my_data_generator():
    for i in range(100):
        yield {"id": i, "value": i * 2}

ds = ray.data.from_generator(my_data_generator, output_type=ray.data.Dataset)
```

## 数据转换

### Map操作

```python
# 对每个元素应用函数
ds = ds.map(lambda row: {"x": row["x"], "y_squared": row["y"] ** 2})

# 使用类进行复杂转换
class DataProcessor:
    def __init__(self):
        # 初始化处理逻辑
        pass
    
    def __call__(self, batch):
        # 批处理转换
        import pandas as pd
        df = pd.DataFrame(batch)
        df["processed"] = df["value"] * 2
        return df.to_dict(orient="records")

ds = ds.map_batches(DataProcessor, batch_size=10000)
```

### Filter操作

```python
# 过滤数据
ds = ds.filter(lambda row: row["value"] > 100)

# 复杂过滤条件
ds = ds.filter(lambda row: row["category"] in ["A", "B"] and row["score"] > 0.5)
```

### FlatMap操作

```python
# 将单个元素转换为多个元素
ds = ds.flat_map(lambda row: [{"word": word} for word in row["text"].split()])
```

## 数据聚合

### 基本聚合

```python
# 计算统计信息
count = ds.count()
mean_value = ds.mean("value")
max_value = ds.max("value")
min_value = ds.min("value")
std_value = ds.std("value")

# 多列聚合
aggregates = ds.aggregate(
    ray.data.aggregate.Mean("value"),
    ray.data.aggregate.Std("value"),
    ray.data.aggregate.Count()
)
```

### 分组聚合

```python
# 按列分组
grouped_ds = ds.groupby("category").aggregate(
    ray.data.aggregate.Mean("value"),
    ray.data.aggregate.Count()
)

# 转换为Pandas DataFrame
result_df = grouped_ds.to_pandas()
```

## 数据处理管道

### 构建数据处理管道

```python
def preprocess_pipeline(dataset):
    # 数据清洗
    dataset = dataset.filter(lambda row: row["value"] is not None)
    
    # 特征工程
    dataset = dataset.map(lambda row: {
        **row,
        "normalized_value": (row["value"] - mean) / std,
        "feature_squared": row["feature"] ** 2
    })
    
    # 采样
    dataset = dataset.random_sample(0.1)  # 采样10%的数据
    
    return dataset

# 应用管道
processed_ds = preprocess_pipeline(raw_ds)
```

## 数据分区

### 重新分区

```python
# 重新分区以优化并行度
ds = ds.repartition(10)  # 分成10个分区

# 按列值分区
ds = ds.randomize_blocks()  # 随机化块
ds = ds.sort("timestamp")   # 按时间戳排序
```

### 分区操作

```python
# 对每个分区应用操作
def process_partition(partition_iterable):
    # 在单个节点上处理整个分区
    rows = list(partition_iterable)
    # 执行密集计算
    processed_rows = [process_row(row) for row in rows]
    return processed_rows

ds = ds.map_batches(
    process_partition,
    batch_size=None,  # 整个分区作为一个批次
    zero_copy_batch=True
)
```

## 与机器学习框架集成

### 与PyTorch集成

```python
import torch
from torch.utils.data import DataLoader

# 转换为PyTorch DataLoader
torch_ds = ds.iter_torch_batches(batch_size=32)

# 在训练循环中使用
for batch in torch_ds:
    # batch是PyTorch张量
    outputs = model(batch["features"])
    loss = criterion(outputs, batch["labels"])
```

### 与TensorFlow集成

```python
import tensorflow as tf

# 转换为TensorFlow数据集
tf_ds = ds.to_tf(
    label_column="label",
    output_signature=(
        {"features": tf.TensorSpec(shape=(None,), dtype=tf.float32)},
        tf.TensorSpec(shape=(), dtype=tf.int32)
    ),
    batch_size=32
)
```

### 与Scikit-learn集成

```python
# 转换为NumPy数组用于scikit-learn
features = ds.select_columns(["feature1", "feature2"]).to_numpy()
labels = ds.select_column("label").to_numpy()

# 在scikit-learn模型中使用
from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier()
model.fit(features, labels)
```

## 性能优化

### 批处理转换

```python
# 使用批处理转换提高性能
def batch_processor(batch):
    import pandas as pd
    df = pd.DataFrame(batch)
    # 向量化操作
    df["new_feature"] = df["feature1"] * df["feature2"]
    return df.to_dict(orient="records")

ds = ds.map_batches(
    batch_processor,
    batch_size=10000,  # 批处理大小
    num_cpus=1         # 每个任务使用的CPU数
)
```

### 零拷贝优化

```python
# 使用零拷贝批处理以提高性能
ds = ds.map_batches(
    my_processing_function,
    batch_size=10000,
    zero_copy_batch=True,  # 启用零拷贝
    batch_format="pandas"  # 使用Pandas格式
)
```

## 数据保存

### 保存到不同格式

```python
# 保存为CSV
ds.write_csv("s3://bucket/processed_data.csv")

# 保存为JSON
ds.write_json("s3://bucket/processed_data.json")

# 保存为Parquet
ds.write_parquet("s3://bucket/processed_data.parquet")

# 自定义保存
def save_to_custom_format(block, path):
    # 自定义保存逻辑
    pass

ds.write_datasource(
    ray.data.datasource.FileBasedDatasource(),
    path="path/to/output"
)
```

## 与Ray AIR集成

```python
from ray.air import session
from ray.air.config import DatasetConfig

# 在Ray AIR中使用Ray Data
def train_func(config):
    # 获取数据集
    train_dataset = session.get_dataset_shard("train")
    
    # 预处理数据
    train_dataset = train_dataset.map_batches(
        preprocessing_fn,
        batch_size=10000
    )
    
    # 训练模型
    for batch in train_dataset.iter_torch_batches(batch_size=32):
        # 训练逻辑
        pass

# 配置数据集
trainer = MyTrainer(
    train_loop_per_worker=train_func,
    datasets={"train": train_ds},
    dataset_config=DatasetConfig(
        "train",
        train_func=train_func,
        split_ratio=0.8
    )
)
```

## 最佳实践

### 内存管理

1. **使用批处理**：对转换操作使用批处理以减少开销
2. **控制分区大小**：保持分区大小在合理范围内（100MB-1GB）
3. **及时释放资源**：不再需要时释放数据集资源

### 性能优化

1. **选择合适的批处理大小**：通常在1000-10000之间
2. **使用零拷贝批处理**：当可能时启用零拷贝
3. **预分区数据**：根据后续处理需求预分区数据

### 数据质量

1. **验证数据**：在处理前验证数据质量和格式
2. **处理缺失值**：明确处理缺失或无效数据
3. **监控数据分布**：确保数据分布符合预期

Ray Data通过提供简单易用的API和强大的分布式处理能力，大大简化了大规模数据处理任务，特别适用于机器学习工作流中的数据预处理需求。