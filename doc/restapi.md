# REST API Design Doc

## REST API LIST

| ID | Type | Description | Resources | Http Method | URL Params | Request Data(json) | Reponse Data(json) | Comment |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | Job | 创建任务 | /jobs | POST | | $job | | |
| 2 | Job | 获取任务列表 | /jobs | GET | userId=$userId, start=$start, length=$length | | $jobs | |
| 3 | Job | 获取运行结果 | /jobs/result/$id | GET | | | $result | |
| 4 | Job | 下载运行结果 | /jobs/download/$id | GET | start=$start, length=$length | | | |
| 5 | TableInfo | 获取表元信息 | /tableInfos | GET | | | $tableInfos | |
| 6 | FieldInfo | 获取字段元信息 | /fieldInfos | GET | tableId=$tableId | | $fieldInfos | |
| 7 | EnumInfo | 获取枚举元信息 | /enumInfos | GET | fieldId=$fieldId | | $enumInfos | |
| 8 | MethodInfo | 获取函数元信息 | /methodInfos | GET | | | $methodInfo | |
| 9 | EngineInfo | 获取引擎列表 | /engineInfos | GET | | | $engineInfos | |

## Resources Format

### Job Resource Format

```
{
  id: $id,
  userId: $userId,  // needed
  state: $state,
  startTime: $startTime,
  endTime: $endTime,
  queryType: $queryType,  // needed, RAW or COMPOUND
  queryPlan: $queryPlan,  // needed
  executionPlan: $executionPlan,
  host: $host
}
```

##### Raw Query

```
{
  engine: $engine, // PRESTO or SPARK-SQL
  sql: $sql,
  info: { $key: $value }  // optional
}
```

##### Compound Query

```
{
  tableName: $tableName,
  metrics: [
    {
      method: $method,
      field: $field,
      alias: $alias // optional
    }
  ],
  buckets: [
    {
      field: $field,
      alias: $alias // optional
    }
  ],
  filters: [
    {
      fieldType: $fieldType,  // WHERE or HAVING
      dataType: $dataType,
      field: $field,
      method: $method,
      value: $value
    }
  ],
  timerange: {
    startTime: $startTime,
    endTime: $endTime
  }
}
```

### TableInfo Resource Format

```
{
  id: $id,
  name: $name,
  description: $description,
  frequency: $frequency
}
```

### FieldInfo Resource Format

```
{
  id: $id,
  tableId: $tableId,
  name: $name,
  description: $description,
  comment: $comment,
  dataType: $dataType,
  fieldType: $fieldType,
  isEnum: $isEnum
}
```

### EnumInfo Resource Format

```
{
  id: $id,
  fieldId: $fieldId,
  value: $value,
  description: $description
}
```

### MethodInfo Resource Format

```
{
  metricMethods: [
    {
      id: $id,
      name: $name,
      description: $description,
      dataType: $dataType
    }
  ],
  filterMethods: [
    {
      id: $id,
      name: $name,
      description: $description
    }
  ]
}

### EngineInfo Resource Format

```
{
  id: $id,
  name: $name,
  description: $description
}

```