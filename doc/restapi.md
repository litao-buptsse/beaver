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
| 7 | MethodInfo | 获取函数元信息 | /methodInfos | GET | | | $methodInfo | |

## Resources Format

### Job Resource Format

```
{
  id: $id,
  userId: $userId,  // needed
  state: $state,
  startTime: $startTime,
  endTime: $endTime,
  queryPlan: $queryPlan,  // needed
  executionPlan: $executionPlan,
  host: $host
}
```

#### QueryPlan Field Format (JSON)

```
{
  type: $type // raw, compound
  query: $query
}
```

##### Raw Query Field

```
sql
```

##### Compound Query Filed (JSON)

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
  type: $type
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
      types: $types
    }
  ],
  filterMethods: [
    {
      id: $id,
      name: $name,
      description: $description,
      types: $types
    }
  ]
}
```