# REST API Design Doc

## REST API LIST

| ID | Type | Description | Resources | Http Method | URL Params | Request Data(json) | Reponse Data(json) | Comment |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | Job | 创建任务 | /jobs | POST | | $job | | |
| 2 | Job | 获取任务列表 | /jobs | GET | userId=$userId | | $jobList | |
| 3 | TableInfo | 获取数据仓库元信息 | /tableInfos | GET | tableName=$tableName | | $tableInfo | |
| 4 | MethodInfo | 获取函数元信息 | /methodInfos | GET | | | $methodInfo | |

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
  host: $host,
  reportURL: $reportURL
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
      alias: $alias
    }
  ],
  buckets: [
    {
      field: $field,
      alias: $alias
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
  tableName: $tableName,
  frequency: $frequency,
  fields: [
    {
      field: $field,
      description: $description,
      enum: $canEnum,
      values: [
        value: $value,
        description: $description
      ]
    }
  ]
}
```

### MethodInfo Resource Format

```
{
  metrics: [
    method: $method,
    description: $description
  ],
  buckets: [
    method: $method,
    description: $description
  ],
  filters: [
    method: $method,
    description: $description
  ]
}
```