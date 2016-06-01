# REST API Design Doc

## REST API LIST

| ID | Type | Description | Resources | Http Method | URL Params | Request Data(json) | Reponse Data(json) | Comment |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | Task | 创建任务 | /tasks | POST | | $task | | |
| 2 | Task | 获取任务列表 | /tasks | GET | userId=$userId | | $taskList | |
| 3 | TableInfo | 获取数据仓库元信息 | /tableInfos | GET | tableName=$tableName | | $tableInfo | |
| 4 | MethodInfo | 获取函数元信息 | /methodInfos | GET | | | $methodInfo | |

## Resources Format

### Task Resource Format

```json
{
  id: $id,
  userId: $userId,  // needed
  state: $state,
  startTime: $startTime,
  endTime: $endTime,
  queryTerm: $queryTerm,  // needed
  executionPlan: $executionPlan,
  host: $host,
  reportURL: $reportURL
}
```

#### QueryTerm Field Format (JSON)

```json
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

```json
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

```json
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