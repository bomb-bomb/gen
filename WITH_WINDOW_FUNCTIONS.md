# WITH子句和窗口函数支持

本扩展为GORM Gen添加了对PostgreSQL WITH子句（Common Table Expression, CTE）和窗口函数的支持。

## 功能特性

### 1. WITH子句（CTE）支持
- 支持单个和多个CTE
- 支持递归CTE（通过原生SQL）
- 与现有查询API完全兼容

### 2. 窗口函数支持
- ROW_NUMBER(), RANK(), DENSE_RANK()
- 聚合窗口函数：COUNT(), SUM(), AVG(), MAX(), MIN()
- 完整的OVER子句支持：PARTITION BY, ORDER BY
- 窗口框架支持：ROWS, RANGE

## 基本用法

### 简单的WITH子句

```go
// 原始SQL:
// WITH ranked_records AS (
//   SELECT *, ROW_NUMBER() OVER (PARTITION BY unionid, platform_name ORDER BY created_at DESC) AS rn
//   FROM project_user_message_push_device_binding_records
//   WHERE unionid IN ('user1', 'user2')
// )
// SELECT * FROM ranked_records WHERE rn = 1;

// Gen代码:
baseQuery := record.
    Select(
        record.ALL,
        RowNumber().Over().
            PartitionBy(record.Unionid, record.PlatformName).
            OrderBy(record.CreatedAt.Desc()).
            As("rn"),
    ).
    Where(record.Unionid.In("user1", "user2"))

result, err := record.
    With("ranked_records", baseQuery).
    From("ranked_records").
    Where(field.NewExpr("rn", clause.Expr{SQL: "rn"}).Eq(1)).
    Find()
```

### 多个CTE

```go
// 用户统计CTE
userStatsQuery := record.
    Select(
        record.Unionid,
        record.Unionid.Count().As("total_records"),
        record.CreatedAt.Max().As("last_activity"),
    ).
    Group(record.Unionid)

// 排名CTE
rankedUsersQuery := q.Table("user_stats").
    Select(
        field.Star,
        Rank().Over().
            OrderBy(field.NewExpr("total_records", clause.Expr{SQL: "total_records"}).Desc()).
            As("user_rank"),
    )

// 最终查询
result, err := record.
    With("user_stats", userStatsQuery).
    With("ranked_users", rankedUsersQuery).
    From("ranked_users").
    Where(field.NewExpr("user_rank", clause.Expr{SQL: "user_rank"}).Lte(10)).
    Find()
```

## 窗口函数详解

### 排名函数

```go
// ROW_NUMBER() - 为每行分配唯一的序号
rowNum := RowNumber().Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt.Desc()).
    As("row_num")

// RANK() - 排名（相同值有相同排名，后续排名跳跃）
rank := Rank().Over().
    OrderBy(record.Score.Desc()).
    As("rank")

// DENSE_RANK() - 密集排名（相同值有相同排名，后续排名连续）
denseRank := DenseRank().Over().
    OrderBy(record.Score.Desc()).
    As("dense_rank")
```

### 聚合窗口函数

```go
// 分区内计数
totalCount := Count(record.ID).Over().
    PartitionBy(record.Unionid).
    As("total_in_partition")

// 累计求和
runningSum := Sum(record.Amount).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Rows("UNBOUNDED PRECEDING", "CURRENT ROW").
    As("running_sum")

// 移动平均
movingAvg := Avg(record.Amount).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Rows("2 PRECEDING", "2 FOLLOWING").
    As("moving_avg")
```

### 窗口框架

```go
// ROWS框架 - 基于物理行数
rowsFrame := Sum(record.Amount).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Rows("2 PRECEDING", "2 FOLLOWING").
    As("sum_5_rows")

// RANGE框架 - 基于逻辑范围
rangeFrame := Avg(record.Amount).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Range("INTERVAL '1 DAY' PRECEDING", "CURRENT ROW").
    As("avg_last_day")

// 无界框架
unboundedFrame := Count(record.ID).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Rows("UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING").
    As("total_count")
```

## API参考

### WithQuery方法

```go
type WithQuery struct {
    *DO
    withClauses []WithClause
}

// 创建WITH查询
func (d *DO) With(name string, query SubQuery) *WithQuery

// 添加更多CTE
func (w *WithQuery) With(name string, query SubQuery) *WithQuery

// 从指定CTE选择
func (w *WithQuery) From(cteName string) Dao

// 执行查询
func (w *WithQuery) Select(columns ...field.Expr) Dao
```

### 窗口函数

```go
// 排名函数
func RowNumber() *WindowFunction
func Rank() *WindowFunction
func DenseRank() *WindowFunction

// 聚合函数
func Count(expr field.Expr) *WindowFunction
func Sum(expr field.Expr) *WindowFunction
func Avg(expr field.Expr) *WindowFunction
func Max(expr field.Expr) *WindowFunction
func Min(expr field.Expr) *WindowFunction

// OVER子句
func (w *WindowFunction) Over() *OverClause
func (o *OverClause) PartitionBy(exprs ...field.Expr) *OverClause
func (o *OverClause) OrderBy(exprs ...field.Expr) *OverClause

// 窗口框架
func (o *OverClause) Rows(start, end string) *OverClause
func (o *OverClause) Range(start, end string) *OverClause

// 创建字段表达式
func (w *WindowFunction) As(alias string) field.Expr
```

## 常见用例

### 1. 去重（获取每组最新记录）

```go
baseQuery := record.
    Select(
        record.ALL,
        RowNumber().Over().
            PartitionBy(record.Unionid, record.PlatformName).
            OrderBy(record.CreatedAt.Desc()).
            As("rn"),
    ).
    Where(record.Unionid.In(userIds...))

result, err := record.
    With("ranked_records", baseQuery).
    From("ranked_records").
    Where(field.NewExpr("rn", clause.Expr{SQL: "rn"}).Eq(1)).
    Find()
```

### 2. 排行榜

```go
rankedQuery := record.
    Select(
        record.ALL,
        Rank().Over().OrderBy(record.Score.Desc()).As("rank"),
        DenseRank().Over().OrderBy(record.Score.Desc()).As("dense_rank"),
    )

result, err := record.
    With("ranked_users", rankedQuery).
    From("ranked_users").
    Where(field.NewExpr("rank", clause.Expr{SQL: "rank"}).Lte(100)).
    Find()
```

### 3. 累计统计

```go
statsQuery := record.
    Select(
        record.ALL,
        Sum(record.Amount).Over().
            PartitionBy(record.Unionid).
            OrderBy(record.CreatedAt).
            Rows("UNBOUNDED PRECEDING", "CURRENT ROW").
            As("running_total"),
        Avg(record.Amount).Over().
            PartitionBy(record.Unionid).
            OrderBy(record.CreatedAt).
            Rows("6 PRECEDING", "CURRENT ROW").
            As("avg_7_days"),
    )

result, err := statsQuery.Find()
```

## 注意事项

1. **性能考虑**：窗口函数可能比较耗时，建议在大数据集上使用时添加适当的WHERE条件和索引。

2. **数据库兼容性**：此功能主要针对PostgreSQL设计，其他数据库可能需要调整语法。

3. **字段引用**：在CTE中引用字段时，使用`field.NewExpr()`创建表达式。

4. **错误处理**：确保正确处理查询错误，特别是在复杂的CTE查询中。

## 扩展建议

1. **递归CTE支持**：可以通过扩展WithQuery添加RECURSIVE关键字支持。

2. **更多窗口函数**：可以添加LAG(), LEAD(), FIRST_VALUE(), LAST_VALUE()等函数。

3. **窗口函数别名**：支持为窗口定义别名以重用。

4. **性能优化**：添加查询计划分析和优化建议。 