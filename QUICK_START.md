# GORM Gen WITH子句和窗口函数 - 快速开始指南

## 快速开始

### 1. 基本设置

```go
package main

import (
    "gorm.io/gen"
    "gorm.io/gorm"
)

func main() {
    // 连接数据库
    db, err := gorm.Open(/* your database driver */)
    if err != nil {
        panic(err)
    }

    // 创建生成器
    g := gen.NewGenerator(gen.Config{
        OutPath: "./query",
        Mode:    gen.WithDefaultQuery | gen.WithQueryInterface,
    })
    g.UseDB(db)

    // 生成查询代码
    g.ApplyBasic(&YourModel{})
    g.Execute()
}
```

### 2. 使用WITH子句去重数据

```go
// 获取每个用户在每个平台的最新记录
baseQuery := record.
    Select(
        record.ALL,
        gen.RowNumber().Over().
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

### 3. 使用窗口函数进行排名

```go
// 用户活跃度排名
rankedQuery := record.
    Select(
        record.ALL,
        gen.Rank().Over().
            OrderBy(record.CreatedAt.Desc()).
            As("activity_rank"),
        gen.Count(record.ID).Over().
            PartitionBy(record.Unionid).
            As("total_records"),
    )

result, err := rankedQuery.Find()
```

### 4. 累计统计

```go
// 累计计数和移动平均
statsQuery := record.
    Select(
        record.ALL,
        gen.Sum(record.Amount).Over().
            PartitionBy(record.Unionid).
            OrderBy(record.CreatedAt).
            Rows("UNBOUNDED PRECEDING", "CURRENT ROW").
            As("running_total"),
        gen.Avg(record.Amount).Over().
            PartitionBy(record.Unionid).
            OrderBy(record.CreatedAt).
            Rows("2 PRECEDING", "2 FOLLOWING").
            As("moving_avg"),
    )

result, err := statsQuery.Find()
```

## 支持的窗口函数

### 排名函数
- `gen.RowNumber()` - 行号
- `gen.Rank()` - 排名（有间隙）
- `gen.DenseRank()` - 密集排名（无间隙）

### 聚合函数
- `gen.Count(field)` - 计数
- `gen.Sum(field)` - 求和
- `gen.Avg(field)` - 平均值
- `gen.Max(field)` - 最大值
- `gen.Min(field)` - 最小值

## OVER子句配置

```go
windowFunc := gen.RowNumber().Over().
    PartitionBy(field1, field2).    // 分区
    OrderBy(field3.Desc()).         // 排序
    Rows("1 PRECEDING", "1 FOLLOWING"). // 窗口框架
    As("alias")                     // 别名
```

## 窗口框架类型

### ROWS框架（基于物理行）
```go
.Rows("UNBOUNDED PRECEDING", "CURRENT ROW")
.Rows("1 PRECEDING", "1 FOLLOWING")
.Rows("2 PRECEDING", "CURRENT ROW")
```

### RANGE框架（基于逻辑范围）
```go
.Range("UNBOUNDED PRECEDING", "CURRENT ROW")
.Range("INTERVAL '1 DAY' PRECEDING", "CURRENT ROW")
```

## 实际应用场景

### 1. 数据去重
```go
// 每个用户保留最新记录
dedupeQuery := record.
    Select(
        record.ALL,
        gen.RowNumber().Over().
            PartitionBy(record.UserID).
            OrderBy(record.UpdatedAt.Desc()).
            As("rn"),
    )

latest, err := record.
    With("latest_records", dedupeQuery).
    From("latest_records").
    Where(field.NewExpr("rn", clause.Expr{SQL: "rn"}).Eq(1)).
    Find()
```

### 2. 排行榜
```go
// 用户积分排行榜
leaderboard := user.
    Select(
        user.ALL,
        gen.Rank().Over().
            OrderBy(user.Score.Desc()).
            As("rank"),
    ).
    Where(user.Status.Eq(1))

topUsers, err := leaderboard.
    Where(field.NewExpr("rank", clause.Expr{SQL: "rank"}).Lte(100)).
    Find()
```

### 3. 时间序列分析
```go
// 每日累计销售额
dailySales := order.
    Select(
        order.Date,
        order.Amount.Sum().As("daily_total"),
        gen.Sum(order.Amount).Over().
            OrderBy(order.Date).
            Rows("UNBOUNDED PRECEDING", "CURRENT ROW").
            As("cumulative_total"),
    ).
    Group(order.Date)

result, err := dailySales.Find()
```

## 性能优化提示

1. **创建合适的索引**
   ```sql
   CREATE INDEX idx_partition_order ON table_name (partition_col, order_col);
   ```

2. **在CTE中尽早过滤**
   ```go
   baseQuery := record.
       Where(record.Status.Eq(1)).  // 先过滤
       Select(/* ... */)
   ```

3. **选择合适的窗口框架**
   - ROWS通常比RANGE性能更好
   - 避免不必要的UNBOUNDED FOLLOWING

4. **监控查询性能**
   ```sql
   EXPLAIN ANALYZE SELECT ...
   ```

## 注意事项

- 主要针对PostgreSQL设计
- 需要GORM v1.20+
- 在CTE中引用计算字段时使用`field.NewExpr()`
- 大数据集上使用窗口函数时注意性能

## 更多示例

查看以下文件获取更多详细示例：
- `WITH_WINDOW_FUNCTIONS.md` - 完整功能文档
- `example_usage.go` - 实际使用示例
- `with_test.go` - 单元测试示例 