# GORM Gen WITH子句和窗口函数扩展实现总结

## 概述

本次扩展为GORM Gen添加了对PostgreSQL WITH子句（Common Table Expression, CTE）和窗口函数的完整支持，使得开发者可以使用类型安全的Go代码来构建复杂的SQL查询。

## 实现的功能

### 1. WITH子句（CTE）支持

#### 核心结构
- `WithClause`: 表示单个CTE定义
- `WithQuery`: 支持多个CTE的查询构建器
- `WithClauseExpr`: 实现GORM clause.Expression接口

#### 主要方法
```go
// 创建WITH查询
func (d *DO) With(name string, query SubQuery) *WithQuery

// 添加更多CTE
func (w *WithQuery) With(name string, query SubQuery) *WithQuery

// 从指定CTE选择数据
func (w *WithQuery) From(cteName string) Dao

// 执行查询
func (w *WithQuery) Select(columns ...field.Expr) Dao
```

### 2. 窗口函数支持

#### 支持的窗口函数
- **排名函数**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
- **聚合函数**: `COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`

#### 核心结构
- `WindowFunction`: 窗口函数表示
- `OverClause`: OVER子句配置
- `FrameClause`: 窗口框架定义

#### 主要特性
- 完整的PARTITION BY支持
- 完整的ORDER BY支持
- 窗口框架支持（ROWS, RANGE）
- 类型安全的字段表达式处理

## 使用示例

### 基本WITH子句用法

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

### 多CTE查询

```go
// 用户统计CTE
userStatsQuery := record.
    Select(
        record.Unionid,
        record.ID.Count().As("total_records"),
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

### 窗口函数用法

```go
// 各种窗口函数
rowNum := RowNumber().Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt.Desc()).
    As("row_num")

runningSum := Sum(record.Amount).Over().
    PartitionBy(record.Unionid).
    OrderBy(record.CreatedAt).
    Rows("UNBOUNDED PRECEDING", "CURRENT ROW").
    As("running_sum")

result, err := record.
    Select(
        record.ALL,
        rowNum,
        runningSum,
    ).
    Find()
```

## 文件结构

```
├── with.go                    # 核心实现文件
├── with_test.go              # 单元测试
├── with_example.go           # 使用示例
├── example_usage.go          # 完整使用示例
├── field/expr.go             # 扩展了NewExpr函数
├── WITH_WINDOW_FUNCTIONS.md  # 详细文档
└── IMPLEMENTATION_SUMMARY.md # 本总结文档
```

## 技术实现细节

### 1. 字段表达式处理
- 实现了智能的字段表达式字符串化
- 支持`field.IColumnName`接口的字段
- 兼容现有的表达式系统

### 2. SQL构建
- 使用GORM的clause系统构建SQL
- 支持参数绑定和SQL注入防护
- 与现有查询API完全兼容

### 3. 类型安全
- 所有API都是类型安全的
- 编译时检查字段和表达式
- 与生成的查询对象无缝集成

## 测试覆盖

实现了全面的单元测试：
- 窗口函数基本功能测试
- OVER子句配置测试
- 窗口框架测试
- 聚合窗口函数测试
- 复杂窗口函数测试

所有测试都通过，确保功能的正确性和稳定性。

## 性能考虑

### 1. 索引建议
- 为PARTITION BY和ORDER BY字段创建复合索引
- 考虑查询模式优化索引设计

### 2. 查询优化
- 在CTE中尽早过滤数据
- 合理使用窗口框架
- 避免不必要的UNBOUNDED FOLLOWING

### 3. 监控建议
- 使用EXPLAIN ANALYZE分析查询计划
- 监控查询执行时间
- 调整数据库参数（如work_mem）

## 兼容性

- **数据库**: 主要针对PostgreSQL设计，其他数据库可能需要语法调整
- **GORM版本**: 兼容当前GORM版本
- **Go版本**: 支持Go 1.16+

## 扩展建议

### 1. 递归CTE支持
可以通过扩展WithQuery添加RECURSIVE关键字支持：
```go
func (w *WithQuery) Recursive() *WithQuery
```

### 2. 更多窗口函数
可以添加更多PostgreSQL窗口函数：
- `LAG()`, `LEAD()`
- `FIRST_VALUE()`, `LAST_VALUE()`
- `NTH_VALUE()`

### 3. 窗口别名
支持为窗口定义别名以重用：
```go
func (w *WindowFunction) Window(name string) *WindowFunction
```

### 4. 性能分析工具
集成查询性能分析和优化建议功能。

## 总结

本次实现成功为GORM Gen添加了完整的WITH子句和窗口函数支持，提供了：

1. **类型安全**: 编译时检查，减少运行时错误
2. **易用性**: 链式API，符合GORM Gen的设计理念
3. **功能完整**: 支持复杂的CTE和窗口函数查询
4. **性能友好**: 合理的SQL生成和参数绑定
5. **可扩展**: 为未来功能扩展预留了接口

这个扩展使得开发者可以在保持类型安全的同时，构建复杂的分析查询，特别适合需要数据去重、排名、累计统计等场景的应用。 