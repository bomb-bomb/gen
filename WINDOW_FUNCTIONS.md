# PostgreSQL 窗口函数扩展

本扩展为 GORM Gen 添加了对 PostgreSQL 窗口函数的完整支持，允许你以类型安全的方式构建复杂的窗口函数查询。

## 功能特性

- ✅ 完整的窗口函数支持（ROW_NUMBER, RANK, LAG, LEAD 等）
- ✅ 灵活的窗口规范定义（PARTITION BY, ORDER BY, FRAME）
- ✅ 类型安全的API设计
- ✅ 与现有查询方法无缝集成
- ✅ 支持复杂的窗口帧定义
- ✅ 链式调用风格

## 快速开始

### 基础用法

```go
// 简单的 ROW_NUMBER 窗口函数
result := u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc()).
    Select(u.ALL, field.RowNumberFunc().As("row_num")).
    Find()

// 生成的SQL:
// SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num 
// FROM users
```

### 支持的窗口函数

#### 1. 排名函数

```go
// ROW_NUMBER() - 行号
u.With().Window(field.RowNumberFunc())

// RANK() - 排名（有并列）
u.With().Window(field.RankFunc())

// DENSE_RANK() - 密集排名
u.With().Window(field.DenseRankFunc())

// PERCENT_RANK() - 百分比排名
u.With().Window(u.Salary.PercentRank())

// CUME_DIST() - 累积分布
u.With().Window(u.Salary.CumeDist())

// NTILE(n) - 分组
u.With().Window(u.Salary.Ntile(4))
```

#### 2. 值函数

```go
// LAG() - 获取前面的值
u.With().Window(u.Salary.Lag(1))           // 前1行
u.With().Window(u.Salary.Lag(2, 0))        // 前2行，默认值0

// LEAD() - 获取后面的值
u.With().Window(u.Salary.Lead(1))          // 后1行
u.With().Window(u.Salary.Lead(2, 0))       // 后2行，默认值0

// FIRST_VALUE() - 窗口第一个值
u.With().Window(u.Salary.FirstValue())

// LAST_VALUE() - 窗口最后一个值
u.With().Window(u.Salary.LastValue())

// NTH_VALUE() - 窗口第N个值
u.With().Window(u.Salary.NthValue(3))
```

#### 3. 聚合函数

```go
// SUM() 作为窗口函数
u.With().Window(u.Salary.WindowSum())

// AVG() 作为窗口函数
u.With().Window(u.Salary.WindowAvg())

// COUNT() 作为窗口函数
u.With().Window(u.Salary.WindowCount())

// MAX() 作为窗口函数
u.With().Window(u.Salary.WindowMax())

// MIN() 作为窗口函数
u.With().Window(u.Salary.WindowMin())
```

## 窗口规范

### PARTITION BY

```go
// 单个分区字段
u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department)

// 多个分区字段
u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department, u.Location)
```

### ORDER BY

```go
// 窗口内排序
u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc())

// 多字段排序
u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc(), u.HireDate.Asc())
```

### 窗口帧 (FRAME)

```go
// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
frameSpec := gen.FrameSpec{
    Type:  gen.FrameRows,
    Start: gen.FrameBound{Type: gen.UnboundedPreceding},
    End:   &gen.FrameBound{Type: gen.CurrentRow},
}

u.With().
    Window(u.Salary.WindowSum()).
    PartitionBy(u.Department).
    WindowOrderBy(u.HireDate).
    Frame(frameSpec)

// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
frameSpec := gen.FrameSpec{
    Type:  gen.FrameRows,
    Start: gen.FrameBound{Type: gen.Preceding, Offset: 1},
    End:   &gen.FrameBound{Type: gen.Following, Offset: 1},
}
```

#### 窗口帧类型

- `FrameRows`: ROWS 帧类型
- `FrameRange`: RANGE 帧类型

#### 窗口帧边界

- `UnboundedPreceding`: UNBOUNDED PRECEDING
- `Preceding`: N PRECEDING (需要指定 Offset)
- `CurrentRow`: CURRENT ROW
- `Following`: N FOLLOWING (需要指定 Offset)
- `UnboundedFollowing`: UNBOUNDED FOLLOWING

## 复杂示例

### 1. 部门内薪资排名

```go
result := u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc()).
    Select(
        u.Name,
        u.Department,
        u.Salary,
        field.RowNumberFunc().As("salary_rank"),
    ).
    Find()

// SQL:
// SELECT name, department, salary, 
//        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank
// FROM users
```

### 2. 累计薪资总和

```go
frameSpec := gen.FrameSpec{
    Type:  gen.FrameRows,
    Start: gen.FrameBound{Type: gen.UnboundedPreceding},
    End:   &gen.FrameBound{Type: gen.CurrentRow},
}

result := u.With().
    Window(u.Salary.WindowSum()).
    PartitionBy(u.Department).
    WindowOrderBy(u.HireDate).
    Frame(frameSpec).
    Select(
        u.Name,
        u.Department,
        u.Salary,
        u.HireDate,
        u.Salary.WindowSum().As("running_total"),
    ).
    Find()

// SQL:
// SELECT name, department, salary, hire_date,
//        SUM(salary) OVER (PARTITION BY department ORDER BY hire_date 
//                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
// FROM users
```

### 3. 薪资变化分析

```go
result := u.With().
    Window(u.Salary.Lag(1)).
    PartitionBy(u.EmployeeID).
    WindowOrderBy(u.ReviewDate).
    Select(
        u.EmployeeID,
        u.ReviewDate,
        u.Salary,
        u.Salary.Lag(1).As("prev_salary"),
        field.NewExpr(clause.Expr{
            SQL: "? - ?",
            Vars: []interface{}{u.Salary.RawExpr(), u.Salary.Lag(1).RawExpr()},
        }).As("salary_change"),
    ).
    Find()

// SQL:
// SELECT employee_id, review_date, salary,
//        LAG(salary, 1) OVER (PARTITION BY employee_id ORDER BY review_date) as prev_salary,
//        salary - LAG(salary, 1) OVER (PARTITION BY employee_id ORDER BY review_date) as salary_change
// FROM users
```

### 4. 多个窗口函数组合

```go
result := u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc()).
    Window(u.Salary.PercentRank()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary).
    Window(u.Salary.Ntile(4)).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary).
    Select(
        u.Name,
        u.Department,
        u.Salary,
        field.RowNumberFunc().As("rank"),
        u.Salary.PercentRank().As("percent_rank"),
        u.Salary.Ntile(4).As("quartile"),
    ).
    Find()
```

## 与现有功能集成

### 与 WHERE 条件结合

```go
result := u.With().
    Where(u.Department.Eq("Engineering")).
    Window(field.RowNumberFunc()).
    WindowOrderBy(u.Salary.Desc()).
    Select(u.ALL, field.RowNumberFunc().As("rank")).
    Find()
```

### 与 JOIN 结合

```go
result := u.With().
    Join(department, department.ID.EqCol(u.DepartmentID)).
    Window(field.RowNumberFunc()).
    PartitionBy(u.DepartmentID).
    WindowOrderBy(u.Salary.Desc()).
    Select(
        u.Name,
        department.Name.As("dept_name"),
        u.Salary,
        field.RowNumberFunc().As("rank"),
    ).
    Find()
```

### 子查询中使用

```go
// 获取每个部门薪资前3名
subQuery := u.With().
    Window(field.RowNumberFunc()).
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc()).
    Select(u.ALL, field.RowNumberFunc().As("rank")).
    As("ranked_users")

result := gen.Table(subQuery).
    Where(field.NewField("", "rank").Lte(3)).
    Find()
```

## 便捷方法

你可以为常用的窗口函数模式创建便捷方法：

```go
// 扩展生成的查询对象
func (u *userQuery) WithRowNumber() gen.WindowView {
    return u.With().Window(field.RowNumberFunc())
}

func (u *userQuery) WithRank() gen.WindowView {
    return u.With().Window(field.RankFunc())
}

func (u *userQuery) WithRunningSum(column field.Expr) gen.WindowView {
    frameSpec := gen.FrameSpec{
        Type:  gen.FrameRows,
        Start: gen.FrameBound{Type: gen.UnboundedPreceding},
        End:   &gen.FrameBound{Type: gen.CurrentRow},
    }
    
    return u.With().
        Window(column.WindowSum()).
        Frame(frameSpec)
}

// 使用便捷方法
result := u.WithRowNumber().
    PartitionBy(u.Department).
    WindowOrderBy(u.Salary.Desc()).
    Select(u.ALL, field.RowNumberFunc().As("rank")).
    Find()
```

## 性能考虑

1. **索引优化**: 确保 PARTITION BY 和 ORDER BY 的字段有适当的索引
2. **窗口帧**: 合理使用窗口帧可以提高性能
3. **分区大小**: 避免过大的分区，可能导致内存问题

## 注意事项

1. 窗口函数只能在 SELECT 子句中使用
2. 窗口函数不能在 WHERE 子句中使用（可以在子查询中使用）
3. 确保使用的数据库支持相应的窗口函数
4. 窗口函数的执行顺序在 GROUP BY 和 HAVING 之后

## API 参考

### 接口

- `WindowView`: 窗口函数视图接口
- `WindowFunc`: 窗口函数接口
- `WindowSpec`: 窗口规范结构
- `FrameSpec`: 窗口帧规范结构

### 主要方法

- `With()`: 创建窗口视图
- `Window()`: 添加窗口函数
- `PartitionBy()`: 设置分区字段
- `WindowOrderBy()`: 设置窗口内排序
- `Frame()`: 设置窗口帧
- `Over()`: 直接设置窗口规范

这个扩展完全兼容现有的 GORM Gen API，可以无缝集成到你的项目中。 