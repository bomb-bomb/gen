package field

import (
	"gorm.io/gorm/clause"
)

// WindowFunction 窗口函数的基础结构
type WindowFunction struct {
	expr
	funcName string
}

// WindowFuncName 实现WindowFunc接口
func (w WindowFunction) WindowFuncName() string {
	return w.funcName
}

// NewExpr 创建表达式（需要在field包中添加这个函数）
func NewExpr(expression clause.Expression) Expr {
	return expr{e: expression}
}

// ==================== 排名窗口函数 ====================

// RowNumber ROW_NUMBER() 窗口函数
func (e expr) RowNumber() WindowFunction {
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: "ROW_NUMBER()"}),
		funcName: "ROW_NUMBER()",
	}
}

// Rank RANK() 窗口函数
func (e expr) Rank() WindowFunction {
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: "RANK()"}),
		funcName: "RANK()",
	}
}

// DenseRank DENSE_RANK() 窗口函数
func (e expr) DenseRank() WindowFunction {
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: "DENSE_RANK()"}),
		funcName: "DENSE_RANK()",
	}
}

// PercentRank PERCENT_RANK() 窗口函数
func (e expr) PercentRank() WindowFunction {
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: "PERCENT_RANK()"}),
		funcName: "PERCENT_RANK()",
	}
}

// CumeDist CUME_DIST() 窗口函数
func (e expr) CumeDist() WindowFunction {
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: "CUME_DIST()"}),
		funcName: "CUME_DIST()",
	}
}

// ==================== 值窗口函数 ====================

// Lag LAG(column, offset, default) 窗口函数
func (e expr) Lag(offset int, defaultValue ...interface{}) WindowFunction {
	var sql string
	var vars []interface{}
	
	if len(defaultValue) > 0 {
		sql = "LAG(?, ?, ?)"
		vars = []interface{}{e.RawExpr(), offset, defaultValue[0]}
	} else {
		sql = "LAG(?, ?)"
		vars = []interface{}{e.RawExpr(), offset}
	}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// Lead LEAD(column, offset, default) 窗口函数
func (e expr) Lead(offset int, defaultValue ...interface{}) WindowFunction {
	var sql string
	var vars []interface{}
	
	if len(defaultValue) > 0 {
		sql = "LEAD(?, ?, ?)"
		vars = []interface{}{e.RawExpr(), offset, defaultValue[0]}
	} else {
		sql = "LEAD(?, ?)"
		vars = []interface{}{e.RawExpr(), offset}
	}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// FirstValue FIRST_VALUE(column) 窗口函数
func (e expr) FirstValue() WindowFunction {
	sql := "FIRST_VALUE(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// LastValue LAST_VALUE(column) 窗口函数
func (e expr) LastValue() WindowFunction {
	sql := "LAST_VALUE(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// NthValue NTH_VALUE(column, n) 窗口函数
func (e expr) NthValue(n int) WindowFunction {
	sql := "NTH_VALUE(?, ?)"
	vars := []interface{}{e.RawExpr(), n}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// ==================== 聚合窗口函数 ====================

// WindowSum SUM() 作为窗口函数
func (e expr) WindowSum() WindowFunction {
	sql := "SUM(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// WindowAvg AVG() 作为窗口函数
func (e expr) WindowAvg() WindowFunction {
	sql := "AVG(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// WindowCount COUNT() 作为窗口函数
func (e expr) WindowCount() WindowFunction {
	sql := "COUNT(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// WindowMax MAX() 作为窗口函数
func (e expr) WindowMax() WindowFunction {
	sql := "MAX(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// WindowMin MIN() 作为窗口函数
func (e expr) WindowMin() WindowFunction {
	sql := "MIN(?)"
	vars := []interface{}{e.RawExpr()}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// ==================== 分布函数 ====================

// Ntile NTILE(n) 窗口函数
func (e expr) Ntile(n int) WindowFunction {
	sql := "NTILE(?)"
	vars := []interface{}{n}
	
	return WindowFunction{
		expr:     e.setE(clause.Expr{SQL: sql, Vars: vars}),
		funcName: sql,
	}
}

// ==================== 便捷构造函数 ====================

// RowNumberFunc 创建ROW_NUMBER()窗口函数
func RowNumberFunc() WindowFunction {
	return WindowFunction{
		expr:     expr{e: clause.Expr{SQL: "ROW_NUMBER()"}},
		funcName: "ROW_NUMBER()",
	}
}

// RankFunc 创建RANK()窗口函数
func RankFunc() WindowFunction {
	return WindowFunction{
		expr:     expr{e: clause.Expr{SQL: "RANK()"}},
		funcName: "RANK()",
	}
}

// DenseRankFunc 创建DENSE_RANK()窗口函数
func DenseRankFunc() WindowFunction {
	return WindowFunction{
		expr:     expr{e: clause.Expr{SQL: "DENSE_RANK()"}},
		funcName: "DENSE_RANK()",
	}
}

// Over 为窗口函数指定窗口规范
func (w WindowFunction) Over(spec WindowSpec) WindowFunction {
	// 这里需要重新构建窗口函数的SQL
	var overClause string
	var vars []interface{}
	
	overClause = " OVER ("
	
	// PARTITION BY
	if len(spec.PartitionBy) > 0 {
		overClause += "PARTITION BY "
		for i, col := range spec.PartitionBy {
			if i > 0 {
				overClause += ", "
			}
			overClause += "?"
			vars = append(vars, col.RawExpr())
		}
	}
	
	// ORDER BY
	if len(spec.OrderBy) > 0 {
		if len(spec.PartitionBy) > 0 {
			overClause += " "
		}
		overClause += "ORDER BY "
		for i, col := range spec.OrderBy {
			if i > 0 {
				overClause += ", "
			}
			overClause += "?"
			vars = append(vars, col.RawExpr())
		}
	}
	
	// FRAME
	if spec.Frame != nil {
		overClause += " " + string(spec.Frame.Type) + " "
		// 这里需要构建frame子句，暂时简化
		if spec.Frame.Start.Type == UnboundedPreceding && spec.Frame.End != nil && spec.Frame.End.Type == CurrentRow {
			overClause += "UNBOUNDED PRECEDING AND CURRENT ROW"
		}
	}
	
	overClause += ")"
	
	// 构建完整的窗口函数表达式
	sql := w.funcName + overClause
	allVars := append([]interface{}{w.RawExpr()}, vars...)
	
	return WindowFunction{
		expr:     w.setE(clause.Expr{SQL: sql, Vars: allVars}),
		funcName: sql,
	}
}

// PercentRankFunc 创建PERCENT_RANK()窗口函数
func PercentRankFunc() WindowFunction {
	return WindowFunction{
		expr:     expr{e: clause.Expr{SQL: "PERCENT_RANK()"}},
		funcName: "PERCENT_RANK()",
	}
} 