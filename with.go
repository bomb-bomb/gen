package gen

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gorm.io/gen/field"
)

// WithClause represents a WITH clause (Common Table Expression)
type WithClause struct {
	Name  string
	Query SubQuery
}

// WithQuery represents a query that can use WITH clauses
type WithQuery struct {
	*DO
	withClauses []WithClause
}

// With creates a new WithQuery with the specified CTE
func (d *DO) With(name string, query SubQuery) *WithQuery {
	return &WithQuery{
		DO:          d,
		withClauses: []WithClause{{Name: name, Query: query}},
	}
}

// With adds another CTE to the existing WithQuery
func (w *WithQuery) With(name string, query SubQuery) *WithQuery {
	w.withClauses = append(w.withClauses, WithClause{Name: name, Query: query})
	return w
}

// Select executes the final query with all WITH clauses
func (w *WithQuery) Select(columns ...field.Expr) Dao {
	// Build the WITH clause SQL
	var withParts []string
	var allArgs []interface{}
	
	for _, withClause := range w.withClauses {
		subDB := withClause.Query.underlyingDB()
		sql := subDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Find(&struct{}{})
		})
		
		// Extract SQL and args from the subquery
		stmt := subDB.Statement
		if stmt != nil {
			sql = stmt.SQL.String()
			allArgs = append(allArgs, stmt.Vars...)
		}
		
		withParts = append(withParts, fmt.Sprintf("%s AS (%s)", withClause.Name, sql))
	}
	
	withSQL := "WITH " + strings.Join(withParts, ", ")
	
	// Create a new DO instance with the WITH clause
	newDB := w.DO.db.Session(&gorm.Session{})
	
	// Add the WITH clause as a raw SQL prefix
	if len(columns) > 0 {
		selectSQL, selectArgs := buildExpr4Select(newDB.Statement, columns...)
		finalSQL := fmt.Sprintf("%s SELECT %s", withSQL, selectSQL)
		allArgs = append(allArgs, selectArgs...)
		newDB = newDB.Raw(finalSQL, allArgs...)
	} else {
		finalSQL := fmt.Sprintf("%s SELECT *", withSQL)
		newDB = newDB.Raw(finalSQL, allArgs...)
	}
	
	return w.DO.getInstance(newDB)
}

// From specifies which CTE to select from
func (w *WithQuery) From(cteName string) Dao {
	// Build the WITH clause SQL
	var withParts []string
	var allArgs []interface{}
	
	for _, withClause := range w.withClauses {
		subDB := withClause.Query.underlyingDB()
		sql := subDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Find(&struct{}{})
		})
		
		// Extract SQL and args from the subquery
		stmt := subDB.Statement
		if stmt != nil {
			sql = stmt.SQL.String()
			allArgs = append(allArgs, stmt.Vars...)
		}
		
		withParts = append(withParts, fmt.Sprintf("%s AS (%s)", withClause.Name, sql))
	}
	
	withSQL := "WITH " + strings.Join(withParts, ", ")
	
	// Create a new DO instance that selects from the specified CTE
	newDB := w.DO.db.Session(&gorm.Session{})
	newDB = newDB.Table(cteName)
	
	// Add the WITH clause using a custom clause
	newDB = newDB.Clauses(&WithClauseExpr{SQL: withSQL, Args: allArgs})
	
	return w.DO.getInstance(newDB)
}

// WithClauseExpr implements clause.Expression for WITH clauses
type WithClauseExpr struct {
	SQL  string
	Args []interface{}
}

// Build implements clause.Expression
func (w *WithClauseExpr) Build(builder clause.Builder) {
	builder.WriteString(w.SQL)
	builder.AddVar(builder, w.Args...)
}

// WindowFunction represents a window function expression
type WindowFunction struct {
	Function string
	overClause *OverClause
}

// OverClause represents the OVER clause in window functions
type OverClause struct {
	partitionBy []field.Expr
	orderBy     []field.Expr
	frame       *FrameClause
}

// FrameClause represents the frame specification in window functions
type FrameClause struct {
	Type  string // ROWS, RANGE, GROUPS
	Start string // UNBOUNDED PRECEDING, CURRENT ROW, etc.
	End   string // UNBOUNDED FOLLOWING, CURRENT ROW, etc.
}

// RowNumber creates a ROW_NUMBER() window function
func RowNumber() *WindowFunction {
	return &WindowFunction{Function: "ROW_NUMBER()"}
}

// Rank creates a RANK() window function
func Rank() *WindowFunction {
	return &WindowFunction{Function: "RANK()"}
}

// DenseRank creates a DENSE_RANK() window function
func DenseRank() *WindowFunction {
	return &WindowFunction{Function: "DENSE_RANK()"}
}

// Count creates a COUNT() window function
func Count(expr field.Expr) *WindowFunction {
	var exprStr string
	if columnName, ok := expr.(field.IColumnName); ok {
		exprStr = string(columnName.ColumnName())
	} else {
		exprStr = fmt.Sprintf("%s", expr.RawExpr())
	}
	return &WindowFunction{Function: fmt.Sprintf("COUNT(%s)", exprStr)}
}

// Sum creates a SUM() window function
func Sum(expr field.Expr) *WindowFunction {
	var exprStr string
	if columnName, ok := expr.(field.IColumnName); ok {
		exprStr = string(columnName.ColumnName())
	} else {
		exprStr = fmt.Sprintf("%s", expr.RawExpr())
	}
	return &WindowFunction{Function: fmt.Sprintf("SUM(%s)", exprStr)}
}

// Avg creates an AVG() window function
func Avg(expr field.Expr) *WindowFunction {
	var exprStr string
	if columnName, ok := expr.(field.IColumnName); ok {
		exprStr = string(columnName.ColumnName())
	} else {
		exprStr = fmt.Sprintf("%s", expr.RawExpr())
	}
	return &WindowFunction{Function: fmt.Sprintf("AVG(%s)", exprStr)}
}

// Max creates a MAX() window function
func Max(expr field.Expr) *WindowFunction {
	var exprStr string
	if columnName, ok := expr.(field.IColumnName); ok {
		exprStr = string(columnName.ColumnName())
	} else {
		exprStr = fmt.Sprintf("%s", expr.RawExpr())
	}
	return &WindowFunction{Function: fmt.Sprintf("MAX(%s)", exprStr)}
}

// Min creates a MIN() window function
func Min(expr field.Expr) *WindowFunction {
	var exprStr string
	if columnName, ok := expr.(field.IColumnName); ok {
		exprStr = string(columnName.ColumnName())
	} else {
		exprStr = fmt.Sprintf("%s", expr.RawExpr())
	}
	return &WindowFunction{Function: fmt.Sprintf("MIN(%s)", exprStr)}
}

// Over specifies the OVER clause for the window function
func (w *WindowFunction) Over() *OverClause {
	if w.overClause == nil {
		w.overClause = &OverClause{}
	}
	return w.overClause
}

// PartitionBy specifies the PARTITION BY clause
func (o *OverClause) PartitionBy(exprs ...field.Expr) *OverClause {
	o.partitionBy = exprs
	return o
}

// OrderBy specifies the ORDER BY clause
func (o *OverClause) OrderBy(exprs ...field.Expr) *OverClause {
	o.orderBy = exprs
	return o
}

// Rows specifies a ROWS frame
func (o *OverClause) Rows(start, end string) *OverClause {
	o.frame = &FrameClause{Type: "ROWS", Start: start, End: end}
	return o
}

// Range specifies a RANGE frame
func (o *OverClause) Range(start, end string) *OverClause {
	o.frame = &FrameClause{Type: "RANGE", Start: start, End: end}
	return o
}

// As creates a field expression with alias for the window function
func (w *WindowFunction) As(alias string) field.Expr {
	sql := w.buildSQL()
	return field.NewExpr(alias, clause.Expr{SQL: sql})
}

// buildSQL builds the complete window function SQL
func (w *WindowFunction) buildSQL() string {
	sql := w.Function + " OVER ("
	
	if w.overClause != nil {
		var parts []string
		
		if len(w.overClause.partitionBy) > 0 {
			var partitions []string
			for _, expr := range w.overClause.partitionBy {
				if columnName, ok := expr.(field.IColumnName); ok {
					partitions = append(partitions, string(columnName.ColumnName()))
				} else {
					partitions = append(partitions, fmt.Sprintf("%s", expr.RawExpr()))
				}
			}
			parts = append(parts, "PARTITION BY "+strings.Join(partitions, ", "))
		}
		
		if len(w.overClause.orderBy) > 0 {
			var orders []string
			for _, expr := range w.overClause.orderBy {
				if columnName, ok := expr.(field.IColumnName); ok {
					orders = append(orders, string(columnName.ColumnName()))
				} else {
					orders = append(orders, fmt.Sprintf("%s", expr.RawExpr()))
				}
			}
			parts = append(parts, "ORDER BY "+strings.Join(orders, ", "))
		}
		
		if w.overClause.frame != nil {
			frameSQL := w.overClause.frame.Type
			if w.overClause.frame.End != "" {
				frameSQL += fmt.Sprintf(" BETWEEN %s AND %s", w.overClause.frame.Start, w.overClause.frame.End)
			} else {
				frameSQL += " " + w.overClause.frame.Start
			}
			parts = append(parts, frameSQL)
		}
		
		sql += strings.Join(parts, " ")
	}
	
	sql += ")"
	return sql
} 