package gen

import (
	"gorm.io/gen/field"
	"gorm.io/gorm/clause"
)

// WindowView 窗口函数视图接口，扩展现有的Dao接口
type WindowView interface {
	Dao

	// Window 定义窗口函数
	Window(windowFunc WindowFunc) WindowView

	// Over 定义窗口规范
	Over(spec WindowSpec) WindowView

	// PartitionBy 分区子句
	PartitionBy(columns ...field.Expr) WindowView

	// WindowOrderBy 窗口内排序（区别于查询结果排序）
	WindowOrderBy(columns ...field.Expr) WindowView

	// Frame 定义窗口帧
	Frame(frameSpec FrameSpec) WindowView
}

// WindowFunc 窗口函数接口
type WindowFunc interface {
	field.Expr
	WindowFuncName() string
}

// WindowSpec 窗口规范
type WindowSpec struct {
	PartitionBy []field.Expr
	OrderBy     []field.Expr
	Frame       *FrameSpec
}

// FrameSpec 窗口帧规范
type FrameSpec struct {
	Type  FrameType
	Start FrameBound
	End   *FrameBound
}

// FrameType 窗口帧类型
type FrameType string

const (
	FrameRows  FrameType = "ROWS"
	FrameRange FrameType = "RANGE"
)

// FrameBound 窗口帧边界
type FrameBound struct {
	Type   FrameBoundType
	Offset interface{} // 用于 PRECEDING/FOLLOWING 的偏移量
}

// FrameBoundType 窗口帧边界类型
type FrameBoundType string

const (
	UnboundedPreceding FrameBoundType = "UNBOUNDED PRECEDING"
	Preceding          FrameBoundType = "PRECEDING"
	CurrentRow         FrameBoundType = "CURRENT ROW"
	Following          FrameBoundType = "FOLLOWING"
	UnboundedFollowing FrameBoundType = "UNBOUNDED FOLLOWING"
)

// windowViewDO 实现WindowView接口的具体结构
type windowViewDO struct {
	*DO
	windowFuncs []WindowFunc
	windowSpecs []WindowSpec
}

// With 创建支持窗口函数的视图对象
func (d *DO) With() WindowView {
	return &windowViewDO{
		DO:          d,
		windowFuncs: make([]WindowFunc, 0),
		windowSpecs: make([]WindowSpec, 0),
	}
}

// Window 添加窗口函数
func (w *windowViewDO) Window(windowFunc WindowFunc) WindowView {
	w.windowFuncs = append(w.windowFuncs, windowFunc)
	return w
}

// Over 定义窗口规范
func (w *windowViewDO) Over(spec WindowSpec) WindowView {
	w.windowSpecs = append(w.windowSpecs, spec)
	return w
}

// PartitionBy 分区子句
func (w *windowViewDO) PartitionBy(columns ...field.Expr) WindowView {
	if len(w.windowSpecs) == 0 {
		w.windowSpecs = append(w.windowSpecs, WindowSpec{})
	}
	lastSpec := &w.windowSpecs[len(w.windowSpecs)-1]
	lastSpec.PartitionBy = columns
	return w
}

// WindowOrderBy 窗口内排序
func (w *windowViewDO) WindowOrderBy(columns ...field.Expr) WindowView {
	if len(w.windowSpecs) == 0 {
		w.windowSpecs = append(w.windowSpecs, WindowSpec{})
	}
	lastSpec := &w.windowSpecs[len(w.windowSpecs)-1]
	lastSpec.OrderBy = columns
	return w
}

// Frame 定义窗口帧
func (w *windowViewDO) Frame(frameSpec FrameSpec) WindowView {
	if len(w.windowSpecs) == 0 {
		w.windowSpecs = append(w.windowSpecs, WindowSpec{})
	}
	lastSpec := &w.windowSpecs[len(w.windowSpecs)-1]
	lastSpec.Frame = &frameSpec
	return w
}

// Select 重写Select方法以支持窗口函数
func (w *windowViewDO) Select(columns ...field.Expr) Dao {
	// 合并普通字段和窗口函数
	allColumns := make([]field.Expr, 0, len(columns)+len(w.windowFuncs))
	allColumns = append(allColumns, columns...)

	// 为每个窗口函数构建完整的窗口表达式
	for i, windowFunc := range w.windowFuncs {
		var spec WindowSpec
		if i < len(w.windowSpecs) {
			spec = w.windowSpecs[i]
		}

		windowExpr := buildWindowExpression(windowFunc, spec)
		allColumns = append(allColumns, windowExpr)
	}

	return w.DO.Select(allColumns...)
}

// buildWindowExpression 构建窗口函数表达式
func buildWindowExpression(windowFunc WindowFunc, spec WindowSpec) field.Expr {
	// 构建 OVER 子句
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
		overClause += buildFrameClause(*spec.Frame)
	}

	overClause += ")"

	// 构建完整的窗口函数表达式
	sql := windowFunc.WindowFuncName() + overClause
	allVars := append([]interface{}{windowFunc.RawExpr()}, vars...)

	return field.NewExpr(clause.Expr{
		SQL:  sql,
		Vars: allVars,
	})
}

// buildFrameClause 构建窗口帧子句
func buildFrameClause(frame FrameSpec) string {
	result := string(frame.Start.Type)
	if frame.Start.Offset != nil {
		result = "? " + result
	}

	if frame.End != nil {
		result += " AND " + string(frame.End.Type)
		if frame.End.Offset != nil {
			result = result[:len(result)-len(string(frame.End.Type))] + "? " + string(frame.End.Type)
		}
	}

	return result
}
