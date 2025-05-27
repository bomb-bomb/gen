package gen

import (
	"fmt"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CTE Common Table Expression 接口
type CTE interface {
	// CTEName 返回CTE的名称
	CTEName() string
	// CTEQuery 返回CTE的查询
	CTEQuery() SubQuery
	// Build 构建CTE的SQL
	Build(builder clause.Builder)
}

// CTEView CTE视图接口，支持窗口函数
type CTEView interface {
	Dao

	// WithCTE 添加CTE定义
	WithCTE(name string, query SubQuery) CTEView

	// Window 定义窗口函数
	Window(windowFunc WindowFunc) CTEView

	// PartitionBy 分区子句
	PartitionBy(columns ...field.Expr) CTEView

	// WindowOrderBy 窗口内排序
	WindowOrderBy(columns ...field.Expr) CTEView

	// Frame 定义窗口帧
	Frame(frameSpec FrameSpec) CTEView

	// FromCTE 从CTE中查询
	FromCTE(cteName string) CTEView
}

// cteImpl CTE的具体实现
type cteImpl struct {
	name  string
	query SubQuery
}

func (c *cteImpl) CTEName() string {
	return c.name
}

func (c *cteImpl) CTEQuery() SubQuery {
	return c.query
}

func (c *cteImpl) Build(builder clause.Builder) {
	builder.WriteString(c.name)
	builder.WriteString(" AS (")
	// 这里需要构建子查询的SQL
	if c.query != nil {
		// 获取子查询的SQL
		sql := c.query.underlyingDB().ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Find(nil)
		})
		builder.WriteString(sql)
	}
	builder.WriteString(")")
}

// cteViewDO CTE视图的具体实现
type cteViewDO struct {
	*DO
	ctes        []CTE
	windowFuncs []WindowFunc
	windowSpecs []WindowSpec
	fromCTE     string
}

// WithCTE 创建支持CTE的查询构建器
func WithCTE(name string, query SubQuery) CTEView {
	// 创建一个新的DO实例
	newDO := &DO{
		db: query.underlyingDB().Session(&gorm.Session{NewDB: true}),
	}

	cte := &cteImpl{
		name:  name,
		query: query,
	}

	return &cteViewDO{
		DO:          newDO,
		ctes:        []CTE{cte},
		windowFuncs: make([]WindowFunc, 0),
		windowSpecs: make([]WindowSpec, 0),
	}
}

// WithCTE 添加额外的CTE
func (c *cteViewDO) WithCTE(name string, query SubQuery) CTEView {
	cte := &cteImpl{
		name:  name,
		query: query,
	}
	c.ctes = append(c.ctes, cte)
	return c
}

// FromCTE 从指定的CTE中查询
func (c *cteViewDO) FromCTE(cteName string) CTEView {
	c.fromCTE = cteName
	// 设置表名为CTE名称
	c.db = c.db.Table(cteName)
	return c
}

// Window 添加窗口函数
func (c *cteViewDO) Window(windowFunc WindowFunc) CTEView {
	c.windowFuncs = append(c.windowFuncs, windowFunc)
	return c
}

// PartitionBy 分区子句
func (c *cteViewDO) PartitionBy(columns ...field.Expr) CTEView {
	if len(c.windowSpecs) == 0 {
		c.windowSpecs = append(c.windowSpecs, WindowSpec{})
	}
	lastSpec := &c.windowSpecs[len(c.windowSpecs)-1]
	lastSpec.PartitionBy = columns
	return c
}

// WindowOrderBy 窗口内排序
func (c *cteViewDO) WindowOrderBy(columns ...field.Expr) CTEView {
	if len(c.windowSpecs) == 0 {
		c.windowSpecs = append(c.windowSpecs, WindowSpec{})
	}
	lastSpec := &c.windowSpecs[len(c.windowSpecs)-1]
	lastSpec.OrderBy = columns
	return c
}

// Frame 定义窗口帧
func (c *cteViewDO) Frame(frameSpec FrameSpec) CTEView {
	if len(c.windowSpecs) == 0 {
		c.windowSpecs = append(c.windowSpecs, WindowSpec{})
	}
	lastSpec := &c.windowSpecs[len(c.windowSpecs)-1]
	lastSpec.Frame = &frameSpec
	return c
}

// Select 重写Select方法以支持CTE和窗口函数
func (c *cteViewDO) Select(columns ...field.Expr) Dao {
	// 如果有窗口函数，需要构建窗口表达式
	allColumns := make([]field.Expr, 0, len(columns)+len(c.windowFuncs))
	allColumns = append(allColumns, columns...)

	// 为每个窗口函数构建完整的窗口表达式
	for i, windowFunc := range c.windowFuncs {
		var spec WindowSpec
		if i < len(c.windowSpecs) {
			spec = c.windowSpecs[i]
		}

		windowExpr := buildWindowExpression(windowFunc, spec)
		allColumns = append(allColumns, windowExpr)
	}

	// 构建带CTE的查询
	if len(c.ctes) > 0 {
		c.db = c.buildCTEQuery(c.db)
	}

	return c.DO.Select(allColumns...)
}

// buildCTEQuery 构建包含CTE的查询
func (c *cteViewDO) buildCTEQuery(db *gorm.DB) *gorm.DB {
	if len(c.ctes) == 0 {
		return db
	}

	// 构建WITH子句
	var cteSQL string
	var cteVars []interface{}

	cteSQL = "WITH "
	for i, cte := range c.ctes {
		if i > 0 {
			cteSQL += ", "
		}

		// 获取CTE查询的SQL
		query := cte.CTEQuery()
		sql := query.underlyingDB().ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Find(nil)
		})

		cteSQL += fmt.Sprintf("%s AS (%s)", cte.CTEName(), sql)
	}

	// 使用Raw查询来包含CTE
	return db.Raw(cteSQL+" SELECT * FROM "+c.fromCTE, cteVars...)
}

// Find 添加便捷方法
func (c *cteViewDO) Find() (interface{}, error) {
	if len(c.ctes) > 0 {
		c.db = c.buildCTEQuery(c.db)
	}
	return c.DO.Find()
}

// WithWindowCTE 扩展DO以支持CTE
func (d *DO) WithWindowCTE(name string) *CTEWindowBuilder {
	return &CTEWindowBuilder{
		do:   d,
		name: name,
	}
}

// CTEWindowBuilder CTE窗口函数构建器
type CTEWindowBuilder struct {
	do          *DO
	name        string
	windowFuncs []WindowFunc
	windowSpecs []WindowSpec
}

// Window 添加窗口函数
func (b *CTEWindowBuilder) Window(windowFunc WindowFunc) *CTEWindowBuilder {
	b.windowFuncs = append(b.windowFuncs, windowFunc)
	return b
}

// PartitionBy 分区子句
func (b *CTEWindowBuilder) PartitionBy(columns ...field.Expr) *CTEWindowBuilder {
	if len(b.windowSpecs) == 0 {
		b.windowSpecs = append(b.windowSpecs, WindowSpec{})
	}
	lastSpec := &b.windowSpecs[len(b.windowSpecs)-1]
	lastSpec.PartitionBy = columns
	return b
}

// WindowOrderBy 窗口内排序
func (b *CTEWindowBuilder) WindowOrderBy(columns ...field.Expr) *CTEWindowBuilder {
	if len(b.windowSpecs) == 0 {
		b.windowSpecs = append(b.windowSpecs, WindowSpec{})
	}
	lastSpec := &b.windowSpecs[len(b.windowSpecs)-1]
	lastSpec.OrderBy = columns
	return b
}

// As 完成CTE定义并返回CTEView
func (b *CTEWindowBuilder) As() CTEView {
	// 构建包含窗口函数的查询
	var allColumns []field.Expr

	// 添加所有字段
	allColumns = append(allColumns, field.NewAsterisk(""))

	// 为每个窗口函数构建完整的窗口表达式
	for i, windowFunc := range b.windowFuncs {
		var spec WindowSpec
		if i < len(b.windowSpecs) {
			spec = b.windowSpecs[i]
		}

		windowExpr := buildWindowExpression(windowFunc, spec)
		allColumns = append(allColumns, windowExpr)
	}

	// 创建子查询
	subQuery := b.do.Select(allColumns...)

	return WithCTE(b.name, subQuery)
}
