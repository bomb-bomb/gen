package field

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var _ Expr = new(Field)

// AssignExpr assign expression
type AssignExpr interface {
	Expr

	AssignExpr() expression
}

// Expr a query expression about field
type Expr interface {
	// Clause Expression interface
	Build(clause.Builder)

	As(alias string) Expr
	IColumnName
	BuildColumn(*gorm.Statement, ...BuildOpt) sql
	BuildWithArgs(*gorm.Statement) (query sql, args []interface{})
	RawExpr() expression

	// col operate expression
	AddCol(col Expr) Expr
	SubCol(col Expr) Expr
	MulCol(col Expr) Expr
	DivCol(col Expr) Expr
	ConcatCol(cols ...Expr) Expr

	// implement Condition
	BeCond() interface{}
	CondError() error

	expression() clause.Expression
}

// OrderExpr order expression
// used in Order()
type OrderExpr interface {
	Expr
	Desc() Expr
	Asc() Expr
}

type expression interface{}

type IColumnName interface {
	ColumnName() sql
}

type sql string

func (e sql) String() string { return string(e) }

type expr struct {
	col clause.Column

	e         clause.Expression
	buildOpts []BuildOpt
}

func (e expr) BeCond() interface{} { return e.expression() }
func (expr) CondError() error      { return nil }

func (e expr) AssignExpr() expression {
	return e.expression()
}

func (e expr) expression() clause.Expression {
	if e.e == nil {
		return clause.NamedExpr{SQL: "?", Vars: []interface{}{e.col}}
	}
	return e.e
}

func (e expr) ColumnName() sql { return sql(e.col.Name) }

// BuildOpt build option
type BuildOpt uint

const (
	// WithTable build column with table
	WithTable BuildOpt = iota

	// WithAll build column with table and alias
	WithAll

	// WithoutQuote build column without quote
	WithoutQuote
)

func (e expr) BuildColumn(stmt *gorm.Statement, opts ...BuildOpt) sql {
	col := clause.Column{Name: e.col.Name}
	for _, opt := range append(e.buildOpts, opts...) {
		switch opt {
		case WithTable:
			col.Table = e.col.Table
		case WithAll:
			col.Table = e.col.Table
			col.Alias = e.col.Alias
		case WithoutQuote:
			col.Raw = true
		}
	}
	if col.Name == "*" {
		if col.Table != "" {
			return sql(stmt.Quote(col.Table)) + ".*"
		}
		return "*"
	}
	return sql(stmt.Quote(col))
}

func (e expr) Build(builder clause.Builder) {
	if e.e == nil {
		if stmt, ok := builder.(*gorm.Statement); ok {
			builder.WriteString(string(e.BuildColumn(stmt, WithAll)))
			return
		}
	}

	e.e.Build(builder)
}

func (e expr) BuildWithArgs(stmt *gorm.Statement) (sql, []interface{}) {
	if e.e == nil {
		return sql(e.BuildColumn(stmt, WithAll)), nil
	}
	newStmt := &gorm.Statement{DB: stmt.DB, Table: stmt.Table, Schema: stmt.Schema}
	e.e.Build(newStmt)
	return sql(newStmt.SQL.String()), newStmt.Vars
}

func (e expr) RawExpr() expression {
	if e.e == nil {
		return e.col
	}
	return e.e
}

func (e expr) setE(expression clause.Expression) expr {
	e.e = expression
	return e
}

func (e expr) appendBuildOpts(opts ...BuildOpt) expr {
	e.buildOpts = append(e.buildOpts, opts...)
	return e
}

// ======================== basic function ========================
func (e expr) WithTable(table string) Expr {
	e.col.Table = table
	return e
}

func (e expr) IsNull() Expr {
	return e.setE(clause.Expr{SQL: "? IS NULL", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) IsNotNull() Expr {
	return e.setE(clause.Expr{SQL: "? IS NOT NULL", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) Count() Int {
	return Int{e.setE(clause.Expr{SQL: "COUNT(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Distinct() Int {
	return Int{e.setE(clause.Expr{SQL: "DISTINCT ?", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Length() Int {
	return Int{e.setE(clause.Expr{SQL: "LENGTH(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Max() Float64 {
	return Float64{e.setE(clause.Expr{SQL: "MAX(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Min() Float64 {
	return Float64{e.setE(clause.Expr{SQL: "MIN(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Avg() Float64 {
	return Float64{e.setE(clause.Expr{SQL: "AVG(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Abs() Float64 {
	return Float64{e.setE(clause.Expr{SQL: "ABS(?)", Vars: []interface{}{e.RawExpr()}})}
}

func (e expr) Null() AssignExpr {
	return e.setE(clause.Eq{Column: e.col.Name, Value: nil})
}

func (e expr) GroupConcat() Expr {
	return e.setE(clause.Expr{SQL: "GROUP_CONCAT(?)", Vars: []interface{}{e.RawExpr()}})
}

// ======================== comparison between columns ========================
func (e expr) EqCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? = ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) NeqCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? <> ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) GtCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? > ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) GteCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? >= ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) LtCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? < ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) LteCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? <= ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) SetCol(col Expr) AssignExpr {
	return e.setE(clause.Eq{Column: e.col.Name, Value: col.RawExpr()})
}

func (e expr) ReverseIncludeTextCol(col Expr) Expr {
	return e.setE(clause.Expr{SQL: "? && ARRAY[ ? ]::text[]", Vars: []interface{}{col.RawExpr(), e.RawExpr()}})
}

// ======================== operate columns ========================
func (e expr) AddCol(col Expr) Expr {
	return Field{e.setE(clause.Expr{SQL: "? + ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})}
}

func (e expr) SubCol(col Expr) Expr {
	return Field{e.setE(clause.Expr{SQL: "? - ?", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})}
}

func (e expr) MulCol(col Expr) Expr {
	return Field{e.setE(clause.Expr{SQL: "(?) * (?)", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})}
}

func (e expr) DivCol(col Expr) Expr {
	return Field{e.setE(clause.Expr{SQL: "(?) / (?)", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})}
}

func (e expr) ConcatCol(cols ...Expr) Expr {
	placeholders := []string{"?"}
	vars := []interface{}{e.RawExpr()}
	for _, col := range cols {
		placeholders = append(placeholders, "?")
		vars = append(vars, col.RawExpr())
	}
	return Field{e.setE(clause.Expr{
		SQL:  fmt.Sprintf("CONCAT(%s)", strings.Join(placeholders, ",")),
		Vars: vars,
	})}
}

// ======================== keyword ========================
func (e expr) As(alias string) Expr {
	if e.e != nil {
		return e.setE(clause.Expr{SQL: "? AS ?", Vars: []interface{}{e.e, clause.Column{Name: alias}}})
	}
	e.col.Alias = alias
	return e
}

// Desc sort by desc
func (e expr) Desc() Expr {
	return e.setE(clause.Expr{SQL: "? DESC", Vars: []interface{}{e.RawExpr()}})
}

// Asc sort by asc
func (e expr) Asc() Expr {
	return e.setE(clause.Expr{SQL: "? ASC", Vars: []interface{}{e.RawExpr()}})
}

// ======================== general experssion ========================
func (e expr) value(value interface{}) AssignExpr {
	return e.setE(clause.Eq{Column: e.col.Name, Value: value})
}

func (e expr) between(values []interface{}) expr {
	return e.setE(clause.Expr{SQL: "? BETWEEN ? AND ?", Vars: append([]interface{}{e.RawExpr()}, values...)})
}

func (e expr) add(value interface{}) expr {
	switch v := value.(type) {
	case time.Duration:
		return e.setE(clause.Expr{SQL: "DATE_ADD(?, INTERVAL ? MICROSECOND)", Vars: []interface{}{e.RawExpr(), v.Microseconds()}})
	default:
		return e.setE(clause.Expr{SQL: "?+?", Vars: []interface{}{e.RawExpr(), value}})
	}
}

func (e expr) sub(value interface{}) expr {
	switch v := value.(type) {
	case time.Duration:
		return e.setE(clause.Expr{SQL: "DATE_SUB(?, INTERVAL ? MICROSECOND)", Vars: []interface{}{e.RawExpr(), v.Microseconds()}})
	default:
		return e.setE(clause.Expr{SQL: "?-?", Vars: []interface{}{e.RawExpr(), value}})
	}
}

func (e expr) mul(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?*?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)*?", Vars: []interface{}{e.e, value}})
}

func (e expr) div(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?/?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)/?", Vars: []interface{}{e.e, value}})
}

func (e expr) mod(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?%?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)%?", Vars: []interface{}{e.e, value}})
}

func (e expr) floorDiv(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "? DIV ?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?) DIV ?", Vars: []interface{}{e.e, value}})
}

func (e expr) floor() expr {
	return e.setE(clause.Expr{SQL: "FLOOR(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) rightShift(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?>>?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)>>?", Vars: []interface{}{e.e, value}})
}

func (e expr) leftShift(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?<<?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)<<?", Vars: []interface{}{e.e, value}})
}

func (e expr) bitXor(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?^?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)^?", Vars: []interface{}{e.e, value}})
}

func (e expr) bitAnd(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?&?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)&?", Vars: []interface{}{e.e, value}})
}

func (e expr) bitOr(value interface{}) expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "?|?", Vars: []interface{}{e.col, value}})
	}
	return e.setE(clause.Expr{SQL: "(?)|?", Vars: []interface{}{e.e, value}})
}

func (e expr) bitFlip() expr {
	if e.isPure() {
		return e.setE(clause.Expr{SQL: "~?", Vars: []interface{}{e.col}})
	}
	return e.setE(clause.Expr{SQL: "~(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) regexp(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? ~ ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) not() expr {
	return e.setE(clause.Expr{SQL: "NOT ?", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) is(value interface{}) expr {
	return e.setE(clause.Eq{Column: e.RawExpr(), Value: value})
}

func (e expr) and(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? AND ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) or(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? OR ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) xor(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? XOR ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) isPure() bool {
	return e.e == nil
}

func (e expr) ifNull(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "IFNULL(?,?)", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) field(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "FIELD(?, ?)", Vars: []interface{}{e.RawExpr(), value}, WithoutParentheses: true})
}

func (e expr) sum() expr {
	return e.setE(clause.Expr{SQL: "SUM(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) DateValueBetweenCol(value interface{}, col Expr) expr {
	return e.setE(clause.Expr{SQL: "? between ? and ? + INTERVAL '1 days' * ?", Vars: []interface{}{value, e.RawExpr(), e.RawExpr(), col.RawExpr()}})
}

func (e expr) Coalesce(col Expr) expr {
	return e.setE(clause.Expr{SQL: "COALESCE(?,?)", Vars: []interface{}{e.RawExpr(), col.RawExpr()}})
}

func (e expr) Include(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? && ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) ReverseLeftLike(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? LIKE '%' || ?", Vars: []interface{}{value, e.RawExpr()}})
}

func (e expr) ReverseRightLike(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? LIKE ? || '%'", Vars: []interface{}{value, e.RawExpr()}})
}

func (e expr) ReverseLike(value interface{}) expr {
	return e.setE(clause.Expr{SQL: "? LIKE '%' || ? || '%'", Vars: []interface{}{value, e.RawExpr()}})
}

func (e expr) JsonEq(paths []string, value interface{}) expr {
	var _paths []string
	for _, path := range paths {
		_paths = append(_paths, "'"+path+"'")
	}
	indexPath := len(_paths) - 1
	pathStr := strings.Join(_paths[:indexPath], "->")
	if len(pathStr) > 0 {
		pathStr = "->"+pathStr
	}
	pathStr += "->>" + _paths[indexPath]
	return e.setE(clause.Expr{SQL: "?"+pathStr+" = ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) JsonValueNull(paths []string) expr {
	var _paths []string
	for _, path := range paths {
		_paths = append(_paths, "'"+path+"'")
	}
	indexPath := len(_paths) - 1
	pathStr := strings.Join(_paths[:indexPath], "->")
	if len(pathStr) > 0 {
		pathStr = "->"+pathStr
	}
	pathStr += "->>" + _paths[indexPath]
	return e.setE(clause.Expr{SQL: "?"+pathStr+" is null", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) JsonValueNotNull(paths []string) expr {
	var _paths []string
	for _, path := range paths {
		_paths = append(_paths, "'"+path+"'")
	}
	indexPath := len(_paths) - 1
	pathStr := strings.Join(_paths[:indexPath], "->")
	if len(pathStr) > 0 {
		pathStr = "->"+pathStr
	}
	pathStr += "->>" + _paths[indexPath]
	return e.setE(clause.Expr{SQL: "?"+pathStr+" is not null", Vars: []interface{}{e.RawExpr()}})
}


func (e expr) ArrayContains(array Expr) Expr {
	return e.setE(clause.Expr{SQL: "? @> ?", Vars: []interface{}{e.RawExpr(), array.RawExpr()}})
}

func (e expr) ArrayContainedBy(array Expr) Expr {
	return e.setE(clause.Expr{SQL: "? <@ ?", Vars: []interface{}{e.RawExpr(), array.RawExpr()}})
}

func (e expr) ArrayOverlap(array Expr) Expr {
	return e.setE(clause.Expr{SQL: "? && ?", Vars: []interface{}{e.RawExpr(), array.RawExpr()}})
}


func (e expr) JsonGetField(field string) Expr {
	return e.setE(clause.Expr{SQL: "? -> ?", Vars: []interface{}{e.RawExpr(), field}})
}

func (e expr) JsonGetTextField(field string) Expr {
	return e.setE(clause.Expr{SQL: "? ->> ?", Vars: []interface{}{e.RawExpr(), field}})
}

func (e expr) JsonContains(value interface{}) Expr {
	return e.setE(clause.Expr{SQL: "? @> ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) JsonbArrayLength() Expr {
	return e.setE(clause.Expr{SQL: "jsonb_array_length(?)", Vars: []interface{}{e.RawExpr()}})
}


func (e expr) RegexpMatch(pattern string) Expr {
	return e.setE(clause.Expr{SQL: "? ~ ?", Vars: []interface{}{e.RawExpr(), pattern}})
}

func (e expr) IRegexpMatch(pattern string) Expr {
	return e.setE(clause.Expr{SQL: "? ~* ?", Vars: []interface{}{e.RawExpr(), pattern}})
}


func (e expr) DatePart(field string) Expr {
	return e.setE(clause.Expr{SQL: "DATE_PART(?, ?)", Vars: []interface{}{field, e.RawExpr()}})
}

func (e expr) Age() Expr {
	return e.setE(clause.Expr{SQL: "AGE(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) Now() Expr {
	return e.setE(clause.Expr{SQL: "CURRENT_TIMESTAMP", Vars: nil})
}



func (e expr) RowNumber() Expr {
	return e.setE(clause.Expr{SQL: "ROW_NUMBER() OVER (PARTITION BY ? ORDER BY ?)", Vars: []interface{}{e.RawExpr(), e.RawExpr()}})
}



func (e expr) BitAnd(value interface{}) Expr {
	return e.setE(clause.Expr{SQL: "? & ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) BitOr(value interface{}) Expr {
	return e.setE(clause.Expr{SQL: "? | ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) BitXor(value interface{}) Expr {
	return e.setE(clause.Expr{SQL: "? # ?", Vars: []interface{}{e.RawExpr(), value}})
}



func (e expr) ILike(value string) Expr {
	return e.setE(clause.Expr{SQL: "? ILIKE ?", Vars: []interface{}{e.RawExpr(), value}})
}

func (e expr) DistinctOn() Expr {
	return e.setE(clause.Expr{SQL: "DISTINCT ON (?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) Coalesce(values ...interface{}) Expr {
	return e.setE(clause.Expr{SQL: "COALESCE(?, ?)", Vars: append([]interface{}{e.RawExpr()}, values...)})
}



func (e expr) CaseWhen(conditions []Expr, results []Expr) Expr {
	var cases []string
	var vars []interface{}
	for i, condition := range conditions {
		cases = append(cases, fmt.Sprintf("WHEN %s THEN %s", condition.RawExpr(), results[i].RawExpr()))
		vars = append(vars, condition.RawExpr(), results[i].RawExpr())
	}
	return e.setE(clause.Expr{
		SQL:  "CASE " + strings.Join(cases, " ") + " ELSE NULL END",
		Vars: vars,
	})
}

func (e expr) NullIf(value interface{}) Expr {
	return e.setE(clause.Expr{SQL: "NULLIF(?, ?)", Vars: []interface{}{e.RawExpr(), value}})
}



func (e expr) Lower() Expr {
	return e.setE(clause.Expr{SQL: "LOWER(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) Upper() Expr {
	return e.setE(clause.Expr{SQL: "UPPER(?)", Vars: []interface{}{e.RawExpr()}})
}

func (e expr) Trim() Expr {
	return e.setE(clause.Expr{SQL: "TRIM(?)", Vars: []interface{}{e.RawExpr()}})
}


