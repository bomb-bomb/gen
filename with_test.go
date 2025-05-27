package gen

import (
	"testing"

	"gorm.io/gorm/clause"
	"gorm.io/gen/field"
)

func TestWindowFunction(t *testing.T) {
	// Test ROW_NUMBER() window function
	rowNum := RowNumber()
	if rowNum.Function != "ROW_NUMBER()" {
		t.Errorf("Expected ROW_NUMBER(), got %s", rowNum.Function)
	}

	// Test RANK() window function
	rank := Rank()
	if rank.Function != "RANK()" {
		t.Errorf("Expected RANK(), got %s", rank.Function)
	}

	// Test DENSE_RANK() window function
	denseRank := DenseRank()
	if denseRank.Function != "DENSE_RANK()" {
		t.Errorf("Expected DENSE_RANK(), got %s", denseRank.Function)
	}
}

func TestWindowFunctionWithOver(t *testing.T) {
	// Create a mock field expression
	mockField := field.NewExpr("test_field", clause.Expr{SQL: "test_field"})
	
	// Test window function with OVER clause
	wf := RowNumber()
	over := wf.Over()
	over.PartitionBy(mockField).OrderBy(mockField)
	
	sql := wf.buildSQL()
	expected := "ROW_NUMBER() OVER (PARTITION BY test_field ORDER BY test_field)"
	if sql != expected {
		t.Errorf("Expected %s, got %s", expected, sql)
	}
}

func TestWindowFunctionWithFrame(t *testing.T) {
	// Create a mock field expression
	mockField := field.NewExpr("test_field", clause.Expr{SQL: "test_field"})
	
	// Test window function with frame specification
	wf := Sum(mockField)
	over := wf.Over()
	over.PartitionBy(mockField).OrderBy(mockField).Rows("UNBOUNDED PRECEDING", "CURRENT ROW")
	
	sql := wf.buildSQL()
	expected := "SUM(test_field) OVER (PARTITION BY test_field ORDER BY test_field ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
	if sql != expected {
		t.Errorf("Expected %s, got %s", expected, sql)
	}
}

func TestAggregateWindowFunctions(t *testing.T) {
	// Create a mock field expression
	mockField := field.NewExpr("amount", clause.Expr{SQL: "amount"})
	
	// Test COUNT window function
	count := Count(mockField)
	if count.Function != "COUNT(amount)" {
		t.Errorf("Expected COUNT(amount), got %s", count.Function)
	}
	
	// Test SUM window function
	sum := Sum(mockField)
	if sum.Function != "SUM(amount)" {
		t.Errorf("Expected SUM(amount), got %s", sum.Function)
	}
	
	// Test AVG window function
	avg := Avg(mockField)
	if avg.Function != "AVG(amount)" {
		t.Errorf("Expected AVG(amount), got %s", avg.Function)
	}
	
	// Test MAX window function
	max := Max(mockField)
	if max.Function != "MAX(amount)" {
		t.Errorf("Expected MAX(amount), got %s", max.Function)
	}
	
	// Test MIN window function
	min := Min(mockField)
	if min.Function != "MIN(amount)" {
		t.Errorf("Expected MIN(amount), got %s", min.Function)
	}
}

func TestWithClauseExpr(t *testing.T) {
	// Test WithClauseExpr Build method
	withExpr := &WithClauseExpr{
		SQL:  "WITH test_cte AS (SELECT * FROM test_table)",
		Args: []interface{}{"arg1", "arg2"},
	}
	
	// Mock builder for testing
	mockBuilder := &mockClauseBuilder{}
	withExpr.Build(mockBuilder)
	
	if mockBuilder.sql != "WITH test_cte AS (SELECT * FROM test_table)" {
		t.Errorf("Expected WITH clause SQL, got %s", mockBuilder.sql)
	}
	
	if len(mockBuilder.vars) != 2 {
		t.Errorf("Expected 2 variables, got %d", len(mockBuilder.vars))
	}
}

// Mock clause builder for testing
type mockClauseBuilder struct {
	sql  string
	vars []interface{}
}

func (m *mockClauseBuilder) WriteString(s string) (int, error) {
	m.sql += s
	return len(s), nil
}

func (m *mockClauseBuilder) AddVar(writer clause.Writer, vars ...interface{}) {
	m.vars = append(m.vars, vars...)
}

func (m *mockClauseBuilder) WriteByte(b byte) error {
	m.sql += string(b)
	return nil
}

func (m *mockClauseBuilder) WriteQuoted(value interface{}) {
	m.sql += "\"" + value.(string) + "\""
}

func (m *mockClauseBuilder) AddError(err error) error {
	return err
}

func TestFrameClause(t *testing.T) {
	// Test ROWS frame
	mockField := field.NewExpr("test_field", clause.Expr{SQL: "test_field"})
	wf := Sum(mockField)
	over := wf.Over()
	over.Rows("2 PRECEDING", "2 FOLLOWING")
	
	if over.frame.Type != "ROWS" {
		t.Errorf("Expected ROWS frame type, got %s", over.frame.Type)
	}
	
	if over.frame.Start != "2 PRECEDING" {
		t.Errorf("Expected '2 PRECEDING' start, got %s", over.frame.Start)
	}
	
	if over.frame.End != "2 FOLLOWING" {
		t.Errorf("Expected '2 FOLLOWING' end, got %s", over.frame.End)
	}
	
	// Test RANGE frame
	wf2 := Avg(mockField)
	over2 := wf2.Over()
	over2.Range("UNBOUNDED PRECEDING", "CURRENT ROW")
	
	if over2.frame.Type != "RANGE" {
		t.Errorf("Expected RANGE frame type, got %s", over2.frame.Type)
	}
}

func TestComplexWindowFunction(t *testing.T) {
	// Test complex window function with multiple clauses
	mockField1 := field.NewExpr("unionid", clause.Expr{SQL: "unionid"})
	mockField2 := field.NewExpr("platform_name", clause.Expr{SQL: "platform_name"})
	mockField3 := field.NewExpr("created_at", clause.Expr{SQL: "created_at"})
	
	wf := RowNumber()
	over := wf.Over()
	over.PartitionBy(mockField1, mockField2).OrderBy(mockField3)
	
	sql := wf.buildSQL()
	expected := "ROW_NUMBER() OVER (PARTITION BY unionid, platform_name ORDER BY created_at)"
	if sql != expected {
		t.Errorf("Expected %s, got %s", expected, sql)
	}
} 