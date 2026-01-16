package parser

import "fmt"

// ParseError represents a parsing error with position information.
type ParseError struct {
	Message string
	Line    int
	Column  int
	Token   string
}

func (e *ParseError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("parse error at line %d, column %d: %s (near %q)",
			e.Line, e.Column, e.Message, e.Token)
	}
	return fmt.Sprintf("parse error: %s", e.Message)
}

// newError creates a new ParseError.
func newError(msg string, line, col int, token string) *ParseError {
	return &ParseError{
		Message: msg,
		Line:    line,
		Column:  col,
		Token:   token,
	}
}
