package main

import "testing"

func TestFormatValueNumericTypeCoercesText(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		colType byte
		want    string
	}{
		{"integer text", "123", 'I', "123"},
		{"integer nonnumeric text", "ijika", 'I', "0"},
		{"real text", "12.345", 'R', "12.3"},
		{"real nonnumeric text", "ijika", 'R', "0"},
		{"text keeps string", "ijika", 'T', "ijika"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatValue(tt.value, tt.colType); got != tt.want {
				t.Fatalf("formatValue(%v, %q) = %q, want %q", tt.value, tt.colType, got, tt.want)
			}
		})
	}
}
