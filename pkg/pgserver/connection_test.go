package pgserver

import "testing"

func TestBindQuery(t *testing.T) {
	query, err := bindQuery(
		"SELECT $1, $2, $3, $4, '$5'",
		[]boundParameter{
			{value: []byte("O'Reilly")},
			{value: []byte("42")},
			{null: true},
			{value: []byte("true")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT 'O''Reilly', 42, NULL, TRUE, '$5'"
	if query != want {
		t.Fatalf("bound query = %q, want %q", query, want)
	}
}

func TestBindQueryRequiresEveryParameter(t *testing.T) {
	if _, err := bindQuery("SELECT $2", []boundParameter{{value: []byte("one")}}); err == nil {
		t.Fatal("expected missing parameter error")
	}
}
