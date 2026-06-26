package output

import (
	"bytes"
	"testing"
)

func TestWriters(t *testing.T) {
	rows := []Row{
		{Value: "apple", Count: 2},
		{Value: "orange", Count: 1},
	}

	t.Run("plain", func(t *testing.T) {
		var buf bytes.Buffer
		if err := NewWriter(&buf, ModePlain, true).Write(rows); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if got := buf.String(); got != "2 apple\n1 orange\n" {
			t.Fatalf("unexpected output: %q", got)
		}
	})

	t.Run("csv", func(t *testing.T) {
		var buf bytes.Buffer
		if err := NewWriter(&buf, ModeCSV, true).Write(rows); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if got := buf.String(); got != "count,value\n2,apple\n1,orange\n" {
			t.Fatalf("unexpected output: %q", got)
		}
	})

	t.Run("json", func(t *testing.T) {
		var buf bytes.Buffer
		if err := NewWriter(&buf, ModeJSON, true).Write(rows); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if got := buf.String(); got != `[{"value":"apple","count":2},{"value":"orange","count":1}]`+"\n" {
			t.Fatalf("unexpected output: %q", got)
		}
	})
}
