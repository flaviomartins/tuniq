package output

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

type Mode string

const (
	ModePlain Mode = "plain"
	ModeCSV   Mode = "csv"
	ModeJSON  Mode = "json"
)

type Row struct {
	Value string
	Count uint64
}

type Writer interface {
	Write(rows []Row) error
}

func NewWriter(w io.Writer, mode Mode, showCount bool) Writer {
	switch mode {
	case ModeCSV:
		return &csvWriter{w: w, showCount: showCount}
	case ModeJSON:
		return &jsonWriter{w: w, showCount: showCount}
	default:
		return &plainWriter{w: w, showCount: showCount}
	}
}

type plainWriter struct {
	w         io.Writer
	showCount bool
}

func (w *plainWriter) Write(rows []Row) error {
	bw := bufio.NewWriterSize(w.w, 256*1024)
	for _, row := range rows {
		if w.showCount {
			if _, err := fmt.Fprintf(bw, "%d %s\n", row.Count, row.Value); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(bw, "%s\n", row.Value); err != nil {
			return err
		}
	}
	return bw.Flush()
}

type csvWriter struct {
	w         io.Writer
	showCount bool
}

func (w *csvWriter) Write(rows []Row) error {
	cw := csv.NewWriter(w.w)
	if w.showCount {
		if err := cw.Write([]string{"count", "value"}); err != nil {
			return err
		}
		for _, row := range rows {
			if err := cw.Write([]string{fmt.Sprintf("%d", row.Count), row.Value}); err != nil {
				return err
			}
		}
	} else {
		if err := cw.Write([]string{"value"}); err != nil {
			return err
		}
		for _, row := range rows {
			if err := cw.Write([]string{row.Value}); err != nil {
				return err
			}
		}
	}
	cw.Flush()
	return cw.Error()
}

type jsonWriter struct {
	w         io.Writer
	showCount bool
}

func (w *jsonWriter) Write(rows []Row) error {
	type jsonEntryWithCount struct {
		Value string `json:"value"`
		Count uint64 `json:"count"`
	}
	type jsonEntryValueOnly struct {
		Value string `json:"value"`
	}

	bw := bufio.NewWriterSize(w.w, 256*1024)
	if _, err := bw.WriteString("["); err != nil {
		return err
	}
	for i, row := range rows {
		if i > 0 {
			if _, err := bw.WriteString(","); err != nil {
				return err
			}
		}
		var (
			b   []byte
			err error
		)
		if w.showCount {
			b, err = json.Marshal(jsonEntryWithCount{Value: row.Value, Count: row.Count})
		} else {
			b, err = json.Marshal(jsonEntryValueOnly{Value: row.Value})
		}
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
	}
	if _, err := bw.WriteString("]\n"); err != nil {
		return err
	}
	return bw.Flush()
}
