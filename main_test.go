package main

import (
	"strings"
	"testing"
)

func TestMakeACHFileReplacesPPDDDA(t *testing.T) {
	out := makeACHFile(ppdDebitACH, "100000123")
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "627") {
			if got := strings.TrimSpace(line[12:29]); got != "100000123" {
				t.Fatalf("PPD DDA = %q, want 100000123", got)
			}
			return
		}
	}
	t.Fatal("missing PPD entry detail line")
}

func TestMakeACHFileReplacesIATDDA(t *testing.T) {
	out := makeACHFile(iatDebitACH, "100000456")
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "627") {
			if got := strings.TrimSpace(line[39:74]); got != "100000456" {
				t.Fatalf("IAT DDA = %q, want 100000456", got)
			}
			if got := strings.TrimSpace(line[29:39]); got != "0000002826" {
				t.Fatalf("IAT amount was changed: %q", got)
			}
			return
		}
	}
	t.Fatal("missing IAT entry detail line")
}
