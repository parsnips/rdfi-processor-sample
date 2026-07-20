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

func TestMakeACHFileWithoutDDALeavesReturnFixtureIntact(t *testing.T) {
	out := makeACHFile(unmatchedReturnACH, "")
	if out != unmatchedReturnACH+"\n" {
		t.Fatal("return fixture changed")
	}
}

func TestUnmatchedReturnACHRecordShape(t *testing.T) {
	lines := strings.Split(unmatchedReturnACH, "\n")
	if len(lines) != 10 {
		t.Fatalf("record count = %d, want 10", len(lines))
	}
	for i, line := range lines {
		if len(line) != 94 {
			t.Fatalf("record %d length = %d, want 94", i+1, len(line))
		}
	}
	if !strings.HasPrefix(lines[3], "799R01") {
		t.Fatalf("addenda record = %q, want an R01 Addenda 99", lines[3])
	}
	if got := lines[3][6:21]; got != "111926410064431" {
		t.Fatalf("original trace = %q, want unmatched trace", got)
	}
}

func TestAutoPendingScenarios(t *testing.T) {
	selected, err := selectScenarios(scenarios(), "^autopend-")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 3 {
		t.Fatalf("auto-pending scenario count = %d, want 3", len(selected))
	}
	for _, sc := range selected {
		if !sc.AutoPending {
			t.Fatalf("scenario %s is not marked auto-pending", sc.ID)
		}
	}
}
