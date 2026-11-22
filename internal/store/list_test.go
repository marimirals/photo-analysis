package store

import (
	"strings"
	"testing"
)

func TestBuildListQuery(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name      string
		f         ListFilter
		wantArgs  int // аргументов в списочном запросе
		wantParts []string
	}{
		{"без фильтров", ListFilter{Limit: 20}, 3, []string{"p.user_id = $1", "LIMIT $2 OFFSET $3"}},
		{"статус", ListFilter{Status: "done", Limit: 20}, 4, []string{"p.status = $2", "LIMIT $3 OFFSET $4"}},
		{"все фильтры", ListFilter{Status: "done", Blurry: &yes, BadShot: &no, Duplicates: &yes, Limit: 5},
			6, []string{"p.status = $2", "a.is_blurry = $3", "a.is_bad_shot = $4", "AND EXISTS", "LIMIT $5 OFFSET $6"}},
		{"без дубликатов", ListFilter{Duplicates: &no, Limit: 5}, 3, []string{"AND NOT EXISTS"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			countSQL, listSQL, countArgs, listArgs := buildListQuery("masha", c.f)
			if len(listArgs) != c.wantArgs || len(countArgs) != c.wantArgs-2 {
				t.Fatalf("args: list %d count %d, want %d/%d", len(listArgs), len(countArgs), c.wantArgs, c.wantArgs-2)
			}
			for _, part := range c.wantParts {
				if !strings.Contains(listSQL, part) {
					t.Errorf("list SQL has no %q:\n%s", part, listSQL)
				}
			}
			if strings.Contains(countSQL, "LIMIT") {
				t.Error("count SQL must not have LIMIT")
			}
			if strings.Contains(listSQL, "masha") {
				t.Error("values must be passed as parameters, not inlined")
			}
		})
	}
}
