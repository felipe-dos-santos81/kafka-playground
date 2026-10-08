package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompileRule(t *testing.T) {
	for _, src := range []string{`msg.total > 100`, `msg.region == "EU"`, `msg.paid`} {
		if _, err := compileRule(src); err != nil {
			t.Errorf("%q: want it to compile, got %v", src, err)
		}
	}
	for src, want := range map[string]string{`"x"`: "expected bool, but got string", `1 + 2`: "expected bool, but got int", `msg.`: "unexpected end of expression"} {
		_, err := compileRule(src)
		if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "\n") {
			t.Errorf("%q: want a one-line error containing %q, got %v", src, want, err)
		}
	}
}

func TestRoute(t *testing.T) {
	routes := []Route{{When: "msg.total > 100", Topic: "big"}, {When: `msg.region == "EU"`, Topic: "eu"}}
	withDefault, err := newRouter(routes, "other")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		value, topic, errHas string
	}{
		{`{"total":150,"region":"EU"}`, "big", ""}, // both hold: the first rule wins
		{`{"total":5,"region":"EU"}`, "eu", ""},
		{`{"total":5}`, "other", ""},
		{`{"total":9007199254740993}`, "big", ""},            // integers exact, as in a transform
		{`{"region":"EU"}`, "", "rule 1: invalid operation"}, // nil > 100: the record is not routed, rule 2 is not tried
		{`{"total":5,"region":"EU"} x`, "", "not valid JSON"},
		{`{"total":200}`, "big", ""}, // a failed record leaves the VM fit for the next
	} {
		topic, err := withDefault.route([]byte(c.value))
		if c.errHas != "" {
			if err == nil || !strings.Contains(err.Error(), c.errHas) || strings.Contains(err.Error(), "\n") || topic != "" {
				t.Errorf("%s: want no topic and a one-line error containing %q, got %q %v", c.value, c.errHas, topic, err)
			}
		} else if err != nil || topic != c.topic {
			t.Errorf("%s: want %q, got %q %v", c.value, c.topic, topic, err)
		}
	}
	want := routeTally{tally: tally{Total: 7, Errors: 2, LastError: "value is not valid JSON"}, Branches: []int64{3, 1, 1}}
	if got := withDefault.read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("per rule, then the default:\n got %+v\nwant %+v", got, want)
	}

	notBoolean, err := newRouter([]Route{{When: "msg.flag", Topic: "flagged"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	if topic, err := notBoolean.route([]byte(`{"flag":1}`)); topic != "" || err == nil || !strings.HasPrefix(err.Error(), "rule 1: ") {
		t.Fatalf("a condition that is not a boolean when it runs is an error, not the default; got %q %v", topic, err)
	}

	noDefault, err := newRouter(routes, "")
	if err != nil {
		t.Fatal(err)
	}
	if topic, err := noDefault.route([]byte(`{"total":5}`)); topic != "" || err != nil {
		t.Fatalf("no rule matches and no default: dropped, not an error; got %q %v", topic, err)
	}
	want = routeTally{tally: tally{Total: 1}, Branches: []int64{0, 0, 0}, Unmatched: 1}
	if got := noDefault.read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("an unmatched record counts as unmatched:\n got %+v\nwant %+v", got, want)
	}
}
