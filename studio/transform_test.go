package main

import (
	"strings"
	"testing"
)

func TestCompileTransform(t *testing.T) {
	if _, err := compileTransform(`{id: msg.id, total: msg.qty * msg.price}`); err != nil {
		t.Fatalf("the spec's example must compile: %v", err)
	}
	for _, src := range []string{`msg.`, `foo + 1`} {
		_, err := compileTransform(src)
		if err == nil || strings.Contains(err.Error(), "\n") {
			t.Fatalf("%q: want a one-line compile error, got %q", src, err)
		}
	}
}

func TestRunTransform(t *testing.T) {
	for _, c := range []struct {
		src, value string
		out        string // "" with errHas "" means dropped
		errHas     string
	}{
		{`{id: msg.id, total: msg.qty * msg.price}`, `{"id":1,"qty":2,"price":3}`, `{"id":1,"total":6}`, ""},
		{`{id: msg.id, total: msg.qty * msg.price}`, `{"id":2}`, "", "invalid operation"},
		{`msg.qty > 0 ? msg : nil`, `{"qty":0}`, "", ""},
		{`msg.qty > 0 ? msg : nil`, `{"qty":5}`, `{"qty":5}`, ""},
		{`msg`, `[1,2]`, `[1,2]`, ""},
		{`msg * 2`, `3`, `6`, ""},
		{`msg[0]`, `[7,8]`, `7`, ""},
		{`msg`, `{"id":9007199254740993,"price":1.5}`, `{"id":9007199254740993,"price":1.5}`, ""},
		{`msg.qty * 1.5`, `{"qty":2}`, `3`, ""},
		{`msg`, `{`, "", "not valid JSON"},
		{`msg`, `{} {}`, "", "not valid JSON"},
		{`msg.a / 0`, `{"a":1}`, "", "result:"},
	} {
		p, err := compileTransform(c.src)
		if err != nil {
			t.Fatalf("%q: %v", c.src, err)
		}
		out, err := runTransform(p, []byte(c.value))
		if c.errHas != "" {
			if err == nil || !strings.Contains(err.Error(), c.errHas) || strings.Contains(err.Error(), "\n") {
				t.Errorf("%q on %s: want a one-line error containing %q, got %v", c.src, c.value, c.errHas, err)
			}
		} else if err != nil || string(out) != c.out {
			t.Errorf("%q on %s: want %q, got %q %v", c.src, c.value, c.out, out, err)
		}
	}
}
