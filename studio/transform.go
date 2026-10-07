// Transforms: an expr-lang program over msg, the record's value decoded from JSON.
// Its result, encoded as JSON, is the value the consumer forwards; nil drops the
// record. Validate compiles every transform on deploy; the consumer that runs one
// compiles it again when it starts.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// transformEnv declares msg as any, so field access, indexing and arithmetic all
// compile; at run time msg is whatever the value decodes to (an object, an
// array, a string, a number: an int when it is one, else a float64).
type transformEnv struct {
	Msg any `expr:"msg"`
}

// compileTransform compiles src for runTransform.
func compileTransform(src string) (*vm.Program, error) {
	p, err := expr.Compile(src, expr.Env(transformEnv{}))
	if err != nil {
		return nil, firstLine(err)
	}
	return p, nil
}

// runTransform runs p over a record's value. A nil out with no error means the
// program returned nil: the record is dropped, which is not an error.
func runTransform(p *vm.Program, value []byte) (out []byte, err error) {
	d := json.NewDecoder(bytes.NewReader(value))
	d.UseNumber()
	var msg any
	if err := d.Decode(&msg); err != nil {
		return nil, errInvalidJSON
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errInvalidJSON // trailing data after the value
	}
	res, err := expr.Run(p, transformEnv{Msg: numbers(msg)})
	if err != nil {
		return nil, firstLine(err)
	}
	if res == nil {
		return nil, nil
	}
	if out, err = json.Marshal(res); err != nil {
		return nil, fmt.Errorf("result: %w", err)
	}
	return out, nil
}

// numbers turns every JSON number in v into an int when it is one (so an id above
// 2^53 passes through exact) and a float64 otherwise.
func numbers(v any) any {
	switch v := v.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i)
		}
		f, _ := v.Float64() // out of range: ±Inf, which the result's encoding refuses
		return f
	case map[string]any:
		for k, x := range v {
			v[k] = numbers(x)
		}
	case []any:
		for i, x := range v {
			v[i] = numbers(x)
		}
	}
	return v
}

// firstLine keeps an expr error's first line; the rest repeats the expression
// with a caret under the fault, which a one-line counter or 422 cannot show.
func firstLine(err error) error {
	first, _, _ := strings.Cut(err.Error(), "\n")
	return errors.New(first)
}
