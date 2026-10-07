// Transforms: an expr-lang program over msg, the record's value decoded from JSON.
// Its result, encoded as JSON, is the value the consumer forwards; nil drops the
// record. Validate compiles every transform on deploy; the consumer that runs one
// compiles it again when it starts.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// transformEnv declares msg as a map so that field access compiles; at run time
// msg is whatever the value decodes to (an object, an array, a string, a number).
var transformEnv = map[string]any{"msg": map[string]any{}}

// compileTransform compiles src for runTransform.
func compileTransform(src string) (*vm.Program, error) {
	p, err := expr.Compile(src, expr.Env(transformEnv))
	if err != nil {
		return nil, firstLine(err)
	}
	return p, nil
}

// runTransform runs p over a record's value. keep is false when the program
// returned nil: the record is dropped, which is not an error.
func runTransform(p *vm.Program, value []byte) (out []byte, keep bool, err error) {
	var msg any
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, false, errInvalidJSON
	}
	res, err := expr.Run(p, map[string]any{"msg": msg})
	if err != nil {
		return nil, false, firstLine(err)
	}
	if res == nil {
		return nil, false, nil
	}
	if out, err = json.Marshal(res); err != nil {
		return nil, false, fmt.Errorf("result: %w", err)
	}
	return out, true, nil
}

// firstLine keeps an expr error's first line; the rest repeats the expression
// with a caret under the fault, which a one-line counter or 422 cannot show.
func firstLine(err error) error {
	first, _, _ := strings.Cut(err.Error(), "\n")
	return errors.New(first)
}
