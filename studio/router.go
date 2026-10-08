// Routers: ordered rules over msg, the record's value (after its consumer's
// transform) decoded from JSON. A record goes to the topic of the first rule
// whose condition holds, else to the default topic, else nowhere. Validate
// compiles every rule on deploy; the consumer that runs a router compiles them
// again when it starts.
package main

import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compileRule compiles a rule's condition in a transform's environment. It must
// yield true or false: a condition known not to (a string, a number) does not
// compile; with msg declared any, one of unknown type does, and a result that is
// not a boolean is an error when it runs.
func compileRule(src string) (*vm.Program, error) {
	p, err := expr.Compile(src, expr.Env(transformEnv{}), expr.AsBool())
	if err != nil {
		return nil, firstLine(err)
	}
	return p, nil
}
