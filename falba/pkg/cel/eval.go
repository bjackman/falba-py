package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// EvalCELPredicate evaluates a CEL expression against an activation map
// and returns a boolean result.
func EvalCELPredicate(expression string, activation map[string]interface{}) (bool, error) {
	// Create a CEL environment.
	// To use the activation map, we need to declare the variables.
	// This is a bit more involved than Python's celpy which infers from activation.
	var declTypes []*cel.Decl
	for key, val := range activation {
		// Infer CEL type from Go type for declarations
		// This is a simplified inference. For more complex types, more robust mapping is needed.
		var celType *cel.Type
		switch val.(type) {
		case bool:
			celType = decls.Bool
		case int, int32, int64:
			celType = decls.Int
		case float32, float64:
			celType = decls.Double
		case string:
			celType = decls.String
		// Add more types as needed, e.g., lists, maps.
		// For map[string]interface{} or []interface{}, it's more complex.
		// For now, assume simple primitive types in activation.
		default:
			// Fallback to Dyn if type is unknown or complex.
			// This might require `cel.HomogeneousMapOrDynType` or similar for maps.
			// Using Dyn allows flexibility but sacrifices some static type checking.
			celType = decls.Dyn
			// return false, fmt.Errorf("unsupported type in activation for key %s: %T", key, val)
		}
		declTypes = append(declTypes, decls.NewVar(key, celType))
	}

	env, err := cel.NewEnv(
		cel.Declarations(declTypes...),
	)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	// Parse the expression.
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	// Compile the expression to a program.
	// Check for compile errors (this is part of NewProgram).
	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("failed to compile CEL program: %w", err)
	}

	// Evaluate the program with the provided activation data.
	out, _, err := prg.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate CEL program: %w", err)
	}

	// Ensure the result is a boolean and return it.
	boolVal, ok := out.(ref.Val)
	if !ok {
		return false, fmt.Errorf("CEL evaluation result is not a ref.Val, got %T", out)
	}

	if boolVal.Type() != types.BoolType {
		return false, fmt.Errorf("CEL evaluation result is not boolean, got type %s", boolVal.Type())
	}

	return boolVal.Value().(bool), nil
}

// Note: Extracting referenced identifiers (like Python's ast.ScanValues)
// from cel-go's AST (ast.SourceInfo, ast.Expr) is more involved and
// not straightforwardly available as a built-in utility.
// It would require traversing the AST and collecting identifier nodes.
// This is deferred as per instructions.
```
