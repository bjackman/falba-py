package main

import (
	"falba/pkg/cel"
	"falba/pkg/derivers"
	"falba/pkg/enrichers"
	"falba/pkg/model"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var (
	resultDbPath string
)

var rootCmd = &cobra.Command{
	Use:   "falba",
	Short: "Falba is a tool for analyzing benchmark results.",
	Long:  `Falba (Framework for Analyzing Benchmarks and Layered Aggregations) is a CLI tool to process and query benchmark result data.`,
}

var abCmd = &cobra.Command{
	Use:   "ab [expr]",
	Short: "Filter and display results based on a CEL expression against facts.",
	Long: `The 'ab' command (short for "analyze benchmarks" or "artifact-based filtering")
evaluates a Common Expression Language (CEL) expression against the facts of each result
in the database. If the expression evaluates to true, the result's identifier is printed.

The facts available for the CEL expression are those extracted or derived for each result.
Example: falba ab "hardware.cpu.model_name == 'Intel Core i9'"
Example: falba ab "os.release.id == 'ubuntu' && os.release.version_id == '22.04'"
	`,
	Args: cobra.ExactArgs(1), // Expects exactly one argument: the CEL expression
	RunE: func(cmd *cobra.Command, args []string) error {
		celExpression := args[0]

		// Read the database
		// Note: model.ReadDbDir expects path to directory containing test_name folders
		db, err := model.ReadDbDir(resultDbPath)
		if err != nil {
			return fmt.Errorf("failed to read result database from %s: %w", resultDbPath, err)
		}

		// Apply all registered enrichers first
		// This ensures facts from files like falba-facts.json, ansible.json etc. are loaded
		// The EnrichAll method was added in the previous subtask.
		// We need to pass the actual registered enrichers.
		enrichmentErrors := db.EnrichAll(enrichers.RegisteredEnrichers)
		if len(enrichmentErrors) > 0 {
			log.Printf("Encountered %d errors during enrichment phase:", len(enrichmentErrors))
			for _, eErr := range enrichmentErrors {
				log.Printf("  - %v", eErr)
			}
			// Decide if enrichment errors are fatal for 'ab' command.
			// For now, log and continue, as some results might still be processable.
		}
		
		// Apply all registered derivers
		// This creates derived facts like 'asi_on' or 'retbleed_mitigation'
		derivationErrors := db.DeriveAll(derivers.RegisteredDerivers)
		if len(derivationErrors) > 0 {
			log.Printf("Encountered %d errors during derivation phase:", len(derivationErrors))
			for _, dErr := range derivationErrors {
				log.Printf("  - %v", dErr)
			}
			// Log and continue
		}


		if len(db.Results) == 0 {
			log.Printf("No results found in the database at %s", resultDbPath)
			return nil
		}

		log.Printf("Loaded %d results. Evaluating CEL expression: %s", len(db.Results), celExpression)

		matchCount := 0
		for _, result := range db.Results {
			// Get fact values for CEL evaluation
			// The Python version uses result.fact_vals, which is a map[str, Any]
			// Our model.Result.FactVals() returns map[string]interface{}
			activation := result.FactVals()
			if activation == nil {
				activation = make(map[string]interface{}) // Ensure activation is not nil
			}
			
			// Add result_id and test_name to activation context, similar to Python's `r` variable
			activation["result_id"] = result.ResultID
			activation["test_name"] = result.TestName


			// For debugging: print activation keys
			// var keys []string
			// for k := range activation {
			// 	keys = append(keys, k)
			// }
			// log.Printf("Evaluating for ResultID %s with facts: %v", result.ResultID, keys)


			// Evaluate CEL expression
			evalResult, err := cel.EvalCELPredicate(celExpression, activation)
			if err != nil {
				// Log error for this specific result and continue to the next
				log.Printf("Error evaluating CEL expression for result %s (%s): %v. Skipping.", result.ResultID, result.TestName, err)
				continue
			}

			if evalResult {
				matchCount++
				// Print identifying information
				// Python version prints f"{result.test_name}/{result.result_id}"
				fmt.Printf("%s/%s\n", result.TestName, result.ResultID)
			}
		}

		log.Printf("CEL expression matched %d results.", matchCount)
		return nil
	},
}

func init() {
	// Add persistent flag to rootCmd
	rootCmd.PersistentFlags().StringVar(&resultDbPath, "result-db", "./results", "Path to the result database directory.")

	// Add abCmd as a subcommand to rootCmd
	rootCmd.AddCommand(abCmd)

	// Here you could also register global flags for rootCmd if needed
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// To build: go build -o falba_cli cmd/falba/main.go
// Example usage:
// ./falba_cli ab "result_id == 'some_id'" --result-db path/to/your/db
// ./falba_cli ab "test_name == 'my_test_suite'"
// ./falba_cli ab "some_fact_key == 'some_value'"
//
// Before running, ensure a database exists with the expected structure:
// result-db/
//   test_name_1/
//     result_id_A/
//       facts.json  (optional, for auto-loading facts)
//       ansible.json (optional, for ansible enricher)
//       phoronix.json (optional, for phoronix enricher)
//       some_artifact.txt
//     result_id_B/
//       ...
//   test_name_2/
//     result_id_C/
//       ...
//
// facts.json example:
// {
//   "os_version": "Ubuntu 22.04",
//   "kernel_version": "5.15.0-generic",
//   "cpus": 8,
//   "is_vm": false,
//   "benchmark_setting": { "value": 100, "unit": "iterations" }
// }

```
