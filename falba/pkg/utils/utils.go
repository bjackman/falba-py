package utils

import (
	"falba/pkg/model" // Assuming 'falba' is the module name
	"fmt"
)

// DumpResult prints the details of a model.Result object in a human-readable format.
func DumpResult(result model.Result) {
	fmt.Printf("Result(%s:%s)\n", result.TestName, result.ResultID)

	fmt.Println("	facts:")
	if len(result.Facts) > 0 {
		for name, fact := range result.Facts {
			// Default unit string if fact.Unit is nil
			factUnitStr := ""
			if fact.Unit != nil {
				factUnitStr = fmt.Sprintf(" (Unit: %s)", *fact.Unit)
			}
			// Using %v for value, which generally works well.
			// Using %-30s for left-justified name with padding.
			fmt.Printf("		%-30s: %v%s\n", name, fact.Value, factUnitStr)
		}
	} else {
		fmt.Println("		(no facts)")
	}

	fmt.Println("	metrics:")
	if len(result.Metrics) > 0 {
		for _, metric := range result.Metrics {
			metricUnitStr := ""
			if metric.Unit != nil {
				metricUnitStr = *metric.Unit
			}
			// Using %v for value.
			// Using %-30s for left-justified name with padding.
			fmt.Printf("		%-30s: %v (Unit: %s)\n", metric.Name, metric.Value, metricUnitStr)
		}
	} else {
		fmt.Println("		(no metrics)")
	}
}
