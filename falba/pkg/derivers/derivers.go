package derivers

import (
	"falba/pkg/model"
	"fmt"
	"log"
	"strings"
)

var RegisteredDerivers []model.DeriverFunc

func RegisterDeriver(d model.DeriverFunc) {
	RegisteredDerivers = append(RegisteredDerivers, d)
}

func GetAllDerivers() []model.DeriverFunc {
	return RegisteredDerivers
}

// DeriveAsiOn derives "asi_on" fact based on "cmdline" fact.
func DeriveAsiOn(result model.Result) ([]model.Fact[any], []model.Metric[any], error) {
	cmdlineFact, ok := result.Facts["cmdline"]
	if !ok {
		log.Printf("Debug: derive_asi_on: 'cmdline' fact not found for result %s/%s. Skipping.", result.TestName, result.ResultID)
		return nil, nil, nil
	}

	cmdline, ok := cmdlineFact.Value.(string)
	if !ok {
		log.Printf("Debug: derive_asi_on: 'cmdline' fact for result %s/%s is not a string. Skipping.", result.TestName, result.ResultID)
		return nil, nil, nil
	}

	asiOn := false
	if strings.Contains(cmdline, "mitigations=auto,nosmt") || strings.Contains(cmdline, "nosmt,mitigations=auto") {
		asiOn = true
	}

	newFact := model.Fact[any]{Name: "asi_on", Value: asiOn}
	log.Printf("Debug: derive_asi_on: for result %s/%s, cmdline: '%s', asi_on: %t", result.TestName, result.ResultID, cmdline, asiOn)

	return []model.Fact[any]{newFact}, nil, nil
}

// DeriveRetbleedMitigation derives "retbleed_mitigation" fact based on "cmdline" and "lscpu_smp_active" facts.
func DeriveRetbleedMitigation(result model.Result) ([]model.Fact[any], []model.Metric[any], error) {
	cmdlineFact, cmdlineFound := result.Facts["cmdline"]
	smpActiveFact, smpActiveFound := result.Facts["lscpu_smp_active"]

	if !cmdlineFound {
		log.Printf("Debug: derive_retbleed_mitigation: 'cmdline' fact not found for result %s/%s. Skipping.", result.TestName, result.ResultID)
		return nil, nil, nil
	}

	cmdline, ok := cmdlineFact.Value.(string)
	if !ok {
		log.Printf("Debug: derive_retbleed_mitigation: 'cmdline' fact for result %s/%s is not a string. Skipping.", result.TestName, result.ResultID)
		return nil, nil, nil
	}

	smpActive := false // Default if fact is missing or not a bool
	if smpActiveFound {
		if val, typeOk := smpActiveFact.Value.(bool); typeOk {
			smpActive = val
		} else {
			log.Printf("Debug: derive_retbleed_mitigation: 'lscpu_smp_active' for result %s/%s is not a boolean. Defaulting to false.", result.TestName, result.ResultID)
		}
	} else {
		log.Printf("Debug: derive_retbleed_mitigation: 'lscpu_smp_active' fact not found for result %s/%s. Defaulting to false.", result.TestName, result.ResultID)
	}

	var mitigation string

	if strings.Contains(cmdline, "retbleed=off") {
		mitigation = "off"
	} else if strings.Contains(cmdline, "retbleed=auto,nosmt") {
		if smpActive {
			mitigation = "stibp"
		} else {
			mitigation = "unret"
		}
	} else if strings.Contains(cmdline, "retbleed=ibpb") {
		mitigation = "ibpb"
	} else if strings.Contains(cmdline, "retbleed=unret") {
		if smpActive {
			mitigation = "stibp" // Python code says "unret,nosmt" -> "stibp", this seems to be a direct mapping
		} else {
			mitigation = "unret"
		}
	} else if strings.Contains(cmdline, "retbleed=unret,nosmt") { // Explicitly check for this combo
		mitigation = "stibp"
	} else {
		log.Printf("Debug: derive_retbleed_mitigation: No specific retbleed mitigation found in cmdline for %s/%s. Defaulting to 'unknown'. Cmdline: %s", result.TestName, result.ResultID, cmdline)
		mitigation = "unknown" // Or some other default/indicator
	}

	newFact := model.Fact[any]{Name: "retbleed_mitigation", Value: mitigation}
	log.Printf("Debug: derive_retbleed_mitigation: for result %s/%s, cmdline: '%s', smp_active: %t, mitigation: '%s'", result.TestName, result.ResultID, cmdline, smpActive, mitigation)

	return []model.Fact[any]{newFact}, nil, nil
}

func init() {
	RegisterDeriver(DeriveAsiOn)
	RegisterDeriver(DeriveRetbleedMitigation)
}
