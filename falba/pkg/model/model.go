package model

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EnricherFunc defines the signature for functions that enrich data from an artifact.
type EnricherFunc func(artifact Artifact) ([]Fact[any], []Metric[any], error)

// DeriverFunc defines the signature for functions that derive new data from a result.
type DeriverFunc func(result Result) ([]Fact[any], []Metric[any], error)

// Metric represents a numerical measurement with a unit.
type Metric[T any] struct {
	Name  string
	Value T
	Unit  *string // Using a pointer to represent an optional unit
}

// Fact represents a key-value pair, where the value can be of any type.
type Fact[T any] struct {
	Name  string
	Value T
	Unit  *string // Using a pointer to represent an optional unit
}

// Artifact represents a file and provides methods to access its content.
type Artifact struct {
	Path string
}

// NewArtifact creates a new Artifact and checks if the path exists.
func NewArtifact(path string) (*Artifact, error) {
	// In Python, this check happens in __post_init__.
	// We can do it here or leave it to the methods accessing the file.
	// For now, let's check on creation to match Python's __post_init__.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("artifact path %s does not exist: %w", path, err)
	}
	return &Artifact{Path: path}, nil
}

// Content reads the entire file and returns its content as bytes.
func (a *Artifact) Content() ([]byte, error) {
	return os.ReadFile(a.Path)
}

// JSON reads the file, parses it as JSON, and returns the data.
// Returns as map[string]interface{} for simplicity, similar to Python's dict.
func (a *Artifact) JSON() (map[string]interface{}, error) {
	data, err := a.Content()
	if err != nil {
		return nil, err
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON from %s: %w", a.Path, err)
	}
	return jsonData, nil
}

// Result holds data for a single test run.
type Result struct {
	TestName  string
	ResultID  string
	Artifacts map[string]Artifact // Map key is artifact path
	Facts     map[string]Fact[any] // Map key is fact name
	Metrics   []Metric[any]
}

// NewResult creates an initialized Result.
// The Python version's ReadDir is more of a constructor.
// We'll have a separate ReadResultDir function for that behavior.
func NewResult(testName, resultID string) *Result {
	return &Result{
		TestName:  testName,
		ResultID:  resultID,
		Artifacts: make(map[string]Artifact),
		Facts:     make(map[string]Fact[any]),
		Metrics:   []Metric[any]{},
	}
}

// AddFact adds a fact to the result, checking for duplicates.
func (r *Result) AddFact(fact Fact[any]) error {
	if _, exists := r.Facts[fact.Name]; exists {
		return fmt.Errorf("fact with name '%s' already exists", fact.Name)
	}
	r.Facts[fact.Name] = fact
	return nil
}

// AddMetric adds a metric to the result.
func (r *Result) AddMetric(metric Metric[any]) {
	r.Metrics = append(r.Metrics, metric)
}

// FactVals returns a map of fact names to their values.
func (r *Result) FactVals() map[string]interface{} {
	vals := make(map[string]interface{})
	for name, fact := range r.Facts {
		vals[name] = fact.Value
	}
	return vals
}

// ReadResultDir reads a directory and constructs a Result object.
// This is analogous to Result.ReadDir in the Python version.
func ReadResultDir(dirPath string, testName string) (*Result, error) {
	// result_dirname is "test_name/result_id"
	// ResultID is the last part of the dirPath
	resultID := filepath.Base(dirPath)

	// If testName is not provided, try to infer it from the parent directory.
	// This matches the Python logic: `self.test_name = result_dirname.split("/")[0]`
	// However, dirPath to ReadResultDir is expected to be the full path to the result dir,
	// e.g., "falba-db/test_foo/result123".
	// So, dirname(dirPath) would be "falba-db/test_foo", and basename of that is "test_foo".
	if testName == "" {
		parentDir := filepath.Dir(dirPath)
		if parentDir != "." && parentDir != "/" { // Basic check to avoid issues with root or current dir
			testName = filepath.Base(parentDir)
		} else {
			// Cannot infer testName, this might be an error or requires a default
			// For now, let's keep it empty or require it to be passed
			// The Python code structure implies test_name is known or inferred from a structure like "test_name/result_id"
		}
	}
	
	// The Python code uses result_dirname for Result.test_name if test_name is not passed.
	// result_dirname in Python is "test_name/result_id".
	// If testName is still empty, and dirPath is "test_name/result_id", then split it.
	// This is a bit fragile. It's better if testName is explicitly passed or the structure is guaranteed.
	// For now, we'll assume resultID is the basename, and testName is passed or inferred from one level up.
	// If dirPath was "test_name/result_id", then resultID = "result_id" and testName = "test_name".

	res := NewResult(testName, resultID)

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read result directory %s: %w", dirPath, err)
	}

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(dirPath, file.Name())
			artifact, err := NewArtifact(filePath) // Path existence check is here
			if err != nil {
				// Decide if this should be a fatal error for ReadResultDir or just a skipped file
				// Python code doesn't explicitly show error handling for non-existent symlinks etc.
				// during Artifact creation in Result.ReadDir
				// For now, let's log or skip. Here, we'll return the error.
				return nil, fmt.Errorf("failed to create artifact for %s: %w", filePath, err)
			}
			res.Artifacts[artifact.Path] = *artifact // In Python, key is basename, but path is more robust
		}
	}
	// The Python code reads facts and metrics from specific files (e.g., facts.json).
	// This logic needs to be added here. For now, Artifacts are just raw files.
	// Let's assume facts.json and metrics.json might exist.

	// Attempt to load facts from "facts.json"
	factsPath := filepath.Join(dirPath, "facts.json")
	if _, err := os.Stat(factsPath); err == nil {
		artifact, err := NewArtifact(factsPath)
		if err == nil { // Should not fail if Stat passed, but check anyway
			jsonData, err := artifact.JSON()
			if err == nil {
				for key, value := range jsonData {
					// Try to infer unit if value is a map with "value" and "unit"
					var factValue interface{} = value
					var factUnit *string

					if valMap, ok := value.(map[string]interface{}); ok {
						if v, exists := valMap["value"]; exists {
							factValue = v
						}
						if u, exists := valMap["unit"].(string); exists {
							factUnit = &u
						}
					}
					err := res.AddFact(Fact[any]{Name: key, Value: factValue, Unit: factUnit})
					if err != nil {
						// Handle duplicate fact error if necessary, or log
						// For now, let's return the error to be strict
						return nil, fmt.Errorf("error adding fact '%s' from %s: %w", key, factsPath, err)
					}
				}
			} else {
				// Log error reading/parsing facts.json?
				// For now, let's be strict.
				return nil, fmt.Errorf("error parsing %s: %w", factsPath, err)
			}
		}
	}
	
	// Attempt to load metrics from "metrics.json"
	// Python code stores metrics as a list in the Result, not from a specific file in Result.ReadDir
	// It seems metrics are added via AddMetric explicitly after Result creation.
	// So, we will not auto-load metrics.json here unless specified.

	return res, nil
}

// Db represents a database of results.
type Db struct {
	Results map[string]Result // Map key is result ID
}

// NewDb creates an initialized Db.
func NewDb() *Db {
	return &Db{
		Results: make(map[string]Result),
	}
}

// ReadDbDir reads a directory containing multiple result directories.
// Each subdirectory is expected to be a "test_name" directory,
// which in turn contains "result_id" directories.
// Or, subdirectories are directly "result_id" directories if results are not grouped by test_name.
// The Python code is: `for test_name in os.listdir(db_path): result_dirname = f"{db_path}/{test_name}/{res_id}"`
// This implies a structure like: db_path/test_name/result_id
// For simplicity, let's assume db_path contains test_name directories.
func ReadDbDir(dbPath string) (*Db, error) {
	db := NewDb()

	testNameDirs, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read db directory %s: %w", dbPath, err)
	}

	for _, testNameEntry := range testNameDirs {
		if testNameEntry.IsDir() {
			testName := testNameEntry.Name()
			testPath := filepath.Join(dbPath, testName)

			resultIdDirs, err := os.ReadDir(testPath)
			if err != nil {
				// Log or handle error for this specific test_name directory
				// For now, continue to next, or return error to be strict
				return nil, fmt.Errorf("failed to read test directory %s: %w", testPath, err)
			}

			for _, resultIdEntry := range resultIdDirs {
				if resultIdEntry.IsDir() {
					resultID := resultIdEntry.Name()
					resultDirPath := filepath.Join(testPath, resultID)
					
					// The Python code reconstructs result_dirname as "test_name/result_id" for Result.
					// We pass testName explicitly to ReadResultDir.
					result, err := ReadResultDir(resultDirPath, testName)
					if err != nil {
						// Log or handle error for this specific result directory
						// For now, continue to next, or return error to be strict
						return nil, fmt.Errorf("failed to read result directory %s: %w", resultDirPath, err)
					}
					// Python uses result.result_id (which is just the basename) as key
					db.Results[result.ResultID] = *result
				}
			}
		}
	}
	return db, nil
}

// FlatDF is the Go equivalent of the Python Db.FlatDF().
// The Python version returns a pandas DataFrame.
// For Go, this will return a slice of maps or a slice of custom structs.
// For now, returning interface{} or leaving it unimplemented as per instructions.
// Let's define it to return [][]string (like a CSV) for a concrete placeholder.
func (db *Db) FlatDF() ([][]string, error) {
	// Implementation deferred.
	// Placeholder: return headers and then rows of strings.
	// Headers could be: TestName, ResultID, FactName1, FactName2, ..., MetricName1_Value, MetricName1_Unit, ...

	if len(db.Results) == 0 {
		return [][]string{}, nil
	}

	// Collect all fact names and metric names to form headers
	factNames := make(map[string]struct{})
	metricNames := make(map[string]struct{}) // Store unique metric names

	for _, result := range db.Results {
		for fn := range result.Facts {
			factNames[fn] = struct{}{}
		}
		for _, m := range result.Metrics {
			metricNames[m.Name] = struct{}{}
		}
	}

	headers := []string{"TestName", "ResultID"}
	sortedFactNames := make([]string, 0, len(factNames))
	for fn := range factNames {
		sortedFactNames = append(sortedFactNames, fn)
	}
	// Sort for consistent column order (optional, but good for stability)
	// sort.Strings(sortedFactNames) 
	headers = append(headers, sortedFactNames...)

	sortedMetricNames := make([]string, 0, len(metricNames))
	for mn := range metricNames {
		sortedMetricNames = append(sortedMetricNames, mn)
	}
	// sort.Strings(sortedMetricNames)
	for _, mn := range sortedMetricNames {
		headers = append(headers, mn+"_Value", mn+"_Unit")
	}

	var data [][]string
	data = append(data, headers)

	for _, result := range db.Results {
		row := make([]string, len(headers))
		row[0] = result.TestName
		row[1] = result.ResultID

		currentCol := 2
		for _, fn := range sortedFactNames {
			if fact, ok := result.Facts[fn]; ok {
				row[currentCol] = fmt.Sprintf("%v", fact.Value)
			} else {
				row[currentCol] = "" // Or some NA marker
			}
			currentCol++
		}

		for _, mn := range sortedMetricNames {
			foundMetric := false
			for _, m := range result.Metrics {
				if m.Name == mn {
					row[currentCol] = fmt.Sprintf("%v", m.Value)
					if m.Unit != nil {
						row[currentCol+1] = *m.Unit
					} else {
						row[currentCol+1] = ""
					}
					foundMetric = true
					break
				}
			}
			if !foundMetric {
				row[currentCol] = ""
				row[currentCol+1] = ""
			}
			currentCol += 2
		}
		data = append(data, row)
	}

	return data, nil
}

// Helper function to get unit string or empty if nil
func unitToString(unit *string) string {
	if unit != nil {
		return *unit
	}
	return ""
}

// Example of how one might load specific facts if they are stored in a known file,
// e.g., a `facts.json` within the result directory.
// This is a simplified version of what ReadResultDir now does for facts.json.
func (r *Result) LoadFactsFromJSON(filePath string) error {
	artifact, err := NewArtifact(filePath)
	if err != nil {
		// If facts.json is optional, this might not be an error.
		if os.IsNotExist(err) { 
			return nil // No facts file, not an error
		}
		return fmt.Errorf("could not create artifact for facts file %s: %w", filePath, err)
	}

	jsonData, err := artifact.JSON()
	if err != nil {
		return fmt.Errorf("could not parse facts JSON from %s: %w", filePath, err)
	}

	for key, value := range jsonData {
		// Assuming simple facts for now, without explicit "unit" field in this example
		// The main ReadResultDir has a more complex fact parsing
		var unit *string
		if valMap, ok := value.(map[string]interface{}); ok {
			if v, vOK := valMap["value"]; vOK {
				value = v
			}
			if u, uOK := valMap["unit"].(string); uOK {
				unit = &u
			}
		}
		if err := r.AddFact(Fact[any]{Name: key, Value: value, Unit: unit}); err != nil {
			return err // Error if fact already exists
		}
	}
	return nil
}

// Note: The Python __main__ block for testing is not directly translated here.
// Go testing is typically done with _test.go files.
// The `falba-db` example structure from Python:
// falba-db/
//   test_foo/
//     result123/
//       falba-facts.json  (or facts.json based on current impl)
//       some_output.txt
//       metrics.json (if metrics were also loaded from a file)
//     result456/
//       ...
//   test_bar/
//     result789/
//       ...

// Utility function to get string value from interface{}
// May not be needed if facts/metrics values are handled carefully
func getStringValue(val interface{}) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

// Python's _BaseMetric is generic, so Metric and Fact in Go are generic.
// Python's Path for Artifact is a string, Go uses string and path/filepath.
// Python's __post_init__ for Artifact path check is in NewArtifact.
// Artifact.content() -> Artifact.Content() ([]byte)
// Artifact.json() -> Artifact.JSON() (map[string]interface{})
// Result.result_dirname -> Result.TestName, Result.ResultID (split)
// Result.artifacts -> Result.Artifacts (map[string]Artifact)
// Result.facts -> Result.Facts (map[string]Fact[any])
// Result.metrics -> Result.Metrics ([]Metric[any])
// Result.ReadDir -> ReadResultDir (constructor-like)
// Result.add_fact -> Result.AddFact
// Result.add_metric -> Result.AddMetric
// Result.fact_vals -> Result.FactVals
// Db.results -> Db.Results (map[string]Result)
// Db.ReadDir -> ReadDbDir (constructor-like)
// Db.FlatDF -> Db.FlatDF (returns [][]string for now)

// FlatRecord represents a denormalized view of a metric and its associated facts.
type FlatRecord struct {
	ResultID    string
	TestName    string
	MetricName  string
	MetricValue interface{}
	MetricUnit  string
	Facts       map[string]interface{}
}

// GetFlatRecords creates a slice of FlatRecord from the Db, denormalizing metrics against facts.
func (db *Db) GetFlatRecords() []FlatRecord {
	var records []FlatRecord

	// Iterate over results. db.Results is map[string]Result.
	// The key of db.Results is result.ResultID.
	for _, result := range db.Results {
		resultFacts := result.FactVals() // This returns map[string]interface{}

		for _, metric := range result.Metrics {
			var metricUnitStr string
			if metric.Unit != nil {
				metricUnitStr = *metric.Unit
			}

			record := FlatRecord{
				ResultID:    result.ResultID, // result.ResultID is a field in Result struct
				TestName:    result.TestName,   // result.TestName is a field in Result struct
				MetricName:  metric.Name,
				MetricValue: metric.Value,
				MetricUnit:  metricUnitStr,
				Facts:       make(map[string]interface{}), // Create a new map for each record
			}

			// Deep copy facts to avoid all records sharing the same fact map instance
			if resultFacts != nil {
				for k, v := range resultFacts {
					record.Facts[k] = v // Basic shallow copy for map values; deeper copy if values are pointers/slices/maps
				}
			}
			records = append(records, record)
		}
	}
	return records
}

// EnrichWith applies a single enricher function to all relevant artifacts in the Db.
func (db *Db) EnrichWith(enricher EnricherFunc) error {
	// Iterate over results by ID to allow modification if Result is a struct value in map
	resultIDs := make([]string, 0, len(db.Results))
	for id := range db.Results {
		resultIDs = append(resultIDs, id)
	}

	for _, id := range resultIDs {
		result := db.Results[id] // Get a copy
		// Iterate over artifacts by path to allow modification if Artifact is a struct value in map
		artifactPaths := make([]string, 0, len(result.Artifacts))
		for path := range result.Artifacts {
			artifactPaths = append(artifactPaths, path)
		}

		for _, path := range artifactPaths {
			artifact := result.Artifacts[path] // Get a copy
			facts, metrics, err := enricher(artifact)
			if err != nil {
				// Consider how to handle/log errors, maybe return a list of errors
				// For now, return on first error to match provided snippet
				return fmt.Errorf("failed to enrich artifact %s with for result %s: %w", artifact.Path, result.ResultID, err)
			}
			for _, f := range facts {
				if err := result.AddFact(f); err != nil {
					// Handle or log duplicate fact errors
					// log.Printf("Warning: failed to add fact %s for result %s during enrichment: %v", f.Name, result.ResultID, err)
				}
			}
			for _, m := range metrics {
				result.AddMetric(m) // This modifies the copy of the result
			}
		}
		db.Results[id] = result // Put the modified copy back
	}
	return nil
}

// EnrichAll applies all given enricher functions to the Db.
func (db *Db) EnrichAll(allEnrichers []EnricherFunc) []error {
	var errs []error
	for _, enricher := range allEnrichers {
		// Apply enricher to a temporary Db copy or handle errors carefully
		// The current EnrichWith modifies db in place.
		if err := db.EnrichWith(enricher); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// DeriveWith applies a single deriver function to all results in the Db.
func (db *Db) DeriveWith(deriver DeriverFunc) error {
	resultIDs := make([]string, 0, len(db.Results))
	for id := range db.Results {
		resultIDs = append(resultIDs, id)
	}

	for _, id := range resultIDs {
		result := db.Results[id] // Get a copy
		facts, metrics, err := deriver(result)
		if err != nil {
			// Consider how to handle/log errors
			return fmt.Errorf("failed to derive for result %s: %w", result.ResultID, err)
		}
		for _, f := range facts {
			if err := result.AddFact(f); err != nil {
				// Handle or log duplicate fact errors
				// log.Printf("Warning: failed to add fact %s for result %s during derivation: %v", f.Name, result.ResultID, err)
			}
		}
		for _, m := range metrics {
			result.AddMetric(m)
		}
		db.Results[id] = result // Put the modified copy back
	}
	return nil
}

// DeriveAll applies all given deriver functions to the Db.
func (db *Db) DeriveAll(allDerivers []DeriverFunc) []error {
	var errs []error
	for _, deriver := range allDerivers {
		if err := db.DeriveWith(deriver); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
```
