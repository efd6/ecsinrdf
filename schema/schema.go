// Package schema provides tools for constructing RDF statements
// for ECS field relationships.
package schema

import (
	"crypto/sha1"
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
)

// Statements calls fn on all RDF statements construct from data in the
// provided schema.
//
// The graph that results has the following triples structure
//
// _:field <is:name> "name" .
// _:field <is:path> "full.dotted.path.to.name" .
// _:field <is:type> "type" .
// _:field <has:child> _:child .
// _:field <has:multi> _:multichild .
//
// Where _:child and _:multichild are have the same behaviour as _:field
// with the exception that _:multichild is only the subject of is: statements.
//
// Statements assumes the yaml field keys are always full dotted paths.
func Statements(parent string, schema map[string]Field, fn func(*rdf.Statement, error)) {
	h := sha1.New()
	hash := func(s string) string {
		h.Reset()
		h.Write([]byte("schema"))
		h.Write([]byte(s))
		return string(hex(h.Sum(nil)))
	}
	for field, props := range schema {
		Statements(field, props.Fields, fn)
		if parent == "" {
			continue
		}

		path := strings.Split(field, ".")
		for i := range path[1:] {
			sub := strings.Join(path[:i+1], ".")
			hashSub := hash(sub)
			obj := strings.Join(path[:i+2], ".")
			hashObj := hash(obj)
			fn(constructTriple(`_:%s <is:type> "group" .`, hashSub))
			fn(constructTriple(`_:%s <is:name> %q .`, hashSub, path[i]))
			fn(constructTriple(`_:%s <is:path> %q .`, hashSub, sub))
			fn(constructTriple(`_:%s <has:child> _:%s .`, hashSub, hashObj))
		}
		hashField := hash(field)
		fn(constructTriple(`_:%s <is:type> %q .`, hashField, props.Type))
		fn(constructTriple(`_:%s <is:name> %q .`, hashField, path[len(path)-1]))
		fn(constructTriple(`_:%s <is:path> %q .`, hashField, field))
		for _, m := range props.MultiFields {
			sub := m.FlatName[:strings.LastIndex(m.FlatName, ".")]
			hashSub := hash(sub)
			hashFlat := hash(m.FlatName)
			fn(constructTriple(`_:%s <has:multi> _:%s .`, hashSub, hashFlat))
			fn(constructTriple(`_:%s <is:type> %q .`, hashFlat, m.Type))
			fn(constructTriple(`_:%s <is:name> %q .`, hashFlat, m.Name))
			fn(constructTriple(`_:%s <is:path> %q .`, hashFlat, m.FlatName))
		}
	}
}

func hex(data []byte) []byte {
	const digit = "0123456789abcdef"
	buf := make([]byte, 0, len(data)*2)
	for _, b := range data {
		buf = append(buf, digit[b>>4], digit[b&0xf])
	}
	return buf
}

func constructTriple(format string, a ...interface{}) (*rdf.Statement, error) {
	formatted := fmt.Sprintf(format, a...)
	s, err := rdf.ParseNQuad(formatted)
	if err != nil {
		return nil, fmt.Errorf("%#q: %v", formatted, err)
	}
	return s, nil
}

// See https://github.com/elastic/ecs/blob/main/schemas/README.md
type Field struct {
	// Name of the field set.
	Name string `yaml:"name"`
	// ECS Level of maturity of the field.
	Level string `yaml:"level"`
	// Capitalized name of the field set.
	Title string `yaml:"title"`
	// Description of the field set.
	Description string `yaml:"description"`
	// Fields is the list of fields in the field set.
	Fields map[string]Field `yaml:"fields,omitempty"`

	// Short version of the description to display
	// in small spaces.
	Short string `yaml:"short"`
	// Whether or not the fields of this field set
	// should be namespaced under the field set name.
	Root bool `yaml:"root"`
	// Group is used to sort field sets against one another.
	Group int `yaml:"group"`
	// Type of the field.
	Type string `yaml:"type"`
	// Used to identify which field sets are expected
	// to be reused in multiple places.
	Reusable Reusable `yaml:"reusable"`
	// Adds a beta marker for the entire fieldset.
	// The text provided in this attribute is used as
	// content of the beta marker in the documentation.
	Beta string `yaml:"beta"`

	// AllowedValues is a list of dictionaries with
	// the name and description of expected values.
	AllowedValues []AllowedValues `yaml:"allowed_values,omitempty"`
	// A single value example of what can be expected
	// in this field.
	Example string `yaml:"example"`
	// Field format that can be used in a Kibana index template.
	Format string `yaml:"format"`
	// Index specifies whether the field is indexed.
	Index *bool `yaml:"index"`
	// MultiFields specifies additional ways to
	// index the field.
	MultiFields []MultiField `yaml:"multi_fields,omitempty"`
	// Normalization steps that should be applied at ingestion time.
	Normalize []string `yaml:"normalize,omitempty"`

	// In initial commit.
	Required    *bool `yaml:"required"`
	IgnoreAbove int   `yaml:"ignore_above"`

	// Introduded in https://github.com/elastic/ecs/pull/16?
	DocValues *bool `yaml:"doc_values"`

	// Introduced in https://github.com/elastic/ecs/pull/20
	Footnote string `yaml:"footnote"`

	// Introduced in https://github.com/elastic/ecs/pull/347
	// and https://github.com/elastic/ecs/pull/336?
	Nestings []string `yaml:"nestings,omitempty"`

	// Introduced in https://github.com/elastic/ecs/pull/336
	OriginalFieldset string `yaml:"original_fieldset"`
	FlatName         string `yaml:"flat_name"`
	Prefix           string `yaml:"prefix"`

	// Introduced in https://github.com/elastic/ecs/pull/385
	InputFormat string `yaml:"input_format"`

	// Introduced in https://github.com/elastic/ecs/pull/425
	OutputFormat    string `yaml:"output_format"`
	OutputPrecision int    `yaml:"output_precision"`

	// Introduced in https://github.com/elastic/ecs/pull/684
	DashedName string `yaml:"dashed_name"`

	// Introduced in https://github.com/elastic/ecs/pull/864
	ReusedHere []ReusedHere `yaml:"reused_here,omitempty"`

	// In initial commit.
	ObjectType string `yaml:"object_type"`

	// A scaled_float has an additional parameter, the scaling_factor.
	// scaling_factor is always required for a scaled_float.
	// Described in https://github.com/elastic/ecs/pull/1250
	// Introduded in https://github.com/elastic/ecs/pull/1028
	ScalingFactor int `yaml:"scaling_factor"`
}

type AllowedValues struct {
	// Name of the allowed_values.
	Name string `yaml:"name"`
	// Description of the allowed_values.
	Description string `yaml:"description"`
	// ExpectedEventTypes is a list of expected
	// event.type values to use in association
	// with a category.
	ExpectedEventTypes []string `yaml:"expected_event_types,omitempty"`
	// Adds a beta marker for the allowed_values.
	// The text provided in this attribute is used as
	// content of the beta marker in the documentation.
	Beta string `yaml:"beta,omitempty"`
}

type MultiField struct {
	// Type of the multi_fields.
	Type string `yaml:"type"`
	// Name of the multi_fields.
	Name string `yaml:"name"`

	// Introduded in https://github.com/elastic/ecs/pull/336
	FlatName string `yaml:"flat_name"`
}

type Reusable struct {
	// Specifies whether the  field set is expected
	// at the root of events or only expected in
	// nested locations. Nil should be interpeted
	// as true.
	TopLevel *bool `yaml:"top_level"`
	// Expected is the list of places the field set's
	// fields are expected.
	Expected []Expected `yaml:"expected,omitempty"`
}

type Expected struct {
	// Concept introduced in https://github.com/elastic/ecs/pull/152
	// field in https://github.com/elastic/ecs/pull/864
	At string `yaml:"at"`

	// Introduced in https://github.com/elastic/ecs/pull/864
	As   string `yaml:"as"`
	Full string `yaml:"full"`

	// ShortOverride sets the short description for the
	// nested field, overriding the default top-level
	// short description
	ShortOverride string `yaml:"short_override"`
	// Beta marks the specific reuse locations as beta.
	Beta string `yaml:"beta"`
}

type ReusedHere struct {
	// Introduced in https://github.com/elastic/ecs/pull/864
	Full       string `yaml:"full"`
	SchemaName string `yaml:"schema_name"`

	// Short version of the description to display
	// in small spaces.
	Short string `yaml:"short"`
	// Beta marks the specific reuse locations as beta.
	Beta string `yaml:"beta"`
}
