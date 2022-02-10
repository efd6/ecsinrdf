// Package schema provides tools for constructing RDF statements
// for integration package field relationships.
package integration

import (
	"crypto/sha1"
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
)

// integrationStatements calls fn on all RDF statements construct from data in the
// provided package field metadata.
//
// The graph that results has the following triples structure
//
// _:field <is:name> "name" .
// _:field <is:path> "full.dotted.path.to.name" .
// _:field <as:type> "type" .
// _:field <has:child> _:child .
// _:field <has:multi> _:multichild .
//
// Where _:child and _:multichild are have the same behaviour as _:field
// with the exception that _:multichild is only the subject of is: and as: statements.
//
// Two additional statement types exist. The <is:published> will be true for
// all fields and the <external:type> will be "ecs" for all fields defined by
// the ECS. For these fields, the path can be used to query the field information.
//
// _:field <is:published> "true" .
// _:field <external:type> "ecs" .
//
func Statements(parent string, schema []Field, fn func(*rdf.Statement, error)) {
	h := sha1.New()
	hash := func(s string) string {
		h.Reset()
		h.Write([]byte("package"))
		h.Write([]byte(s))
		return string(hex(h.Sum(nil)))
	}
	for _, props := range schema {
		if parent != "" {
			props.Name = parent + "." + props.Name
		}
		Statements(props.Name, props.Fields, fn)

		path := strings.Split(props.Name, ".")
		for i := range path[1:] {
			sub := strings.Join(path[:i+1], ".")
			hashSub := hash(sub)
			obj := strings.Join(path[:i+2], ".")
			hashObj := hash(obj)
			fn(constructTriple(`_:%s <is:published> "true" .`, hashSub))
			fn(constructTriple(`_:%s <as:type> "group" .`, hashSub))
			fn(constructTriple(`_:%s <is:name> %q .`, hashSub, path[i]))
			fn(constructTriple(`_:%s <is:path> %q .`, hashSub, sub))
			fn(constructTriple(`_:%s <has:child> _:%s .`, hashSub, hashObj))
		}
		hashField := hash(props.Name)
		fn(constructTriple(`_:%s <is:published> "true" .`, hashField))
		fn(constructTriple(`_:%s <is:name> %q .`, hashField, path[len(path)-1]))
		fn(constructTriple(`_:%s <is:path> %q .`, hashField, props.Name))
		if props.External != "" {
			fn(constructTriple(`_:%s <external:type> %q .`, hashField, props.External))
		}
		if props.Type != "" {
			fn(constructTriple(`_:%s <as:type> %q .`, hashField, props.Type))
		}
		for _, m := range props.MultiFields {
			hashSub := hash(m.Name)
			flatName := props.Name + "." + m.Name
			hashFlat := hash(flatName)
			fn(constructTriple(`_:%s <has:multi> _:%s .`, hashSub, hashFlat))
			fn(constructTriple(`_:%s <is:published> "true" .`, hashFlat))
			fn(constructTriple(`_:%s <as:type> %q .`, hashFlat, m.Type))
			fn(constructTriple(`_:%s <is:name> %q .`, hashFlat, m.Name))
			fn(constructTriple(`_:%s <is:path> %q .`, hashFlat, flatName))
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

type Field struct {
	Name           string       `yaml:"name"`
	Type           string       `yaml:"type"`
	Description    string       `yaml:"description"`
	Format         string       `yaml:"format"`
	Fields         []Field      `yaml:"fields"`
	MultiFields    []MultiField `yaml:"multi_fields"`
	Enabled        *bool        `yaml:"enabled"`
	Analyzer       string       `yaml:"analyzer"`
	SearchAnalyzer string       `yaml:"search_analyzer"`
	Norms          bool         `yaml:"norms"`
	Dynamic        string       `yaml:"dynamic"`
	Index          *bool        `yaml:"index"`
	DocValues      *bool        `yaml:"doc_values"`
	CopyTo         string       `yaml:"copy_to"`
	IgnoreAbove    int          `yaml:"ignore_above"`
	Path           string       `yaml:"path"`
	MigrationAlias bool         `yaml:"migration"`
	Dimension      *bool        `yaml:"dimension"`

	// DynamicTemplate controls whether this field represents an explicitly
	// named dynamic template.
	//
	// Such dynamic templates are only suitable for use in dynamic_template
	// parameter in bulk requests or in ingest pipelines, as they will have
	// no path or type match criteria.
	DynamicTemplate bool `yaml:"dynamic_template"`

	// Unit holds a standard unit for numeric fields: "percent", "byte", or a time unit.
	// See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-field-meta.html.
	Unit string `yaml:"unit"`

	// MetricType holds a standard metric type for numeric fields: "gauge" or "counter".
	// See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-field-meta.html.
	MetricType string `yaml:"metric_type"`

	ObjectType            string          `yaml:"object_type"`
	ObjectTypeMappingType string          `yaml:"object_type_mapping_type"`
	ScalingFactor         int             `yaml:"scaling_factor"`
	ObjectTypeParams      []ObjectTypeCfg `yaml:"object_type_params"`

	// Kibana specific
	Analyzed     *bool  `yaml:"analyzed"`
	Count        int    `yaml:"count"`
	Searchable   *bool  `yaml:"searchable"`
	Aggregatable *bool  `yaml:"aggregatable"`
	Script       string `yaml:"script"`

	// Kibana params
	Pattern              string   `yaml:"pattern"`
	InputFormat          string   `yaml:"input_format"`
	OutputFormat         string   `yaml:"output_format"`
	OutputPrecision      *int     `yaml:"output_precision"`
	LabelTemplate        string   `yaml:"label_template"`
	UrlTemplate          []string `yaml:"url_template"`
	OpenLinkInCurrentTab *bool    `yaml:"open_link_in_current_tab"`

	// Incorrectly here.
	Overwrite    bool  `yaml:"overwrite"`
	DefaultField *bool `yaml:"default_field"`

	// Added

	External string `yaml:"external,omitempty"`
	// ECS Level of maturity of the field.
	Level string `yaml:"level,omitempty"`
	// An example of what can be expected
	// in this field.
	Example interface{} `yaml:"example,omitempty"` // inconsistent with ECS
	// Capitalized name of the field set.
	Title string `yaml:"title,omitempty"`
	// Group is used to sort field sets against one another.
	Group    int    `yaml:"group,omitempty"`
	Value    string `yaml:"value,omitempty"`
	Footnote string `yaml:"footnote,omitempty"`
	Release  string `yaml:"release,omitempty"`
	Required bool   `yaml:"required,omitempty"`
	// Same as AllowedValues in Properties?
	PossibleValues []string `yaml:"possible_values,omitempty"`
	// The version when the field was deprecated.
	Deprecated string `yaml:"deprecated,omitempty"`
	// Same as Prefix in Properties?
	Prefix string `yaml:"prefix,omitempty"`

	// typos
	DefaultFields bool   `yaml:"default_fields,omitempty"`
	Descriiption  string `yaml:"descriiption,omitempty"`
	Descripion    string `yaml:"descripion,omitempty"`
	Dimensions    bool   `yaml:"dimensions,omitempty"`
	Dimensiont    bool   `yaml:"dimensiont,omitempty"`
}

type MultiField struct {
	// Type of the multi_fields.
	Type string `yaml:"type"`
	// Name of the multi_fields.
	Name string `yaml:"name"`

	// Introduded in https://github.com/elastic/ecs/pull/336
	FlatName string `yaml:"flat_name"`

	// Field that only exist in integrations field descriptions.
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html
	Norms        bool   `yaml:"norms"`
	DefaultField bool   `yaml:"default_field"`
	Analyzer     string `yaml:"analyzer,omitempty"`
}

// ObjectTypeCfg defines type and configuration of object attributes
type ObjectTypeCfg struct {
	ObjectType            string `yaml:"object_type"`
	ObjectTypeMappingType string `yaml:"object_type_mapping_type"`
	ScalingFactor         int    `yaml:"scaling_factor"`
}
