package main

import (
	"bytes"
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
	"gopkg.in/yaml.v3"
)

func main() {
	root := flag.String("ecs-root", "", "specify the path to the root of the ecs repo")
	version := flag.String("version", "", "specify the version of ECS to use (tag, branch or sha)")
	flag.Parse()
	if *root == "" || *version == "" {
		flag.Usage()
		os.Exit(2)
	}

	ecs, err := ecsSpec(*root, *version)
	if err != nil {
		log.Fatal(err)
	}

	var statements []*rdf.Statement
	dec := yaml.NewDecoder(ecs)
	dec.KnownFields(true)
	for {
		var f map[string]Properties
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		schemaGraph("", f, func(s *rdf.Statement, err error) {
			if err != nil {
				log.Println(err)
				return
			}
			statements = append(statements, s)
		})
	}

	dec = yaml.NewDecoder(os.Stdin)
	dec.KnownFields(true)
	for {
		var f []PackageField
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		packageGraph("", f, func(s *rdf.Statement, err error) {
			if err != nil {
				log.Println(err)
				return
			}
			statements = append(statements, s)
		})
	}

	statements, err = rdf.URDNA2015(statements, statements)
	if err != nil {
		log.Fatal(err)
	}
	statements = rdf.Deduplicate(statements)
	for _, s := range statements {
		fmt.Println(s)
	}
}

// schemaGraph calls fn on all RDF statements construct from data in the
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
// schemaGraph assumes the yaml field keys are always full dotted paths.
func schemaGraph(parent string, schema map[string]Properties, fn func(*rdf.Statement, error)) {
	h := sha1.New()
	hash := func(s string) string {
		h.Reset()
		h.Write([]byte("schema"))
		h.Write([]byte(s))
		return string(hex(h.Sum(nil)))
	}
	for field, props := range schema {
		schemaGraph(field, props.Fields, fn)
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

// packageGraph calls fn on all RDF statements construct from data in the
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
func packageGraph(parent string, schema []PackageField, fn func(*rdf.Statement, error)) {
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
		packageGraph(props.Name, props.Fields, fn)

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

const nestedPath = "generated/ecs/ecs_nested.yml"

func ecsSpec(path, version string) (io.Reader, error) {
	cmd := exec.Command("git", "show", fmt.Sprintf("%s:"+nestedPath, version))
	cmd.Dir = path
	var buf bytes.Buffer
	cmd.Stdout = &buf
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

// See https://github.com/elastic/ecs/blob/main/schemas/README.md
type Properties struct {
	// Name of the field set.
	Name string `yaml:"name"`
	// ECS Level of maturity of the field.
	Level string `yaml:"level"`
	// Capitalized name of the field set.
	Title string `yaml:"title"`
	// Description of the field set.
	Description string `yaml:"description"`
	// Fields is the list of fields in the field set.
	Fields map[string]Properties `yaml:"fields,omitempty"`

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

	// Field that only exist in integrations field descriptions.
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html
	Norms        bool   `yaml:"norms"`
	DefaultField bool   `yaml:"default_field"`
	Analyzer     string `yaml:"analyzer,omitempty"`
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

// The same thing but for packages.

type PackageField struct {
	Name           string         `yaml:"name"`
	Type           string         `yaml:"type"`
	Description    string         `yaml:"description"`
	Format         string         `yaml:"format"`
	Fields         []PackageField `yaml:"fields"`
	MultiFields    []MultiField   `yaml:"multi_fields"`
	Enabled        *bool          `yaml:"enabled"`
	Analyzer       string         `yaml:"analyzer"`
	SearchAnalyzer string         `yaml:"search_analyzer"`
	Norms          bool           `yaml:"norms"`
	Dynamic        string         `yaml:"dynamic"`
	Index          *bool          `yaml:"index"`
	DocValues      *bool          `yaml:"doc_values"`
	CopyTo         string         `yaml:"copy_to"`
	IgnoreAbove    int            `yaml:"ignore_above"`
	Path           string         `yaml:"path"`
	MigrationAlias bool           `yaml:"migration"`
	Dimension      *bool          `yaml:"dimension"`

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

// ObjectTypeCfg defines type and configuration of object attributes
type ObjectTypeCfg struct {
	ObjectType            string `yaml:"object_type"`
	ObjectTypeMappingType string `yaml:"object_type_mapping_type"`
	ScalingFactor         int    `yaml:"scaling_factor"`
}
