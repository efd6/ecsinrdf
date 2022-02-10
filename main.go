package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"gonum.org/v1/gonum/graph/formats/rdf"
	"gopkg.in/yaml.v3"

	"github.com/efd6/ecsinrdf/integration"
	"github.com/efd6/ecsinrdf/query"
	"github.com/efd6/ecsinrdf/schema"
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
		var f map[string]schema.Field
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		schema.Statements("", f, func(s *rdf.Statement, err error) {
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
		var f []integration.Field
		err := dec.Decode(&f)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		integration.Statements("", f, func(s *rdf.Statement, err error) {
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
	g := rdf.NewGraph()
	for _, s := range statements {
		g.AddStatement(s)
	}

	// threat.enrichments.indicator.registry.path
	fmt.Println(query.CandidateGraftsFor(g, "foo_package.indicator.registry.path", "keyword"))
	fmt.Println(query.CandidateGraftsFor(g, "foo_package.registry.path", "keyword"))
	fmt.Println(query.CandidateGraftsFor(g, "foo_package.registry.path", "text"))

	fakeField("foo_package.indicator.registry.path", "keyword", func(s *rdf.Statement, err error) {
		if err != nil {
			return
		}
		g.AddStatement(s)
	})

	fmt.Println(query.CandidateGraftsFor(g, "foo_package.indicator.registry.path", "keyword"))
	fmt.Println(query.CandidateGraftsFor(g, "foo_package.registry.path", "keyword"))
	fmt.Println(query.CandidateGraftsFor(g, "foo_package.registry.path", "text"))

	fmt.Println(query.CandidateGraftsIn(g, "foo_package.indicator.registry.path"))
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
