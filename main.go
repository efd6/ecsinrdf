package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
	"gopkg.in/yaml.v3"

	"github.com/efd6/ecsinrdf/integration"
	"github.com/efd6/ecsinrdf/query"
	"github.com/efd6/ecsinrdf/schema"
)

func main() {
	qry := flag.String("query", "", "specify a field path and type query path.to.field:type")
	pkg := flag.String("pkg-path", ".", "specify the path to the root of the package(s) (ignored if query is not empty)")
	root := flag.String("ecs-root", "", "specify the path to the root of the ecs repo")
	version := flag.String("version", "", "specify the version of ECS to use (tag, branch or sha)")
	flag.Parse()

	if *root == "" || *version == "" || (*qry != "" && len(strings.Split(*qry, ":")) != 2) {
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

	if *qry == "" {
		fr, err := fieldsReader(*pkg)
		if err != nil {
			log.Fatal(err)
		}
		defer fr.Close()

		dec = yaml.NewDecoder(fr)
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

	if *qry != "" {
		parts := strings.Split(*qry, ":")
		if len(parts) != 2 {
			flag.Usage()
			os.Exit(2)
		}
		cands, err := query.CandidateGraftsFor(g, strconv.Quote(parts[0]), strconv.Quote(parts[1]))
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(cands)
		return
	}

	// Do some actual work.
	notGroup := func(s *rdf.Statement) bool {
		return s.Predicate.Value == "<as:type>" && s.Object.Value != `"group"`
	}
	p := query.PublishedFieldsIn(g)
	p = p.Out(notGroup).In(notGroup).And(p)
	for _, f := range p.Result() {
		paths := g.Query(f).Out(func(s *rdf.Statement) bool {
			return s.Predicate.Value == "<is:path>"
		})
		for _, n := range paths.Result() {
			cands, err := query.CandidateGraftsIn(g, n.Value)
			if len(cands) != 0 || err != nil {
				fmt.Printf("%s\n", n.Value)
			}
			if err != nil {
				fmt.Printf("\t%s: %v\n", n.Value, err)
			}
			for _, c := range cands {
				fmt.Printf("\t%s\n", c)
			}
			if len(cands) != 0 || err != nil {
				fmt.Println()
			}
		}
	}
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

func fieldsReader(path string) (io.ReadCloser, error) {
	var (
		readers []io.Reader
		mr      multiReaderCloser
	)

	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".yml" {
			return nil
		}
		if filepath.Base(filepath.Dir(path)) != "fields" {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		mr.files = append(mr.files, f)
		readers = append(readers, f)
		return nil
	})
	if err != nil {
		mr.Close()
		return nil, err
	}

	mr.Reader = io.MultiReader(readers...)

	return &mr, nil
}

type multiReaderCloser struct {
	io.Reader
	files []*os.File
}

func (mr *multiReaderCloser) Close() error {
	var err error
	for _, f := range mr.files {
		e := f.Close()
		if err != nil {
			err = e
		}
	}
	return err
}
