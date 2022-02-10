// Package query provides graph queries for ECS in RDF.
package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
)

// CandidateGrafts returns a list of potential ECS graft candidate
// destinations for the field with the provided full path. The field
// must already be in the the graph. Candidates will have the same type
// as the query field and will have matching path suffixes.
//
// The graph g is expected to be an ECS graph with statements relating
// to the ECS and package field constructed by the schema and integration
// packages in this repo.
func CandidateGraftsIn(g *rdf.Graph, full string) ([]string, error) {
	path := strings.Split(full, ".")
	node, ok := g.TermFor(strconv.Quote(full))
	if !ok {
		return nil, errors.New("not found")
	}

	// Select nodes that that are the right full path.
	q := g.Query(node).In(byPath)
	// Confirm it is published and get its type. There should be exactly one.
	typs := q.And(q.Out(isPublished).In(isPublished)).Out(byUsedType).Unique().Result()
	switch len(typs) {
	case 0:
		return nil, errors.New("no type")
	case 1:
	default:
		return nil, fmt.Errorf("found multiple types: %v", typs)
	}

	// Get all the other nodes with the same name.
	q = q.Out(byName).In(byName).Not(q)

	// Walk the path.
	paths := walkMatchingPath(q, typs[0], path)
	return paths, nil
}

// CandidateGraftsFor returns a list of potential ECS graft candidate
// destinations for the field with the provided full path and typ.
// Candidates will have the same type as the query field and will have
// matching path suffixes.
//
// The graph g is expected to be an ECS graph with statements relating
// to the ECS field constructed by the schema packages in this repo.
// It may contain statements relating to integration fields.
func CandidateGraftsFor(g *rdf.Graph, full, typ string) ([]string, error) {
	path := strings.Split(full, ".")
	node, ok := g.TermFor(strconv.Quote(path[len(path)-1]))
	if !ok {
		return nil, errors.New("path not found")
	}
	quotedTyp := strconv.Quote(typ)

	// Select nodes that that are the right name.
	q := g.Query(node).In(byName)
	// Get the typ node.
	typs, ok := g.TermFor(quotedTyp)
	if !ok {
		return nil, errors.New("typ not found")
	}

	// Walk the path.
	paths := walkMatchingPath(q, typs, path)
	return paths, nil
}

func walkMatchingPath(q rdf.Query, typ rdf.Term, path []string) []string {
	// Filter start by type.
	matchingType := func(s *rdf.Statement) bool {
		return s.Predicate.Value == "<is:type>" && s.Object.Value == typ.Value
	}
	q = q.And(q.Out(matchingType).In(matchingType))

	// Walk the path.
	var final []rdf.Term
	for i := len(path) - 2; i >= 0; i-- {
		quotedName := strconv.Quote(path[i])

		c := q.In(hasChild)
		q = c.Out(func(s *rdf.Statement) bool {
			return s.Predicate.Value == "<is:name>" && s.Object.Value == quotedName
		}).In(byName).And(c)

		r := q.Out(byPath).Unique().Result()
		if len(r) == 0 {
			break
		}
		final = r
	}

	// Collate the results.
	paths := make([]string, len(final))
	for i, v := range final {
		paths[i] = v.Value
	}
	return paths
}

// Predicate helpers.

// byUsedType filters statements on the used type.
func byUsedType(s *rdf.Statement) bool {
	return s.Predicate.Value == "<as:type>"
}

// isPublished filters statements on the published attribute.
func isPublished(s *rdf.Statement) bool {
	return s.Predicate.Value == "<is:published>" && s.Object.Value == `"true"`
}

// byName filters statements referring to name.
func byName(s *rdf.Statement) bool {
	return s.Predicate.Value == "<is:name>"
}

// byPath filters statements referring to path.
func byPath(s *rdf.Statement) bool {
	return s.Predicate.Value == "<is:path>"
}

// hasChild filters statements referring to path relationships.
func hasChild(s *rdf.Statement) bool {
	return s.Predicate.Value == "<has:child>"
}
