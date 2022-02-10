package main

import (
	"crypto/sha1"
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"
)

// fakeField constructs a fake field to exercise the code.
func fakeField(full, typ string, fn func(*rdf.Statement, error)) {
	h := sha1.New()
	hash := func(s string) string {
		h.Reset()
		h.Write([]byte("package"))
		h.Write([]byte(s))
		return string(hex(h.Sum(nil)))
	}
	path := strings.Split(full, ".")
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
	hashField := hash(full)
	fn(constructTriple(`_:%s <is:published> "true" .`, hashField))
	fn(constructTriple(`_:%s <is:name> %q .`, hashField, path[len(path)-1]))
	fn(constructTriple(`_:%s <is:path> %q .`, hashField, full))
	fn(constructTriple(`_:%s <as:type> %q .`, hashField, typ))
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
