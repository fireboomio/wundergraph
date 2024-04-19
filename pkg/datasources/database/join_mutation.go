package database

import (
	"bytes"
	"github.com/buger/jsonparser"
	"strings"
)

const (
	fieldQueryName         = "query"
	mutationUpperFirstName = "Mutation"
	joinMutationPrefix     = "_join_mutation."
)

var (
	mutationBytes    = []byte("mutation")
	mutationBytesLen = len(mutationBytes)
)

func (s *Source) ensureMutationPrefix(request []byte) []byte {
	if s.rootTypeName != mutationUpperFirstName {
		return request
	}

	queryBytes, _, _, _ := jsonparser.Get(request, fieldQueryName)
	if bytes.HasPrefix(queryBytes, mutationBytes) {
		return request
	}

	if !strings.HasPrefix(s.datasourceName, joinMutationPrefix) {
		s.datasourceName = joinMutationPrefix + s.datasourceName
	}
	newQueryBytesLen := mutationBytesLen + len(queryBytes)
	newQueryBytes := make([]byte, newQueryBytesLen+2)
	copy(newQueryBytes[1:], mutationBytes)
	copy(newQueryBytes[mutationBytesLen+1:], queryBytes)
	newQueryBytes[0], newQueryBytes[newQueryBytesLen+1] = '"', '"'
	request, _ = jsonparser.Set(request, newQueryBytes, fieldQueryName)
	return request
}
