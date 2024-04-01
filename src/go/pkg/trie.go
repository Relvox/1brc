package pkg

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

type TrieNode struct {
	Key      []byte
	Value    *CityData
	Children []*TrieNode
}

func (n TrieNode) String() string {
	var sb strings.Builder
	for _, c := range n.Children {
		sb.Write(n.Key)
		sb.WriteString(" -> ")
		sb.Write(c.Key)
		if c.Value != nil {
			sb.WriteString(" {")
			fmt.Fprint(&sb, *c.Value)
			sb.WriteString("} ")
		}
		sb.WriteString("\n")
		sb.WriteString(c.String())
	}

	return sb.String()
}

func MakeRoot() *TrieNode {
	return &TrieNode{
		Key:      []byte{},
		Value:    nil,
		Children: make([]*TrieNode, 0),
	}
}

func (root *TrieNode) Get(key []byte) *CityData {
	if bytes.Equal(key, root.Key) {
		return root.Value
	}

	key = key[len(root.Key):]
	lenKey := len(key)
	for _, c := range root.Children {
		lenCKey := len(c.Key)
		if lenKey < lenCKey {
			continue
		}
		if len(commonPrefix(c.Key, key)) == lenCKey {
			return c.Get(key)
		}
	}

	return nil
}

func (root *TrieNode) Insert(key []byte, value *CityData) {
	key = key[len(root.Key):]
	if len(key) == 0 {
		if root.Value == nil {
			root.Value = value
			return
		}
		root.Value.Merge(value)
		return
	}

	if len(root.Children) == 0 {
		root.Children = append(root.Children, &TrieNode{
			Key:      key,
			Value:    value,
			Children: make([]*TrieNode, 0),
		})

		return
	}

	for ci, c := range root.Children {
		prefix := commonPrefix(c.Key, key)
		lenCKey, lenPrefix := len(c.Key), len(prefix)
		if lenPrefix == lenCKey {
			c.Insert(key, value)
			return
		}

		if lenPrefix == 0 {
			cmp := bytes.Compare(key[:lenCKey], c.Key)
			if cmp < 0 {
				root.Children = slices.Insert(root.Children, ci, &TrieNode{
					Key:      key,
					Value:    value,
					Children: make([]*TrieNode, 0),
				})

				return
			}

			continue
		}

		c.Key = c.Key[lenPrefix:]
		newNode := &TrieNode{Key: prefix, Children: []*TrieNode{c}}
		root.Children[ci] = newNode
		newNode.Insert(key, value)
		return
	}

	root.Children = append(root.Children, &TrieNode{
		Key:      key,
		Value:    value,
		Children: make([]*TrieNode, 0),
	})
}

func (root *TrieNode) Iter(prefix []byte, f func(key []byte, value *CityData)) {
	if root.Value != nil {
		f(append(prefix, root.Key...), root.Value)
	}

	for _, c := range root.Children {
		c.Iter(append(prefix, root.Key...), f)
	}
}

func commonPrefix(b1, b2 []byte) []byte {
	end := min(len(b1), len(b2))
	for i := range end {
		if b1[i] == b2[i] {
			continue
		}

		return b1[:i]
	}
	return b1[:end]
}
