package pkg

import (
	"fmt"
	"slices"
	"strings"
)

type Trie2Node struct {
	Key          byte
	Value        *CityData
	ChildrenKeys []byte
	Children     []*Trie2Node
}

func MakeNode(key byte, value *CityData) *Trie2Node {
	return &Trie2Node{
		Key:   key,
		Value: value,
	}
}

func (root Trie2Node) String() string {
	var sb strings.Builder
	for _, n := range root.Children {
		sb.WriteByte(root.Key)
		sb.WriteString(" -> ")
		sb.WriteByte(n.Key)
		if n.Value != nil {
			sb.WriteString(" {")
			fmt.Fprint(&sb, *n.Value)
			sb.WriteString("} ")
		}
		sb.WriteString("\n")
		sb.WriteString(n.String())

	}
	return sb.String()
}

func (root *Trie2Node) Insert(key []byte, value *CityData) {
	for _, c := range key {
		i, ok := slices.BinarySearch(root.ChildrenKeys, c)
		if ok {
			root = root.Children[i]
			continue
		}

		root.ChildrenKeys = slices.Insert(root.ChildrenKeys, i, c)
		root.Children = slices.Insert(root.Children, i, MakeNode(c, nil))
		root = root.Children[i]
	}

	if root.Value == nil {
		root.Value = value
		return
	}

	root.Value.Merge(value)
}

func (root *Trie2Node) Get(key []byte) *CityData {
	if len(key) == 0 {
		return root.Value
	}

	for _, c := range key {
		i, ok := slices.BinarySearch(root.ChildrenKeys, c)
		if ok {
			root = root.Children[i]
			continue
		}

		return nil
	}

	return root.Value
}

func (root *Trie2Node) Iter(prefix []byte, f func(key []byte, value *CityData)) {
	if root.Value != nil {
		f(append(prefix, root.Key), root.Value)
	}

	for _, c := range root.Children {
		c.Iter(append(prefix, root.Key), f)
	}
} 
