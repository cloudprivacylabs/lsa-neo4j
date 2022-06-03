package neo4j

import (
	"strings"
)

// Node represents each node in the trie
type TrieNode struct {
	children   map[rune]*TrieNode
	isTerminal bool
	mapping    string
}

// Trie represents a trie and has a pointer to the root node
type Trie struct {
	root *TrieNode
}

// InitTrie will create a new Trie
func InitTrie() *Trie {
	result := &Trie{root: &TrieNode{
		children: make(map[rune]*TrieNode),
	}}
	return result
}

// Insert will take in a word and add it to the trie
func (t *Trie) Insert(word, mapping string) {
	currentNode := t.root
	for _, ch := range word {
		if _, exists := currentNode.children[ch]; !exists {
			newChild := &TrieNode{children: make(map[rune]*TrieNode)}
			currentNode.children[ch] = newChild
			currentNode = newChild
		} else {
			currentNode = currentNode.children[ch]
		}
	}
	currentNode.mapping = mapping
	currentNode.isTerminal = true
}

// Search will search if a word is in the trie
func (t *Trie) Search(word string) (string, string, bool) {
	currentNode := t.root
	prefix := strings.Builder{}
	var lastPrefixMapping string
	var lastPrefix string
	var lastPrefixFlag bool
	for _, ch := range word {
		if _, ok := currentNode.children[ch]; !ok {
			return lastPrefix, lastPrefixMapping, lastPrefixFlag
		}
		currentNode = currentNode.children[ch]
		prefix.WriteRune(ch)
		if currentNode.isTerminal {
			lastPrefix = prefix.String()
			lastPrefixMapping = currentNode.mapping
			lastPrefixFlag = true
		}
	}
	return lastPrefix, lastPrefixMapping, lastPrefixFlag
}
