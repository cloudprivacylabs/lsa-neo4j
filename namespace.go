package neo4j

import (
	"strings"
)

// Node represents each node in the trie
type TrieNode struct {
	children map[rune]*TrieNode
	mapping  string
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
}

// Search will search if a word is in the trie
func (t *Trie) Search(word string) (string, string, bool) {
	currentNode := t.root
	prefix := strings.Builder{}
	var lastPrefixMatch string
	for _, ch := range word {
		if _, ok := currentNode.children[ch]; !ok {
			return prefix.String(), lastPrefixMatch, true
		}
		prefix.WriteRune(ch)
		currentNode = currentNode.children[ch]
		lastPrefixMatch = currentNode.mapping
	}
	if len(currentNode.mapping) > 0 {
		return word, currentNode.mapping, true
	}
	return word, "", false
}

// // Returns a list of all words that share a common prefix
// func (t *Trie) AllWordsFromPrefix(prefix string) []string {
// 	var wordsFromPrefix func(string, *TrieNode, *[]string)
// 	wordsFromPrefix = func(prefix string, node *TrieNode, words *[]string) {
// 		if node.isWord {
// 			*words = append(*words, prefix)
// 		}
// 		if len(node.children) == 0 {
// 			return
// 		}
// 		for ch := range node.children {
// 			wordsFromPrefix(prefix+string(ch), node.children[ch], words)
// 		}
// 	}
// 	words := make([]string, 0)
// 	current := t.root
// 	for _, ch := range prefix {
// 		next := current.children[ch]
// 		if next == nil {
// 			return words
// 		}
// 		current = next
// 	}
// 	if len(current.children) > 0 {
// 		wordsFromPrefix(prefix, current, &words)
// 	} else {
// 		if current.isWord {
// 			words = append(words, prefix)
// 		}
// 	}
// 	sort.Slice(words, func(i, j int) bool {
// 		return len(words[i]) > len(words[j])
// 	})
// 	return words
// }
