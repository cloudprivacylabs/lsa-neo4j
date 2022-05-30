package neo4j

import "sort"

// AlphabetSize is the number of possible characters in the trie
const AlphabetSize = 26

// Node represents each node in the trie
type TrieNode struct {
	// children [AlphabetSize]*TrieNode
	children map[string]*TrieNode
	isWord   bool
}

// Trie represents a trie and has a pointer to the root node
type Trie struct {
	root *TrieNode
}

// InitTrie will create a new Trie
func InitTrie() *Trie {
	result := &Trie{root: &TrieNode{children: make(map[string]*TrieNode)}}
	return result
}

// Insert will take in a word and add it to the trie
func (t *Trie) Insert(word string) {
	currentNode := t.root
	for _, ch := range word {
		if _, exists := currentNode.children[string(ch)]; !exists {
			newChild := &TrieNode{children: make(map[string]*TrieNode)}
			currentNode.children[string(ch)] = newChild
			currentNode = newChild
		} else {
			currentNode = currentNode.children[string(ch)]
		}
	}
	currentNode.isWord = true
}

// Returns if there is any word in the trie that starts with the given prefix.
func (t *Trie) PrefixStart(prefix string) bool {
	node := t.root
	for i := 0; i < len(prefix); i++ {
		var c byte = prefix[i]
		if node.children[string(c)] == nil {
			return false
		}
		node = node.children[string(c)]
	}
	return true
}

// Search will search if a word is in the trie
func (t *Trie) Search(word string) bool {
	currentNode := t.root
	for _, ch := range word {
		if _, ok := currentNode.children[string(ch)]; !ok {
			return false
		}
		currentNode = currentNode.children[string(ch)]
	}
	if currentNode.isWord == true {
		return true
	}
	return false
}

// Returns a list of all words that share a common prefix
func (t *Trie) AllWordsFromPrefix(prefix string) []string {
	var wordsFromPrefix func(string, *TrieNode, *[]string)
	wordsFromPrefix = func(prefix string, node *TrieNode, words *[]string) {
		if node.isWord {
			*words = append(*words, prefix)
		}
		if len(node.children) == 0 {
			return
		}
		for ch := range node.children {
			wordsFromPrefix(prefix+ch, node.children[ch], words)
		}
	}
	words := make([]string, 0)
	current := t.root
	for _, ch := range prefix {
		next := current.children[string(ch)]
		if next == nil {
			return words
		}
		current = next
	}
	if len(current.children) > 0 {
		wordsFromPrefix(prefix, current, &words)
	} else {
		if current.isWord {
			words = append(words, prefix)
		}
	}
	sort.Slice(words, func(i, j int) bool {
		return len(words[i]) > len(words[j])
	})
	return words
}
