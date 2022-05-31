package neo4j

import (
	"reflect"
	"testing"
)

func TestNamespace(t *testing.T) {
	myTrie := InitTrie()
	toAdd := []string{
		"orc",
		"okra",
		"orchard",
		"orcharx",
		"orcharac",
		"ocular",
		"orca",
	}
	for _, w := range toAdd {
		myTrie.Insert(w)
	}
	table := []struct {
		search string
	}{
		{"orc"},
		{"orca"},
		{"orchard"},
		{"orcharac"},
		{"ocular"},
		{"orcharx"},
		{"okra"},
	}
	for _, tt := range table {
		if !myTrie.Search(tt.search) {
			t.Errorf("Got %v, expected %v for %v", false, true, tt.search)
		}
	}
	expected := []string{"orcharac", "orchard", "orcharx", "orca", "orc"}
	if !reflect.DeepEqual(myTrie.AllWordsFromPrefix("or"), expected) {
		t.Errorf("Got %v, expected %v", myTrie.AllWordsFromPrefix("or"), expected)
	}
}
