package neo4j

import (
	"fmt"
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
	fmt.Println(myTrie.Search("orc"))
	fmt.Println(myTrie.Search("orcs"))
	fmt.Println(myTrie.Search("orca"))
	fmt.Println(myTrie.Search("orchard"))
	fmt.Println(myTrie.Search("okra"))
	fmt.Println(myTrie.Search("orchards"))
	y := myTrie.AllWordsFromPrefix("or")
	fmt.Println(y)
	t.Fail()
}
