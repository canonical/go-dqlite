package dqlite_test

import (
	"fmt"
	"sort"

	dqlite "github.com/canonical/go-dqlite/v2"
)

type infoSorter []dqlite.LastEntryInfo

func (is infoSorter) Len() int {
	return len(is)
}

func (is infoSorter) Less(i, j int) bool {
	return is[i].Before(is[j])
}

func (is infoSorter) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func ExampleLastEntryInfo() {
	infos := []dqlite.LastEntryInfo{
		{Term: 1, Index: 2},
		{Term: 2, Index: 2},
		{Term: 1, Index: 1},
		{Term: 2, Index: 1},
	}
	sort.Sort(infoSorter(infos))
	fmt.Println(infos)
	// Output:
	// [{1 1} {1 2} {2 1} {2 2}]

}
