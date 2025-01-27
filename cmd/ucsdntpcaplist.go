package main

import (
	"fmt"

	"github.com/CAIDA/goucsdnt"
)

func main() {
	b := goucsdnt.NewUCSDNTBucket()
	keys, err := b.ListObjects()
	if err != nil {
		panic(err)
	}
	for _, key := range keys {
		fmt.Println(key)
	}
}
