package main

import (
	"context"
	"fmt"

	"github.com/CAIDA/goucsdnt"
)

func main() {
	ctx := context.Background()
	b := goucsdnt.NewUCSDNTBucket(ctx)
	keys, err := b.ListObjects()
	if err != nil {
		panic(err)
	}
	for _, key := range keys {
		fmt.Println(key)
	}
}
