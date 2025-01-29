package main

import (
	"context"
	"fmt"
	"log"

	"github.com/CAIDA/goucsdnt"
)

func main() {
	ctx := context.Background()
	b := goucsdnt.NewUCSDNTBucket(ctx)
	if b == nil {
		log.Println("NewUCSDNTBucket failed")
	}
	keys, err := b.ListObjects()
	if err != nil {
		log.Panic(err)
	}
	for _, key := range keys {
		fmt.Println(key)
	}
}
