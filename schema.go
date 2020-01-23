package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
)

func applySchema(dgraph *dgo.Dgraph) {
	ctx := context.Background()
	op := &api.Operation{
		Schema: `
		uri: string @index(hash) .
		domain: string @index(exact) .
		BACKLINK: [uid] .
		`,
	}
	err := dgraph.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Schema done")
}
