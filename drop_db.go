package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
)

func dropDB(dgraph *dgo.Dgraph) {
	ctx := context.Background()
	err := dgraph.Alter(ctx, &api.Operation{DropAll: true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cleanup done")
}
