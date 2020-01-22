package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	conn, err := grpc.Dial(dbHost, grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}

	dc := api.NewDgraphClient(conn)
	dgraph := dgo.NewDgraphClient(dc)

	dropDB(dgraph)
	applySchema(dgraph)

	var chs [workersCount]chan Batch
	for i := range chs {
		chs[i] = make(chan Batch)
		go worker(dgraph, chs[i])
	}
	chM := make(chan bool)
	go manager(chM, chs)

	_ = <-chM
	fmt.Println("Done")

}
