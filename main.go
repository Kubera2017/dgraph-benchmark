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

	var dataChs [workersCount]chan Batch
	jobRequestsCh := make(chan int)
	for i := range dataChs {
		dataChs[i] = make(chan Batch)
		go worker(dgraph, i, jobRequestsCh, dataChs[i])
	}
	chM := make(chan bool)
	go manager(chM, jobRequestsCh, dataChs)

	_ = <-chM
	fmt.Println("Done")

}
