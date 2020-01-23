package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
)

type page struct {
	UID string `json:"uid,omitempty"`
	URI string `json:"uri,omitempty"`
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {

	dbHost := "127.0.0.1:9080"
	domainsCount := 1000000
	batchSize := 1000
	batchCount := 1000000
	breakAt := 0 // total number of batches to procced

	rand.Seed(time.Now().UnixNano())

	conn, err := grpc.Dial(dbHost, grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)
	ctx := context.Background()

	err = dg.Alter(ctx, &api.Operation{DropAll: true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cleanup done")

	op := &api.Operation{
		Schema: `
		uri: string @index(hash) .
		BACKLINK: [uid] .
		`,
	}
	err = dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Schema done")

	for i := 0; i < batchCount; i++ {
		mutations := make([]*api.Mutation, 0, 4)
		variables := make(map[string]string)
		query := `query q(`

		queryArr := make([]string, 0, batchSize*2)
		for j := 0; j < batchSize; j++ {
			domainFrom := "domain" + strconv.Itoa(rand.Intn(domainsCount)) + ".com"
			uriFrom := domainFrom + "/" + randStr(rand.Intn(13)+3)
			domainTo := "domain" + strconv.Itoa(rand.Intn(domainsCount)) + ".com"
			uriTo := domainTo + "/" + randStr(rand.Intn(13)+3)
			anchorText := randStr(rand.Intn(15) + 3)
			rel := randStr(rand.Intn(4) + 4)

			// fmt.Println(domainFrom, uriFrom, domainTo, uriTo, anchorText, rel)

			if j != 0 {
				query += `, `
			}
			query += `$uriFrom` + strconv.Itoa(j) + `: string`
			query += `, $uriTo` + strconv.Itoa(j) + `: string`

			queryArr = append(queryArr, `uri_from`+strconv.Itoa(j)+` as var(func: eq(uri, $uriFrom`+strconv.Itoa(j)+`))
			`)
			queryArr = append(queryArr, `uri_to`+strconv.Itoa(j)+` as var(func: eq(uri, $uriTo`+strconv.Itoa(j)+`))
			`)

			variables[`$uriFrom`+strconv.Itoa(j)] = uriFrom
			variables[`$uriTo`+strconv.Itoa(j)] = uriTo

			// Create PageFrom node if not exists
			newURIFrom := page{
				UID: `_:newUriFrom` + strconv.Itoa(j),
				URI: uriFrom,
			}
			newURIFromJSON, err := json.Marshal(newURIFrom)
			if err != nil {
				log.Fatal(err)
			}
			newURIFromMutation := &api.Mutation{
				Cond:    ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 0) ) `,
				SetJson: newURIFromJSON,
			}
			mutations = append(mutations, newURIFromMutation)

			// Create PageTo node if not exists
			newURITo := page{
				UID: `_:newUriTo` + strconv.Itoa(j),
				URI: uriTo,
			}
			newURIToJSON, err := json.Marshal(newURITo)
			if err != nil {
				log.Fatal(err)
			}
			newURIToMutation := &api.Mutation{
				Cond:    ` @if( eq(len(uri_to` + strconv.Itoa(j) + `), 0) ) `,
				SetJson: newURIToJSON,
			}
			mutations = append(mutations, newURIToMutation)

			// Edge if source and target exists
			m11 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 1) AND eq(len(uri_to` + strconv.Itoa(j) + `), 1) ) `,
				SetNquads: []byte(`uid(uri_from` + strconv.Itoa(j) + `) <BACKLINK> uid(uri_to` + strconv.Itoa(j) + `) (anchorText = "` + anchorText + `", rel = "` + rel + `") .
				`),
			}
			mutations = append(mutations, m11)

			// Edge if source is not exist and target exists
			m01 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 0) AND eq(len(uri_to` + strconv.Itoa(j) + `), 1) ) `,
				SetNquads: []byte(`_:newUriFrom` + strconv.Itoa(j) + ` <BACKLINK> uid(uri_to` + strconv.Itoa(j) + `) (anchorText = "` + anchorText + `", rel = "` + rel + `") .
				`),
			}
			mutations = append(mutations, m01)

			// Edge if source exists and target is not exist
			m10 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 1) AND eq(len(uri_to` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`uid(uri_from` + strconv.Itoa(j) + `) <BACKLINK> _:newUriTo` + strconv.Itoa(j) + ` (anchorText = "` + anchorText + `", rel = "` + rel + `") .
				`),
			}
			mutations = append(mutations, m10)

			// Edge if source and target exists
			m00 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 0) AND eq(len(uri_to` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`_:newUriFrom` + strconv.Itoa(j) + ` <BACKLINK> _:newUriTo` + strconv.Itoa(j) + ` (anchorText = "` + anchorText + `", rel = "` + rel + `") .
				`),
			}
			mutations = append(mutations, m00)
		}

		query += `) {
		`
		query += strings.Join(queryArr[:], "")
		query += `
		}`

		req := &api.Request{
			Query:     query,
			Vars:      variables,
			CommitNow: true,
			Mutations: mutations,
		}
		// fmt.Println(req)

		tBatchStart := time.Now()
		response, err := dg.NewTxn().Do(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(response.Latency)

		tBatchEnd := time.Now()
		fmt.Println(i, "Batch done", tBatchEnd.Sub(tBatchStart))
		if breakAt > 0 && i > breakAt-1 {
			break
		}

	}

}
