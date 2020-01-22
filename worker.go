package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
)

func worker(dgraph *dgo.Dgraph, c chan Batch) {
	ctx := context.Background()
	for batch := range c {

		mutations := make([]*api.Mutation, 0, 4)
		variables := make(map[string]string)
		query := `query q(`

		queryArr := make([]string, 0, len(batch.entries)*2)

		for j := 0; j < len(batch.entries); j++ {
			if j != 0 {
				query += `, `
			}
			query += `$uriFrom` + strconv.Itoa(j) + `: string`
			query += `, $uriTo` + strconv.Itoa(j) + `: string`

			queryArr = append(queryArr, `uri_from`+strconv.Itoa(j)+` as var(func: eq(uri, $uriFrom`+strconv.Itoa(j)+`))
			`)
			queryArr = append(queryArr, `uri_to`+strconv.Itoa(j)+` as var(func: eq(uri, $uriTo`+strconv.Itoa(j)+`))
			`)

			variables[`$uriFrom`+strconv.Itoa(j)] = batch.entries[j].uriFrom
			variables[`$uriTo`+strconv.Itoa(j)] = batch.entries[j].uriTo

			// Create PageFrom node if not exists
			newURIFrom := PageVertex{
				UID:    `_:newUriFrom` + strconv.Itoa(j),
				URI:    batch.entries[j].uriFrom,
				Domain: batch.entries[j].domainFrom,
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
			newURITo := PageVertex{
				UID:    `_:newUriTo` + strconv.Itoa(j),
				URI:    batch.entries[j].uriTo,
				Domain: batch.entries[j].domainTo,
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
				SetNquads: []byte(`uid(uri_from` + strconv.Itoa(j) + `) <BACKLINK> uid(uri_to` + strconv.Itoa(j) + `) (anchorText = "` + batch.entries[j].anchorText + `", rel = "` + batch.entries[j].rel + `") .
				`),
			}
			mutations = append(mutations, m11)

			// Edge if source is not exist and target exists
			m01 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 0) AND eq(len(uri_to` + strconv.Itoa(j) + `), 1) ) `,
				SetNquads: []byte(`_:newUriFrom` + strconv.Itoa(j) + ` <BACKLINK> uid(uri_to` + strconv.Itoa(j) + `) (anchorText = "` + batch.entries[j].anchorText + `", rel = "` + batch.entries[j].rel + `") .
				`),
			}
			mutations = append(mutations, m01)

			// Edge if source exists and target is not exist
			m10 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 1) AND eq(len(uri_to` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`uid(uri_from` + strconv.Itoa(j) + `) <BACKLINK> _:newUriTo` + strconv.Itoa(j) + ` (anchorText = "` + batch.entries[j].anchorText + `", rel = "` + batch.entries[j].rel + `") .
				`),
			}
			mutations = append(mutations, m10)

			// Edge if source and target exists
			m00 := &api.Mutation{
				Cond: ` @if( eq(len(uri_from` + strconv.Itoa(j) + `), 0) AND eq(len(uri_to` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`_:newUriFrom` + strconv.Itoa(j) + ` <BACKLINK> _:newUriTo` + strconv.Itoa(j) + ` (anchorText = "` + batch.entries[j].anchorText + `", rel = "` + batch.entries[j].rel + `") .
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
		response, err := dgraph.NewTxn().Do(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(response.Latency)
	}

}
