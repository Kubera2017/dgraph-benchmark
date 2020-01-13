package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
)

type domain struct {
	UID  string `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func main() {
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
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
		name: string @index(exact) .
		has_backlink: [uid] .
		backlink_to: [uid] .
		`,
	}
	err = dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Schema done")

	domainsCount := 1000000
	backlinksCount := 10000

	for i := 0; i < domainsCount; i++ {
		mutations := make([]*api.Mutation, 0, 4)
		variables := make(map[string]string)

		sourceDomainName := "domain" + strconv.Itoa(i) + ".com"
		variables["$sourceDomainName"] = sourceDomainName

		query := `query q($sourceDomainName: string`
		for j := 0; j < backlinksCount; j++ {
			query += `, $targetDomainName` + strconv.Itoa(j) + `: string`
		}
		query += `) {
			source_domain as var(func: eq(name, $sourceDomainName))
		`
		for j := 0; j < backlinksCount; j++ {
			query += `target_domain` + strconv.Itoa(j) + ` as var(func: eq(name, $targetDomainName` + strconv.Itoa(j) + `))
			`
		}
		query += `}`
		// fmt.Println(query)

		// Create source domain if not exists
		newSourceDomain := domain{
			UID:  "_:newSourceDomain",
			Name: sourceDomainName,
		}
		newSourceDomainJSON, err := json.Marshal(newSourceDomain)
		if err != nil {
			log.Fatal(err)
		}
		newSourceDomainMutation := &api.Mutation{
			Cond:    ` @if( eq(len(source_domain), 0) ) `,
			SetJson: newSourceDomainJSON,
		}
		mutations = append(mutations, newSourceDomainMutation)

		backlinkArr := make([]string, 0, backlinksCount)
		for j := 0; j < backlinksCount; j++ {
			targetDomainName := ``
			for {
				targetDomainName = "domain" + strconv.Itoa(rand.Intn(domainsCount)) + ".com"
				if targetDomainName != sourceDomainName && contains(backlinkArr, targetDomainName) == false {
					backlinkArr = append(backlinkArr, targetDomainName)
					break
				}
			}
			variables["$targetDomainName"+strconv.Itoa(j)] = targetDomainName

			// Create target domain if not exists
			newTargetDomain := domain{
				UID:  "_:newTargetDomain" + strconv.Itoa(j),
				Name: targetDomainName,
			}
			newTargetDomainJSON, err := json.Marshal(newTargetDomain)
			if err != nil {
				log.Fatal(err)
			}
			newTargetDomainMutation := &api.Mutation{
				Cond:    ` @if( eq(len(target_domain` + strconv.Itoa(j) + `), 0) ) `,
				SetJson: newTargetDomainJSON,
			}
			mutations = append(mutations, newTargetDomainMutation)

			// Edge if source and target exists
			m11 := &api.Mutation{
				Cond: ` @if( eq(len(source_domain), 1) AND eq(len(target_domain` + strconv.Itoa(j) + `), 1) ) `,
				SetNquads: []byte(`
					_:back_link` + strconv.Itoa(j) + ` <url_from> "https://docs.dgraph.io/query-language/" .
					_:back_link` + strconv.Itoa(j) + ` <url_to> "https://docs.dgraph.io/design-concepts/#queries" .
					_:back_link` + strconv.Itoa(j) + ` <anchor_text> "Queries design concept" .
					_:back_link` + strconv.Itoa(j) + ` <rel> "rel" .
					uid(source_domain) <has_backlink> _:back_link` + strconv.Itoa(j) + `  .
					_:back_link` + strconv.Itoa(j) + ` <backlink_to> uid(target_domain` + strconv.Itoa(j) + `) .
				`),
			}
			mutations = append(mutations, m11)

			// Edge if source is not exist and target exists
			m01 := &api.Mutation{
				Cond: ` @if( eq(len(source_domain), 0) AND eq(len(target_domain` + strconv.Itoa(j) + `), 1) ) `,
				SetNquads: []byte(`
					_:back_link` + strconv.Itoa(j) + ` <url_from> "https://docs.dgraph.io/query-language/" .
					_:back_link` + strconv.Itoa(j) + ` <url_to> "https://docs.dgraph.io/design-concepts/#queries" .
					_:back_link` + strconv.Itoa(j) + ` <anchor_text> "Queries design concept" .
					_:back_link` + strconv.Itoa(j) + ` <rel> "rel" .
					_:newSourceDomain <has_backlink> _:back_link` + strconv.Itoa(j) + ` .
					_:back_link` + strconv.Itoa(j) + ` <backlink_to> uid(target_domain` + strconv.Itoa(j) + `) .
				`),
			}
			mutations = append(mutations, m01)

			// Edge if source exists and target is not exist
			m10 := &api.Mutation{
				Cond: ` @if( eq(len(source_domain), 1) AND eq(len(target_domain` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`
					_:back_link` + strconv.Itoa(j) + ` <url_from> "https://docs.dgraph.io/query-language/" .
					_:back_link` + strconv.Itoa(j) + ` <url_to> "https://docs.dgraph.io/design-concepts/#queries" .
					_:back_link` + strconv.Itoa(j) + ` <anchor_text> "Queries design concept" .
					_:back_link` + strconv.Itoa(j) + ` <rel> "rel" .
					uid(source_domain) <has_backlink> _:back_link` + strconv.Itoa(j) + ` .
					_:back_link` + strconv.Itoa(j) + ` <backlink_to> ` + `_:newTargetDomain` + strconv.Itoa(j) + ` .
				`),
			}
			mutations = append(mutations, m10)

			// Edge if source and target are not exist
			m00 := &api.Mutation{
				Cond: ` @if( eq(len(source_domain), 0) AND eq(len(target_domain` + strconv.Itoa(j) + `), 0) ) `,
				SetNquads: []byte(`
					_:back_link` + strconv.Itoa(j) + ` <url_from> "https://docs.dgraph.io/query-language/" .
					_:back_link` + strconv.Itoa(j) + ` <url_to> "https://docs.dgraph.io/design-concepts/#queries" .
					_:back_link` + strconv.Itoa(j) + ` <anchor_text> "Queries design concept" .
					_:back_link` + strconv.Itoa(j) + ` <rel> "rel" .
					_:newSourceDomain <has_backlink> _:back_link` + strconv.Itoa(j) + ` .
					_:back_link` + strconv.Itoa(j) + ` <backlink_to> ` + `_:newTargetDomain` + strconv.Itoa(j) + ` .
				`),
			}
			mutations = append(mutations, m00)
		}

		req := &api.Request{
			Query:     query,
			Vars:      variables,
			CommitNow: true,
			Mutations: mutations,
		}

		// fmt.Println("Batch format done", variables)
		tBatchStart := time.Now()
		_, err = dg.NewTxn().Do(ctx, req)
		if err != nil {
			log.Fatal(err)
		}

		tBatchEnd := time.Now()
		fmt.Println(i, "Batch done", tBatchEnd.Sub(tBatchStart))
		if i > 300 {
			break
		}
	}

}
