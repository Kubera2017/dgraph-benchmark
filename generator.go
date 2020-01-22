package main

import (
	"math/rand"
	"strconv"
)

func randStr(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func generateEntry() Entry {
	domainFrom := "domain" + strconv.Itoa(rand.Intn(domainsCount)) + ".com"
	domainTo := "domain" + strconv.Itoa(rand.Intn(domainsCount)) + ".com"
	entry := Entry{
		domainFrom: domainFrom,
		uriFrom:    domainFrom + "/" + randStr(rand.Intn(13)+3),
		domainTo:   domainTo,
		uriTo:      domainTo + "/" + randStr(rand.Intn(13)+3),
		anchorText: randStr(rand.Intn(15) + 3),
		rel:        randStr(rand.Intn(4) + 4),
	}
	return entry
}
