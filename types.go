package main

// Entry ..
type Entry struct {
	domainFrom string
	uriFrom    string
	domainTo   string
	uriTo      string
	anchorText string
	rel        string
}

// Batch ..
type Batch struct {
	entries []Entry
}

// PageVertex ..
type PageVertex struct {
	UID    string `json:"uid,omitempty"`
	Domain string `json:"domain,omitempty"`
	URI    string `json:"uri,omitempty"`
}
