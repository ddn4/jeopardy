package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const SERVER_URL = "nats://127.0.0.1:62158"
const USER = "local"
const PASSWORD = "5ZEdxbKjfsUdx65nfEjsa6mmmlNDVDXg"

var kv jetstream.KeyValue
var keys []string

type Request struct {
	Number int `json:"number"`
	Value  int `json:"value"`
}

func main() {

	// Connect to NATS
	nc, err := nats.Connect(SERVER_URL, nats.UserInfo(USER, PASSWORD))
	if err != nil {
		log.Fatal(err)
	}

	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatal(err)
	}
	defer ec.Close()

	// Create JetStreamContext from nats connection
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// connect to KV store
	kv, err = js.KeyValue(ctx, "questions")
	if err != nil {
		log.Fatal(err)
	}

	// get keys from KV store
	keys, err = kv.Keys(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to NATS server")
	log.Printf("Found %d keys in KV store", len(keys))

	questionChanRecv := make(chan *Request)
	ec.BindRecvChan("request.questions", questionChanRecv)

	for {
		req := <-questionChanRecv
		selectedKeys, err := processRequest(req)
		if err != nil {
			log.Println(err)
			continue
		}

		for _, key := range selectedKeys {
			log.Printf("Key: %s\n", key)
		}
	}

}

func processRequest(req *Request) ([]string, error) {
	log.Printf("Processing request for %d questions of value $%d\n", req.Number, req.Value)
	//empty := make([]string, 0)
	out := make([]string, 0)

	filteredList := filterByValue(req.Value)

	for i := 0; i < req.Number; i++ {
		r := rand.Intn(len(filteredList))
		out = append(out, filteredList[r])
	}

	return out, nil
}

func filterByValue(v int) []string {
	filtered := make([]string, 0)
	vstr := "-" + strconv.Itoa(v)
	vlen := len(vstr)
	for _, key := range keys {
		kval := key[len(key)-vlen:]
		if kval == vstr {
			filtered = append(filtered, key)
		}
	}
	return filtered
}
