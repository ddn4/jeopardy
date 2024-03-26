package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Employee struct to hold personnel data
type Question struct {
	Id         string `json:"id,omitempty"`
	ShowNumber int    `json:"show_number"`
	AirDate    string `json:"airdate"`
	Round      string `json:"round"`
	Category   string `json:"category"`
	Value      string `json:"value"`
	Question   string `json:"question"`
	Answer     string `json:"answer"`
	Played     bool   `json:"played"`
}

func (q *Question) ParseId(i int) (string, error) {
	sn := strconv.Itoa(q.ShowNumber)

	san1 := strings.Replace(q.Value, "$", "", -1)
	san2 := strings.Replace(san1, ",", "", -1)
	san3 := strings.Replace(san2, "None", "0", -1)
	v, err := strconv.Atoi(san3)
	if err != nil {
		return "", err
	}
	q.Id = fmt.Sprintf("%d-%s-%v", i, sn, v)
	return q.Id, nil
}

var kv jetstream.KeyValue

const SERVER_URL = "nats://127.0.0.1:62158"
const USER = "local"
const PASSWORD = "5ZEdxbKjfsUdx65nfEjsa6mmmlNDVDXg"

func initNATS() {
	// Connect to NATS
	nc, _ := nats.Connect(SERVER_URL, nats.UserInfo(USER, PASSWORD))
	if nc == nil {
		log.Fatal("Error connecting to NATS server")
	}

	// Create JetStreamContext from nats connection
	js, _ := jetstream.New(nc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvc := &jetstream.KeyValueConfig{Bucket: "questions"}

	// Create a KV store named questions
	_, err := js.CreateKeyValue(ctx, *kvc)
	if err != nil {
		log.Printf("KV store already exists.\n\n %v", err)
	}
	kv, err = js.KeyValue(ctx, "questions")
	if err != nil {
		log.Printf("Error getting KV store: %v", err)
	}
}

func main() {
	initNATS() // Initialize NATS and JetStream

	// Populate the KV store with employee data from CSV file
	file, err := os.Open("../data/data.csv")
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a new reader
	reader := csv.NewReader(file)

	// Read the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Unable to read data from file", err)
	}

	var i int = 0
	var s int = 1

	// Loop through the records and update the KV store
	for _, record := range records {
		// Skip the header row
		if i == 0 {
			i++
			continue
		}
		i++

		// Convert the ShowNumber to an integer
		showNumber, err := strconv.Atoi(record[0])
		if err != nil {
			log.Printf("Error converting ShowNumber to integer: %v", err)
		}

		// Create a Question object
		q := Question{
			ShowNumber: showNumber,
			AirDate:    record[1],
			Round:      record[2],
			Category:   record[3],
			Value:      record[4],
			Question:   record[5],
			Answer:     record[6],
		}

		// Parse the ID for the KV store
		_, err = q.ParseId(s)
		if err != nil {
			log.Printf("Error parsing ID: %v", err)
			continue
		}

		// Convert Question to JSON for storage
		data, err := json.Marshal(q)
		if err != nil {
			log.Printf("Error marshalling question data: %v", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Store the Question data
		_, err = kv.Put(ctx, q.Id, data)
		if err != nil {
			log.Printf("Error storing question data: %v", err)
			continue
		} else {
			s++
		}
	}

}
