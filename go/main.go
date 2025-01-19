package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
)

type HandlerInput struct {
	Federations []string `json:"federations"`
	Year        int      `json:"year"`
	Month       int      `json:"month"`
	S3Bucket    string   `json:"s3_bucket"`
	S3KeyPrefix string   `json:"s3_key_prefix"`
	Region      string   `json:"region"`
	MaxConcurrentScrapes  int      `json:"max_concurrent_scrapes"`
	MaxRetries            int      `json:"max_retries"`
	BatchSize             int      `json:"batch_size"`
}

type HandlerOutput struct {
	Games []GameData `json:"games"`
}

func handler(ctx context.Context, input HandlerInput) (HandlerOutput, error) {

	// Validate input
	if len(input.Federations) == 0 {
		return HandlerOutput{}, fmt.Errorf("no federations provided")
	} else {
		log.Printf("Scraping %d federations", len(input.Federations))
	}
	if input.S3Bucket == "" {
		return HandlerOutput{}, fmt.Errorf("S3 bucket not specified")
	}
	if input.Year < 1900 || input.Month < 1 || input.Month > 12 {
		return HandlerOutput{}, fmt.Errorf("invalid year or month")
	}
	if input.Region == "" {
		input.Region = "us-east-2"
	}
	if input.MaxConcurrentScrapes == 0 {
		input.MaxConcurrentScrapes = 35
	}
	if input.MaxRetries == 0 {
		input.MaxRetries = 3
	}
	if input.BatchSize == 0 {
		input.BatchSize = 3000
	}

	// Save player info
	GetPlayersInfo(&input)

	// Get tournament list
	allTournaments := scrapeAllFederations(input.Year, input.Month, input.Federations)

	// Save tournament details
	getTournamentDetails(allTournaments, &input)
	// Get games in batches
	batchId := 0
	for len(allTournaments) > 0 {
		batch := allTournaments[:min(input.BatchSize, len(allTournaments))]
		allTournaments = allTournaments[min(input.BatchSize, len(allTournaments)):]
		failedTournaments := processBatch(batch, &input, batchId)
		allTournaments = append(allTournaments, failedTournaments...)
		batchId++
	}

	return HandlerOutput{}, nil

}

func main() {
	lambda.Start(handler)
}
