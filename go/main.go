package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

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

func scrapeBatch(batch []TournamentData, input *HandlerInput, batch_id int) []TournamentData {

	// Prepare for scraping tournament details
	var scrapeWg sync.WaitGroup             // WaitGroup to synchronize scrapes
	var mu sync.Mutex                       // Mutex to protect shared resources
	results := make([]TournamentData, 0, input.BatchSize) // Preallocate slice for batch
	semaphore := make(chan struct{}, input.MaxConcurrentScrapes)
	tournaments_scraped := 0
	tournaments_total := len(batch)

	// Channel to handle failed tournaments for retries
	failedChan := make(chan TournamentData, min(100, input.BatchSize)) // Buffer size can be adjusted

	// Function to process tournaments
	processTournament := func(t TournamentData, input *HandlerInput) {
		scrapeWg.Add(1)
		go func(t TournamentData, input *HandlerInput) {
			scrapeTournamentDetails(t, semaphore, &mu, &results, &scrapeWg, failedChan, &tournaments_scraped, &tournaments_total, input, batch_id)
		}(t, input)
	}

	// Initial processing of all tournaments
	for _, tournament := range batch {
		processTournament(tournament, input)
	}

	// Wait for all scrapes to finish
	scrapeWg.Wait()
	close(failedChan)

	failed_tournaments := []TournamentData{}
	for failedTournament := range failedChan {
		if failedTournament.RetryCount < input.MaxRetries {
			failed_tournaments = append(failed_tournaments, failedTournament)
		}
	}

	log.Printf("Saving %d tournaments to S3", len(results))

	saveCompressedJSONToS3(results, input.Year, input.Month, input.S3Bucket, input.S3KeyPrefix, input.Region, batch_id)
	saveSampleJSONToS3(results, input.S3Bucket, input.S3KeyPrefix, input.Region, batch_id)

	return failed_tournaments

}


// handler is the Lambda function entry point
func handler(ctx context.Context, input HandlerInput) (HandlerOutput, error) {

	start := time.Now()

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
		input.BatchSize = 5000
	}

	players, err := GetPlayersInfo(input.Year, input.Month, input.MaxRetries)
	if err != nil {
		return HandlerOutput{}, fmt.Errorf("failed to get players info: %v", err)
	}

	allTournaments := scrapeAllFederations(input.Year, input.Month, input.Federations)


	batch_id := 0
	// Process by batches
	for len(allTournaments) > 0 {
		batch := allTournaments[:min(input.BatchSize, len(allTournaments))]
		failed_tournaments := scrapeBatch(batch, &input, batch_id)
		// Remove the processed tournaments from allTournaments and add the failed ones
		allTournaments = allTournaments[len(batch):]
		allTournaments = append(allTournaments, failed_tournaments...)
		batch_id += 1
	}
	
	elapsed := time.Since(start)

	log.Printf("Time taken: %s", elapsed)

	return HandlerOutput{
		tournaments_total: len(allTournaments),
	}, nil
}

func main() {
	lambda.Start(handler)
}