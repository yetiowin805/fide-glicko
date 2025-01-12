package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Constants
const (
	BaseURL               = "https://ratings.fide.com"
	Year                  = 2024
	Month                 = 7
	TournamentDetailsPath = "tournament_details.phtml?event="
	TournamentSourcePath  = "view_source.phtml?code="
	TournamentReportPath  = "tournament_report.phtml?event16="
	MaxConcurrentScrapes  = 10 // Maximum number of concurrent tournament detail scrapes
	MaxRetries            = 3   // Maximum number of retries per tournament
	RetryDelay            = 5 * time.Second
)

// TournamentData represents the structure to hold tournament information
type TournamentData struct {
	Federation string                 `json:"federation"`
	Code       string                 `json:"code"`
	Details    map[string]interface{} `json:"details"`
	Crosstable []map[string]interface{} `json:"crosstable"`
	Report     map[string]interface{} `json:"report"`
	RetryCount int                    `json:"retry_count"`
}

// ScrapeResult holds the result of a scraping attempt
type ScrapeResult struct {
	Data  TournamentData
	Error error
}

// get fetches the content from a URL and returns it as a string with a timeout
func get(url string) (string, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request for URL %s: %v", url, err)
	}
	// Set a custom User-Agent to identify your scraper
	req.Header.Set("User-Agent", "FIDE-MyScraperBot/1.0 (+https://yourdomain.com/bot)")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch the URL %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("non-200 status code for URL %s: %v", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body from URL %s: %v", url, err)
	}

	return string(body), nil
}

// parseTournamentList extracts tournament codes from the tournament list HTML
func parseTournamentList(html string) ([]string, error) {
	var codes []string
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return codes, fmt.Errorf("failed to parse HTML: %v", err)
	}

	doc.Find("a[href*='tournament_report.phtml']").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			parts := strings.Split(href, "=")
			if len(parts) > 1 {
				code := parts[len(parts)-1]
				codes = append(codes, code)
			}
		}
	})

	return codes, nil
}

// parseTournamentInfo parses the tournament details from the HTML
func parseTournamentInfo(htmlContent string) map[string]interface{} {
	data := make(map[string]interface{})
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing tournament details HTML: %v", err)
		return data
	}

	var dateReceived, timeControl string

	// Iterate over all table rows
	doc.Find("tr").Each(func(i int, row *goquery.Selection) {
		tdTags := row.Find("td")
		if tdTags.Length() < 2 {
			return // Skip rows without at least two columns
		}

		key := strings.TrimSpace(tdTags.Eq(0).Text())
		value := strings.TrimSpace(tdTags.Eq(1).Text())

		if key == "Date received" {
			dateReceived = value
			if dateReceived == "0000-00-00" {
				// Look for "End Date" if "Date received" is invalid
				doc.Find("tr").Each(func(j int, innerRow *goquery.Selection) {
					tdInner := innerRow.Find("td")
					if tdInner.Length() < 2 {
						return
					}

					innerKey := strings.TrimSpace(tdInner.Eq(0).Text())
					innerValue := strings.TrimSpace(tdInner.Eq(1).Text())

					if innerKey == "End Date" {
						dateReceived = innerValue
					}
				})
			}
		} else if key == "Time Control" {
			timeControl = strings.TrimSpace(strings.Split(value, ":")[0])
		}
	})

	data["date_received"] = dateReceived
	data["time_control"] = timeControl

	return data
}

// parseResult parses the result text and converts it into a float
func parseResult(resultText string) string {
	switch resultText {
	case "0", "0.5", "1.0":
		return resultText
	default:
		return ""
	}
}

// parseCrosstable parses the crosstable from the HTML
func parseCrosstable(htmlContent string, code string) []map[string]interface{} {
	var players []map[string]interface{}
	var currentPlayer map[string]interface{}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing crosstable HTML: %v", err)
		return players
	}

	// Iterate over table rows
	doc.Find("tr").Each(func(i int, row *goquery.Selection) {
		tdTags := row.Find("td")
		if tdTags.Length() != 8 {
			return // Skip rows that don't have exactly 8 columns
		}

		// Check background color of the second cell
		bgcolor, _ := tdTags.Eq(1).Attr("bgcolor")

		if bgcolor == "#CBD8F9" { // Player row
			fideID := strings.TrimSpace(tdTags.Eq(0).Text())
			nameTag := tdTags.Eq(1).Find("a")
			var name, number string

			if nameTag.Length() > 0 {
				name = strings.TrimSpace(nameTag.Text())
				number, _ = nameTag.Attr("name")
			} else {
				name = strings.TrimSpace(tdTags.Eq(1).Text())
				number = ""
			}

			// Create a new player map
			currentPlayer = map[string]interface{}{
				"fide_id":   fideID,
				"name":      name,
				"number":    number,
				"opponents": []map[string]interface{}{},
			}
			players = append(players, currentPlayer)

		} else if bgcolor == "#FFFFFF" && currentPlayer != nil { // Opponent row
			nameTag := tdTags.Eq(1).Find("a")
			if nameTag.Length() == 0 || strings.Contains(tdTags.Eq(0).Text(), "NOT Rated Game") {
				return // Skip rows without a valid opponent or non-rated games
			}

			// Parse the result from the last but one column
			result := parseResult(strings.TrimSpace(tdTags.Eq(6).Text()))
			if result != "" && result != "Forfeit" {
				opponent := map[string]interface{}{
					"name":   strings.TrimSpace(nameTag.Text()),
					"id":     strings.TrimSpace(strings.TrimPrefix(nameTag.AttrOr("href", ""), "#")),
					"result": result,
				}

				// Append opponent to the current player's opponents
				currentPlayer["opponents"] = append(currentPlayer["opponents"].([]map[string]interface{}), opponent)
			}
		}
	})

	log.Printf("Processed %d players for tournament %s", len(players), code)
	return players
}

// parseReport parses the tournament report from the HTML
func parseReport(htmlContent string) map[string]interface{} {
	data := make(map[string]interface{})
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing report HTML: %v", err)
		return data
	}

	// Check if the report contains the specific text indicating it was updated or replaced
	reportUpdated := false
	doc.Find("body").Each(func(i int, s *goquery.Selection) {
		if strings.Contains(s.Text(), "Tournament report was updated or replaced") {
			reportUpdated = true
		}
	})

	data["report_updated"] = reportUpdated
	return data
}

// scrapeTournamentDetails fetches and parses the details, crosstable, and report for a tournament
func scrapeTournamentDetails(
	tournament TournamentData,
	semaphore chan struct{},
	mu *sync.Mutex,
	results *[]TournamentData,
	wg *sync.WaitGroup,
	failedChan chan<- TournamentData,
) {
	defer wg.Done()

	// Acquire semaphore
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// URLs for details and crosstable
	detailsURL := fmt.Sprintf("%s/%s%s", BaseURL, TournamentDetailsPath, tournament.Code)
	crosstableURL := fmt.Sprintf("%s/%s%s", BaseURL, TournamentSourcePath, tournament.Code)

	// Channel to receive fetched content with type
	type fetchResult struct {
		content string
		err     error
		typ     string
	}

	respChan := make(chan fetchResult, 2)
	var tournament_wg sync.WaitGroup // WaitGroup to ensure goroutines complete before closing respChan

	// Function to fetch a URL and send the response with type
	fetchURL := func(url string, typ string) {
		defer tournament_wg.Done() // Mark this goroutine as done when it completes
		content, err := get(url)
		respChan <- fetchResult{content: content, err: err, typ: typ}
	}

	// Increment the WaitGroup counter for each goroutine
	tournament_wg.Add(2)
	go fetchURL(detailsURL, "details")
	go fetchURL(crosstableURL, "crosstable")

	// Wait for all fetchURL goroutines to finish
	tournament_wg.Wait()

	// Collect responses
	var details, crosstable string
	success := true

	for i := 0; i < 2; i++ {
		resp := <-respChan
		if resp.err != nil {
			log.Printf("Error fetching %s URL for code %s: %v", resp.typ, tournament.Code, resp.err)
			success = false
			break
		}
		switch resp.typ {
		case "details":
			details = resp.content
		case "crosstable":
			crosstable = resp.content
		}
	}
	close(respChan) // Close the channel only after all goroutines have finished

	if !success {
		if tournament.RetryCount < MaxRetries {
			log.Printf("Retrying tournament %s (Attempt %d)", tournament.Code, tournament.RetryCount+1)
			failedChan <- TournamentData{
				Federation: tournament.Federation,
				Code:       tournament.Code,
				RetryCount: tournament.RetryCount + 1,
			}
		} else {
			log.Printf("Max retries reached for tournament %s. Skipping.", tournament.Code)
		}
		return
	}


	// Parse the fetched content
	parsedDetails := parseTournamentInfo(details)
	parsedCrosstable := parseCrosstable(crosstable, tournament.Code)

	// Initialize data
	data := TournamentData{
		Federation: tournament.Federation,
		Code:       tournament.Code,
		Details:    parsedDetails,
		Crosstable: parsedCrosstable,
		Report:     nil, // To be filled if needed
	}

	// If crosstable is empty, fetch the report
	if len(parsedCrosstable) == 0 {
		reportURL := fmt.Sprintf("%s/%s%s", BaseURL, TournamentReportPath, tournament.Code)
		reportContent, err := get(reportURL)
		if err != nil {
			log.Printf("Error fetching report URL for code %s: %v", tournament.Code, err)
			if tournament.RetryCount < MaxRetries {
				log.Printf("Retrying tournament %s for report (Attempt %d)", tournament.Code, tournament.RetryCount+1)
				failedChan <- TournamentData{
					Federation: tournament.Federation,
					Code:       tournament.Code,
				}
			} else {
				log.Printf("Max retries reached for report of tournament %s. Skipping.", tournament.Code)
			}
			return
		}
		parsedReport := parseReport(reportContent)
		data.Report = parsedReport
	}

	// Safely append to the results slice
	mu.Lock()
	*results = append(*results, data)
	mu.Unlock()

	log.Printf("Scraped tournament details for federation %s, code %s", tournament.Federation, tournament.Code)
}

// scrapeFederation fetches the tournament list for a federation and sends tournament data to a channel
func scrapeFederation(
	federation string,
	year int,
	month int,
	tournamentChan chan<- TournamentData,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	url := fmt.Sprintf("%s/tournament_list.phtml?country=%s&rating_period=%d-%02d-01", BaseURL, federation, year, month)
	log.Printf("Fetching tournament list for federation %s from %s", federation, url)

	body, err := get(url)
	if err != nil {
		log.Printf("Failed to fetch tournament list for %s: %v", federation, err)
		return
	}

	// Parse tournament codes from the HTML
	tournamentCodes, err := parseTournamentList(body)
	if err != nil {
		log.Printf("Failed to parse tournament list for %s: %v", federation, err)
		return
	}

	log.Printf("Found %d tournaments for federation %s", len(tournamentCodes), federation)

	// Send tournament codes to the channel
	for _, code := range tournamentCodes {
		data := TournamentData{
			Federation: federation,
			Code:       code,
			RetryCount: 0,
		}
		tournamentChan <- data
	}
}

// saveToJSON saves the tournament data to a JSON file
func saveToJSON(tournaments []TournamentData, year int, month int) error {
	filename := fmt.Sprintf("tournaments_%d_%02d.json", year, month)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(tournaments)
	if err != nil {
		return fmt.Errorf("failed to encode JSON: %v", err)
	}

	log.Printf("Saved %d tournaments to %s", len(tournaments), filename)
	return nil
}

func main() {
	start := time.Now()

	// Read federations from federations.txt
	federations, err := os.ReadFile("federations.txt")
	if err != nil {
		log.Fatalf("Failed to read federations.txt: %v", err)
	}

	// Split federations into lines and remove any empty lines
	federationsList := strings.Split(strings.TrimSpace(string(federations)), "\n")
	log.Printf("Found %d federations", len(federationsList))

	// Channel to collect all tournament codes
	tournamentChan := make(chan TournamentData, 1000) // Buffer size can be adjusted

	var fetchWg sync.WaitGroup // WaitGroup to synchronize fetching tournament lists

	// Fetch tournament lists concurrently
	for _, federation := range federationsList {
		fetchWg.Add(1)
		go scrapeFederation(federation, Year, Month, tournamentChan, &fetchWg)
	}

	// Close the channel once all fetches are done
	go func() {
		fetchWg.Wait()
		close(tournamentChan)
	}()

	// Collect all tournament codes into a slice
	var allTournaments []TournamentData
	for tournament := range tournamentChan {
		allTournaments = append(allTournaments, tournament)
	}

	log.Printf("Total tournaments found: %d", len(allTournaments))

	// Prepare for scraping tournament details
	var scrapeWg sync.WaitGroup             // WaitGroup to synchronize scrapes
	var mu sync.Mutex                       // Mutex to protect shared resources
	results := make([]TournamentData, 0, len(allTournaments)) // Preallocate slice
	semaphore := make(chan struct{}, MaxConcurrentScrapes)

	// Channel to handle failed tournaments for retries
	failedChan := make(chan TournamentData, 1000) // Buffer size can be adjusted

	// Counters for successes and failures
	var successCount, failureCount int

	// Function to process tournaments
	processTournament := func(t TournamentData) {
		scrapeWg.Add(1)
		go func(t TournamentData) {
			scrapeTournamentDetails(t, semaphore, &mu, &results, &scrapeWg, failedChan)
		}(t)
	}

	// Initial processing of all tournaments
	for _, tournament := range allTournaments {
		processTournament(tournament)
	}

	// Handle retries
	go func() {
		for failedTournament := range failedChan {
			// Find the current retry count (not tracked in TournamentData, so pass as 1)
			// Alternatively, you can modify TournamentData to include retry count
			processTournament(failedTournament)
		}
	}()

	// Wait for all scrapes to finish
	scrapeWg.Wait()
	close(failedChan)

	log.Println("Scraped all tournament details")

	for _, tournament := range results {
		if len(tournament.Report) > 0 || len(tournament.Crosstable) > 0 {
			successCount++
		} else {
			failureCount++
		}
	}

	// Save the collected tournament data to a JSON file
	err = saveToJSON(results, Year, Month)
	if err != nil {
		log.Fatalf("Failed to save tournament data: %v", err)
	}

	// Display the counts of successes and failures
	log.Printf("Total Scraped Successfully: %d", successCount)
	log.Printf("Total Scraped Failed: %d", failureCount)

	// // Optionally, display the collected tournament data
	// for _, tournament := range results {
	// 	fmt.Printf("Federation: %s, Tournament Code: %s\n", tournament.Federation, tournament.Code)
	// 	// Uncomment below lines to print parsed data
	// 	// fmt.Printf("Details: %+v\n", tournament.Details)
	// 	// fmt.Printf("Crosstable: %+v\n", tournament.Crosstable)
	// 	// fmt.Printf("Report: %+v\n", tournament.Report)
	// }

	elapsed := time.Since(start)
	log.Printf("Total scraped successfully: %d", successCount)
	log.Printf("Total scraped failed: %d", failureCount)
	log.Printf("Time taken: %s", elapsed)
}
