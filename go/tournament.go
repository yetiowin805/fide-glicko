package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

// ScrapeResult holds the result of a scraping attempt
type ScrapeResult struct {
	Data  TournamentData
	Error error
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

// scrapeTournamentDetails fetches and parses the details, crosstable, and report for a tournament
func scrapeTournamentDetails(tournament TournamentData, semaphore chan struct{}, wg *sync.WaitGroup) (TournamentData, error) {

	defer wg.Done()

	// Acquire semaphore
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// Get the details pages
	details, err := get(fmt.Sprintf("%s/%s%s", BaseTournamentURL, TournamentDetailsPath, tournament.Code))
	if err != nil {
		log.Printf("Error fetching details URL for code %s: %v", tournament.Code, err)
		tournament.RetryCount++
		return tournament, err
	}

	tournamentInfo := parseTournamentInfo(details)

	tournament.DateReceived = tournamentInfo["date_received"].(string)
	tournament.TimeControl = tournamentInfo["time_control"].(string)

	return tournament, nil

}

func getTournamentDetails(tournaments []TournamentData, input *HandlerInput) []TournamentData {

	// Prepare for scraping tournament details
	var scrapeWg sync.WaitGroup                            // WaitGroup to synchronize scrapes
	var mu sync.Mutex                                      // Mutex to protect shared resources
	results := make([]TournamentData, 0, len(tournaments)) // Preallocate slice for batch
	semaphore := make(chan struct{}, input.MaxConcurrentScrapes)

	log.Printf("Getting details for %d tournaments", len(tournaments))

	for len(tournaments) > 0 {

		// Channel to handle failed tournaments for retries
		failedChan := make(chan TournamentData, 100)

		// Function to process tournaments
		processTournament := func(t TournamentData) {
			scrapeWg.Add(1)
			go func(t TournamentData) {
				details, err := scrapeTournamentDetails(t, semaphore, &scrapeWg)
				if err != nil {
					failedChan <- t
				} else {
					mu.Lock()
					results = append(results, details)
					mu.Unlock()
				}
			}(t)
		}

		// Initial processing of all tournaments
		for _, tournament := range tournaments {
			processTournament(tournament)
		}

		// Wait for all scrapes to finish
		scrapeWg.Wait()
		close(failedChan)

		tournaments = []TournamentData{}

		for failedTournament := range failedChan {
			if failedTournament.RetryCount < input.MaxRetries {
				tournaments = append(tournaments, failedTournament)
			}
		}

	}

	err := saveTournamentDetailsToS3(results, input)
	if err != nil {
		log.Printf("Error saving tournament details to S3: %v", err)
	}

	return results
}

// parseReport parses crosstable from the report
func parseReport(reportDoc *goquery.Document, code string) map[string]map[string]interface{} {
	// Define the colors for identifying the relevant rows
	colors := map[string]bool{
		"#e2e2e2": true,
		"#ffffff": true,
	}

	// Slice to hold player information
	playersInfo := map[string]map[string]interface{}{}

	// Find all <tr> tags in the document
	reportDoc.Find("tr").Each(func(i int, tr *goquery.Selection) {
		// Check if the row has the desired background color
		bgColor, exists := tr.Attr("bgcolor")
		if !exists || !colors[bgColor] {
			return // Skip rows without the specified background colors
		}

		// Find all <td> tags within the row
		tdTags := tr.Find("td")
		if tdTags.Length() < 8 {
			return // Skip rows with fewer than 8 columns
		}

		// Extract the player data
		playersInfo[tdTags.Eq(0).Text()] = map[string]interface{}{
			"RC":    tdTags.Eq(4).Text(),
			"score": tdTags.Eq(6).Text(),
			"N":     tdTags.Eq(7).Text(),
		}
	})

	// Return the result as a map
	return playersInfo
}

func isMissingCrosstable(doc *goquery.Document) bool {
	// Check if the specific text exists anywhere in the document
	found := doc.Find("body").Text()
	return strings.Contains(found, "Tournament report was updated")
}

// parseResult parses the result text and converts it into a float
func parseResult(resultText string) float32 {
	switch resultText {
	case "0":
		return 0.0
	case "0.5":
		return 0.5
	case "1.0":
		return 1.0
	default:
		return -1.0
	}
}

// parseCrosstable parses the crosstable from the HTML
func parseCrosstable(htmlContent string, code string) (map[string]map[string]interface{}, bool, error) {
	var players map[string]map[string]interface{} = make(map[string]map[string]interface{})
	var currentPlayer map[string]interface{}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing crosstable HTML: %v", err)
		return players, false, err
	}

	if isMissingCrosstable(doc) {
		log.Printf("Crosstable is missing for tournament %s", code)
		return parseReport(doc, code), true, nil
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
				"opponents": []map[string]interface{}{},
			}
			players[number] = currentPlayer

		} else if bgcolor == "#FFFFFF" && currentPlayer != nil { // Opponent row
			nameTag := tdTags.Eq(1).Find("a")
			if nameTag.Length() == 0 || strings.Contains(tdTags.Eq(0).Text(), "NOT Rated Game") {
				return // Skip rows without a valid opponent or non-rated games
			}

			// Parse the result from the last but one column
			result := parseResult(strings.TrimSpace(tdTags.Eq(6).Text()))
			if result != -1.0 {
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

	return players, false, nil
}

func scrapeCrosstable(tournament TournamentData) (TournamentData, error) {
	// Get the crosstable
	crosstable, err := get(fmt.Sprintf("%s/%s%s", BaseTournamentURL, TournamentSourcePath, tournament.Code))
	if err != nil {
		log.Printf("Error fetching crosstable URL for code %s: %v", tournament.Code, err)
		return tournament, err
	}

	// Parse the crosstable
	parsedCrosstable, missing_crosstable, err := parseCrosstable(crosstable, tournament.Code)
	if err != nil {
		log.Printf("Error parsing crosstable for code %s: %v", tournament.Code, err)
		return tournament, err
	}
	tournament.Crosstable = parsedCrosstable
	tournament.HasReport = missing_crosstable

	return tournament, nil
}

func getTournamentGames(
	tournament TournamentData,
	semaphore chan struct{},
	wg *sync.WaitGroup,
) ([]GameData, []ReportData, error) {

	defer wg.Done()

	// Acquire semaphore
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	games := []GameData{}
	reports := []ReportData{}

	// Scrape tournament details
	tournament, err := scrapeCrosstable(tournament)
	if err != nil {
		return games, reports, err
	}

	if !tournament.HasReport {

		for _, player := range tournament.Crosstable {
			player_id, _ := strconv.Atoi(player["fide_id"].(string))
			for _, opponent := range player["opponents"].([]map[string]interface{}) {
				opponent_id, _ := strconv.Atoi(tournament.Crosstable[opponent["id"].(string)]["fide_id"].(string))
				if player_id < opponent_id {
					games = append(games, GameData{
						Player1:      player_id,
						Player2:      opponent_id,
						Result:       opponent["result"].(float32),
						TournamentId: tournament.Id,
					})
				}
			}
		}
	} else {
		for _, player := range tournament.Crosstable {
			player_id, _ := strconv.Atoi(player["fide_id"].(string))
			reports = append(reports, ReportData{
				PlayerId: player_id,
				RC:       player["rc"].(string),
				Score:    player["score"].(float32),
				N:        player["n"].(int32),
			})
		}
	}

	return games, reports, nil

}

func processBatch(batch []TournamentData, input *HandlerInput, batch_id int) []TournamentData {

	// Prepare for scraping tournament details
	var scrapeWg sync.WaitGroup                     // WaitGroup to synchronize scrapes
	var mu sync.Mutex                               // Mutex to protect shared resources
	games := make([]GameData, 0, input.BatchSize) // Preallocate slice for batch
	reports := make([]ReportData, 0, input.BatchSize) // Preallocate slice for batch
	semaphore := make(chan struct{}, input.MaxConcurrentScrapes)

	// Channel to handle failed tournaments for retries
	failedChan := make(chan TournamentData, 100)

	// Function to process tournaments
	processTournament := func(t TournamentData) {
		scrapeWg.Add(1)
		go func(t TournamentData) {
			gameData, reportData, err := getTournamentGames(t, semaphore, &scrapeWg)
			if err != nil {
				failedChan <- t
			} else {
				mu.Lock()
				if len(gameData) > 0 {
					games = append(games, gameData...)
				} else {
					reports = append(reports, reportData...)
				}
				mu.Unlock()
			}
		}(t)
	}

	// Initial processing of all tournaments
	for _, tournament := range batch {
		processTournament(tournament)
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

	log.Printf("Saving %d games to S3 for batch %d", len(games), batch_id)

	err := saveGamesParquetToS3(games, input, batch_id)
	if err != nil {
		log.Printf("Error saving games to S3: %v", err)
	}
	// Save a sample of the games to CSV
	err = saveGamesCSVToS3(games[:min(100, len(games))], input, batch_id)
	if err != nil {
		log.Printf("Error saving games to S3: %v", err)
	}

	if len(reports) > 0 {
		err = saveReportsParquetToS3(reports, input, batch_id)
		if err != nil {
			log.Printf("Error saving reports to S3: %v", err)
		}

		err = saveReportsCSVToS3(reports[:min(100, len(reports))], input, batch_id)
		if err != nil {
			log.Printf("Error saving reports to S3: %v", err)
		}
	}

	return failed_tournaments

}
