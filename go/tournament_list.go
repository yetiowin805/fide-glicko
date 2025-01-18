package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

// parseTournamentList extracts unique tournament codes from the tournament list HTML
func parseTournamentList(html string) ([]string, error) {
	var codes []string
	seen := make(map[string]struct{}) // Map to track unique codes
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
				if _, found := seen[code]; !found { // Check if code is already seen
					seen[code] = struct{}{} // Mark as seen
					codes = append(codes, code)
				}
			}
		}
	})
	return codes, nil
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

	url := fmt.Sprintf("%s/tournament_list.phtml?country=%s&rating_period=%d-%02d-01", BaseTournamentURL, federation, year, month)
	
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

func scrapeAllFederations(year int, month int, federations []string) []TournamentData {
	
	// Channel to collect all tournament codes
	tournamentChan := make(chan TournamentData, 1000) // Buffer size can be adjusted

	var fetchWg sync.WaitGroup // WaitGroup to synchronize fetching tournament lists

	// Fetch tournament lists concurrently
	for _, federation := range federations {
		fetchWg.Add(1)
		go scrapeFederation(federation, year, month, tournamentChan, &fetchWg)
	}

	// Close the channel once all fetches are done
	go func() {
		fetchWg.Wait()
		close(tournamentChan)
	}()

	// Collect all tournament codes into a slice
	var allTournaments []TournamentData
	counter := 0
	for tournament := range tournamentChan {
		tournament.Id = counter
		counter++
		allTournaments = append(allTournaments, tournament)
	}

	log.Printf("Total tournaments found: %d", len(allTournaments))

	return allTournaments
}
