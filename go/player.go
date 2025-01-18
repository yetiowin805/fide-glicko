package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var MonthMappings = map[int]string{
	1:  "jan",
	2:  "feb",
	3:  "mar",
	4:  "apr",
	5:  "may",
	6:  "jun",
	7:  "jul",
	8:  "aug",
	9:  "sep",
	10: "oct",
	11: "nov",
	12: "dec",
}

func downloadAndExtract(year, month int) (string, error) {
	// Downloads the ZIP file from FIDE, extracts the TXT file, and returns its content.

	var zipHeader string
	if year > 12 || (year == 12 && month >= 9) {
		zipHeader = fmt.Sprintf("standard_%s%d", MonthMappings[month], year)
	} else {
		zipHeader = fmt.Sprintf("%s%d", MonthMappings[month], year)
	}

	url := fmt.Sprintf("%s%sfrl.zip", BasePlayerURL, zipHeader)
	log.Printf("Downloading from URL: %s\n", url)

	response, err := get(url)
	if err != nil {
		log.Printf("Error downloading ZIP file: %v\n", err)
		return "", err
	}

	log.Printf("Downloaded file from %s\n", url)

	// Create a temporary file for the ZIP archive
	zipPath := filepath.Join(os.TempDir(), fmt.Sprintf("%sfrl.zip", zipHeader))
	err = os.WriteFile(zipPath, []byte(response), 0644)
	if err != nil {
		return "", fmt.Errorf("failed to save ZIP file: %w", err)
	}
	log.Printf("ZIP file saved to %s\n", zipPath)

	// Open the ZIP file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("failed to open ZIP file: %w", err)
	}
	defer reader.Close()

	// Extract the TXT file
	for _, file := range reader.File {
		if filepath.Ext(file.Name) == ".txt" {
			log.Printf("Extracting TXT file: %s\n", file.Name)
			rc, err := file.Open()
			if err != nil {
				return "", fmt.Errorf("failed to open TXT file in ZIP: %w", err)
			}
			defer rc.Close()

			content, err := io.ReadAll(rc)
			if err != nil {
				return "", fmt.Errorf("failed to read TXT file content: %w", err)
			}

			log.Printf("Downloaded and extracted file: %s\n", file.Name)
			return string(content), nil
		}
	}

	return "", errors.New("no .txt file found in the ZIP archive")

}

func getFormatConfig(year, month int) ([]int, []string) {
	// Determine the format configuration based on the date.
	// Returns a tuple of (lengths, keys) for the given year and month.

	switch {
	case year == 2001:
		if month <= 4 {
			return []int{10, 33, 6, 8, 6, 6, 11, 4},
				[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "flag"}
		}
		return []int{10, 33, 6, 8, 6, 6, 11, 4, 8},
			[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "sex", "flag"}

	case year == 2002 && month < 10:
		if month == 4 {
			return []int{10, 33, 6, 8, 6, 6, 11, 4, 6},
				[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "sex", "flag"}
		}
		return []int{10, 33, 6, 8, 6, 6, 11, 6},
			[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "flag"}

	case year < 2005 || (year == 2005 && month <= 7):
		return []int{9, 32, 6, 8, 6, 5, 11, 4},
			[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "flag"}

	case year < 2012 || (year == 2012 && month <= 8):
		return []int{10, 32, 6, 4, 6, 4, 6, 4},
			[]string{"id", "name", "title", "fed", "rating", "games", "b_year", "flag"}

	case year < 2016 || (year == 2016 && month <= 8):
		return []int{15, 61, 4, 4, 5, 5, 15, 6, 4, 3, 6, 4},
			[]string{"id", "name", "fed", "sex", "title", "w_title", "o_title", "rating", "games", "k", "b_year", "flag"}

	default:
		return []int{15, 61, 4, 4, 5, 5, 15, 4, 6, 4, 3, 6, 4},
			[]string{"id", "name", "fed", "sex", "title", "w_title", "o_title", "foa", "rating", "games", "k", "b_year", "flag"}
	}

}

// processLine splits a line into parts of fixed lengths.
func processLine(line string, lengths []int) ([]string, error) {
	var parts []string
	start := 0
	for _, length := range lengths {
		if start+length > len(line) {
			parts = append(parts, strings.TrimSpace(line[start:]))
			break
		}
		parts = append(parts, strings.TrimSpace(line[start:start+length]))
		start += length
	}
	return parts, nil
}

func processFile(content string, lengths []int, keys []string, year, month int) ([]map[string]string, error) {
	var processedLines []map[string]string

	lines := strings.Split(content, "\n")

	// Skip header line only for certain dates
	if len(lines) > 0 && !((year == 2003 && (month == 7 || month == 10)) ||
		(year == 2004 && month == 1) ||
		(year == 2005 && month == 4)) {
		lines = lines[1:]
	}

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts, err := processLine(line, lengths)
		if err != nil {
			return nil, fmt.Errorf("error processing line: %w", err)
		}

		// Create a map from keys to parts
		playerMap := make(map[string]string)
		for i, key := range keys {
			playerMap[key] = parts[i]
		}

		processedLines = append(processedLines, playerMap)
	}

	return processedLines, nil

}

func GetPlayersInfo(handlerInput *HandlerInput) ([]map[string]string, error) {

	year := handlerInput.Year % 100
	month := handlerInput.Month
	maxRetries := handlerInput.MaxRetries

	// Download and extract the file
	var content string
	var err error
	for i := 0; i < maxRetries; i++ {
		content, err = downloadAndExtract(year, month)
		if err != nil {
			log.Printf("Error downloading or extracting file: %v\n", err)
			continue
		}
	}
	// If we have retried maxRetries times and still failed, return an error
	if err != nil {
		return nil, fmt.Errorf("failed to download or extract file after %d retries", maxRetries)
	}

	lengths, keys := getFormatConfig(year, month)

	// Process the file
	players, err := processFile(content, lengths, keys, year, month)
	if err != nil {
		log.Printf("Error processing file: %v\n", err)
		return nil, err
	}

	// Save to S3
	err = savePlayersInfoToS3(players, handlerInput)
	if err != nil {
		log.Printf("Error saving players info to S3: %v", err)
	}

	// Return the processed content
	return players, nil

}