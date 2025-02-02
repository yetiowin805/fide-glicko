package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/parquet"
)

const (
	BasePlayerURL = "https://ratings.fide.com/download/"
	BaseTournamentURL = "https://ratings.fide.com"
	TournamentDetailsPath = "tournament_details.phtml?event="
	TournamentSourcePath  = "view_source.phtml?code="
	TournamentReportPath  = "tournament_report.phtml?event16="
)

type PlayerData struct {
	Id          int    `parquet:"name=id, type=INT64"`
	Name        string `parquet:"name=name, type=BYTE_ARRAY"`
	Federation  string `parquet:"name=fed, type=BYTE_ARRAY"`
	Sex         string `parquet:"name=sex, type=BYTE_ARRAY"`
	Title       string `parquet:"name=title, type=BYTE_ARRAY"`
	WTitle      string `parquet:"name=w_title, type=BYTE_ARRAY"`
	OTitle      string `parquet:"name=o_title, type=BYTE_ARRAY"`
	FOA         string `parquet:"name=foa, type=BYTE_ARRAY"`
	Rating      int    `parquet:"name=rating, type=INT32"`
	Games       int    `parquet:"name=games, type=INT32"`
	K           int    `parquet:"name=k, type=INT32"`
	BirthYear   int    `parquet:"name=b_year, type=INT32"`
	Flag        string `parquet:"name=flag, type=BYTE_ARRAY"`
}

// TournamentData represents the structure to hold tournament information
type TournamentData struct {
	Id int `json:"id"`
	Federation string                 `json:"federation"`
	Code       string                 `json:"code"`
	DateReceived string               `json:"date_received"`
	TimeControl string               `json:"time_control"`
	Crosstable map[string]map[string]interface{} `json:"crosstable"`
	HasReport  bool                   `json:"has_report"`
	RetryCount int                    `json:"retry_count"`
}

type GameData struct {
	Player1      int     `parquet:"name=player1, type=INT64"`
	Player2      int     `parquet:"name=player2, type=INT64"`
	Result       float32 `parquet:"name=result, type=FLOAT"`
	TournamentId int     `parquet:"name=tournament_id, type=INT64"`
}

type ReportData struct {
	PlayerId int `parquet:"name=player_id, type=INT64"`
	RC string `parquet:"name=rc, type=BYTE_ARRAY"`
	Score float32 `parquet:"name=score, type=FLOAT"`
	N int32 `parquet:"name=n, type=INT32"`
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

func saveToS3(data []byte, filename, suffix, contentType, contentEncoding string, input *HandlerInput) error {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(input.Region),
	})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %v", err)
	}

	svc := s3.New(sess)

	key := fmt.Sprintf("%s/%s_%d_%02d.%s", input.S3KeyPrefix, filename, input.Year, input.Month, suffix)

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(input.S3Bucket),
		Key: aws.String(key),
		Body: bytes.NewReader(data),
		ContentType: aws.String(contentType),
		ContentEncoding: aws.String(contentEncoding),
	})
	if err != nil {
		return fmt.Errorf("failed to upload data to S3: %v", err)
	}

	log.Printf("Successfully uploaded data to s3://%s/%s", input.S3Bucket, key)
	return nil
}

func savePlayersInfoToS3(players []PlayerData, input *HandlerInput) error {

	// Write data as parquet
	buf := new(bytes.Buffer)
	pw := writerfile.NewWriterFile(buf)

	playerWriter, err := writer.NewParquetWriter(pw, new(PlayerData), 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer playerWriter.WriteStop()

	playerWriter.CompressionType = parquet.CompressionCodec_GZIP

	for _, player := range players {
		if err := playerWriter.Write(player); err != nil {
			return fmt.Errorf("failed to write player to Parquet: %w", err)
		}
	}

	if err := playerWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop Parquet writer: %w", err)
	}

	compressedBuf := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(compressedBuf)
	if _, err := gzipWriter.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to compress Parquet data: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	err = saveToS3(compressedBuf.Bytes(), "players", "parquet.gzip", "application/x-parquet", "gzip", input)
	if err != nil {
		return fmt.Errorf("failed to save players info to S3: %v", err)
	}

	return nil
}

func saveTournamentDetailsToS3(tournaments []TournamentData, input *HandlerInput) error {

	// Create a buffer to hold the CSV data
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	header := []string{"Federation", "Code", "DateReceived", "TimeControl"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header to CSV: %v", err)
	}

	for _, tournament := range tournaments {
		row := []string{tournament.Federation, tournament.Code, tournament.DateReceived, tournament.TimeControl}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row to CSV: %v", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %v", err)
	}

	err := saveToS3(buffer.Bytes(), "tournament_details", "csv", "text/csv", "", input)
	if err != nil {
		return fmt.Errorf("failed to save tournament details to S3: %v", err)
	}

	return nil

}

func saveGamesParquetToS3(games []GameData, input *HandlerInput, batch_id int) error {

	buf := new(bytes.Buffer)
	pw := writerfile.NewWriterFile(buf)


	gameWriter, err := writer.NewParquetWriter(pw, new(GameData), 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer gameWriter.WriteStop()

	gameWriter.CompressionType = parquet.CompressionCodec_GZIP

	for _, game := range games {
		if err := gameWriter.Write(game); err != nil {
			return fmt.Errorf("failed to write game to Parquet: %w", err)
		}
	}

	if err := gameWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop Parquet writer: %w", err)
	}

	compressedBuf := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(compressedBuf)
	if _, err := gzipWriter.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to compress Parquet data: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	err = saveToS3(compressedBuf.Bytes(), fmt.Sprintf("games_%d", batch_id), "parquet.gzip", "application/x-parquet", "gzip", input)
	if err != nil {
		return fmt.Errorf("failed to save games to S3: %w", err)
	}

	return nil

}

func saveReportsParquetToS3(reports []ReportData, input *HandlerInput, batch_id int) error {

	buf := new(bytes.Buffer)
	pw := writerfile.NewWriterFile(buf)

	reportWriter, err := writer.NewParquetWriter(pw, new(ReportData), 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer reportWriter.WriteStop()

	reportWriter.CompressionType = parquet.CompressionCodec_GZIP

	for _, report := range reports {
		if err := reportWriter.Write(report); err != nil {
			return fmt.Errorf("failed to write report to Parquet: %w", err)
		}
	}

	if err := reportWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop Parquet writer: %w", err)
	}

	compressedBuf := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(compressedBuf)
	if _, err := gzipWriter.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to compress Parquet data: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	err = saveToS3(compressedBuf.Bytes(), fmt.Sprintf("reports_%d", batch_id), "parquet.gzip", "application/x-parquet", "gzip", input)
	if err != nil {
		return fmt.Errorf("failed to save reports to S3: %w", err)
	}

	return nil
}

func saveGamesCSVToS3(games []GameData, input *HandlerInput, batch_id int) error {

	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)

	header := []string{"Player1", "Player2", "Result", "TournamentId"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header to CSV: %w", err)
	}

	for _, game := range games {
		row := []string{fmt.Sprintf("%d", game.Player1), fmt.Sprintf("%d", game.Player2), fmt.Sprintf("%f", game.Result), fmt.Sprintf("%d", game.TournamentId)}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row to CSV: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	err := saveToS3(buf.Bytes(), fmt.Sprintf("games_%d", batch_id), "csv", "text/csv", "", input)
	if err != nil {
		return fmt.Errorf("failed to save games to S3: %w", err)
	}

	return nil
}


func saveReportsCSVToS3(reports []ReportData, input *HandlerInput, batch_id int) error {

	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)

	header := []string{"PlayerId", "RC", "Score", "N"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header to CSV: %w", err)
	}

	for _, report := range reports {
		row := []string{fmt.Sprintf("%d", report.PlayerId), report.RC, fmt.Sprintf("%f", report.Score), fmt.Sprintf("%d", report.N)}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row to CSV: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	err := saveToS3(buf.Bytes(), fmt.Sprintf("reports_%d", batch_id), "csv", "text/csv", "", input)
	if err != nil {
		return fmt.Errorf("failed to save reports to S3: %w", err)
	}

	return nil
}
