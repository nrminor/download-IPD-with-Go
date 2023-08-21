package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func defineUrls(maxCount int, gene string) []string {

	if gene == "MHC" {

		result := make([]string, maxCount)
		for i := 1; i <= maxCount; i++ {
			nhpID := fmt.Sprintf("NHP%05d", i)
			result[i-1] = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdmhc;id=" + nhpID + ";style=raw"
		}
		return result

	} else if gene == "KIR" {

		result := make([]string, maxCount)
		for i := 1; i <= maxCount; i++ {
			nhpID := fmt.Sprintf("NHP%05d", i)
			result[i-1] = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdnhkir;id=" + nhpID + ";style=raw"
		}
		return result
	}

	// Return an empty slice if gene is neither "MHC" nor "KIR"
	return []string{}

}

// checkAndSaveFile reads the date 'DT' line of the given file, parses the date, and saves the file if the date is after dateStr.
func checkAndSaveFile(fileContent io.Reader, dateStr string) error {
	// Parse the given date string
	givenDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(fileContent)

	// collect the idline and dateline
	var dateLine string
	var idLine string
	var fileContentBuffer bytes.Buffer
	for scanner.Scan() {
		line := scanner.Text()
		fileContentBuffer.WriteString(line + "\n") // Save the content to a buffer
		if strings.HasPrefix(line, "ERROR") {
			idLine = line
			dateLine = line
			break
		}
		if strings.HasPrefix(line, "ID") {
			// fmt.Println("Found line starting with 'ID':", line)
			idLine = line
		}
		if strings.HasPrefix(line, "DT") {
			// fmt.Println("Found line starting with 'DT':", line)
			dateLine = line
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error finding date line:", err)
	}

	if idLine != "ERROR 12 No entries found." {
		// parse the date line
		dateLine = strings.TrimPrefix(dateLine, "DT")
		dateLine = strings.TrimSpace(dateLine)       // Remove any leading or trailing spaces
		dateInFileStr := strings.Fields(dateLine)[0] // Get the first field, which should be the date
		dateInFile, err := time.Parse("02/01/2006", dateInFileStr)
		if err != nil {
			return err
		}

		// parse the ID line
		idLine = strings.TrimPrefix(idLine, "ID")
		idLine = strings.TrimSpace(idLine)
		idInFileStr := strings.Split(idLine, ";")[0]
		idInFileStr = strings.TrimSpace(idInFileStr)

		// define the output file name
		outputFilename := fmt.Sprintf("%s.embl", idInFileStr)

		// Compare the dates and save the file if the date in the file is after the given date
		if dateInFile.After(givenDate) {
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				return err
			}
			defer outputFile.Close()
			_, err = io.Copy(outputFile, &fileContentBuffer)
			return err
		}
	}

	return nil
}

func downloadFile(url string, wg *sync.WaitGroup, dateStr string) error {
	// defer wg.Done() // Decrease counter when the goroutine completes

	var resp *http.Response
	var err error

	// Retry up to 3 times
	for retries := 0; retries < 3; retries++ {
		resp, err = http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			break // Success, exit the retry loop
		}
		if err != nil {
			fmt.Printf("Error fetching URL: %s. Retrying...\n", err)
		} else {
			fmt.Printf("Received unexpected status code: %d. Retrying...\n", resp.StatusCode)
		}
		time.Sleep(2 * time.Second) // Wait before retrying
	}

	if err != nil {
		return fmt.Errorf("Failed to fetch URL after retries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Received unexpected status code: %d", resp.StatusCode)
	}

	// Check and save the file if conditions are met
	return checkAndSaveFile(resp.Body, dateStr)
}

func main() {

	// Check if enough arguments are provided
	if len(os.Args) < 4 {
		fmt.Println("Usage: download-ipd-alleles.go <gene> <number to download> <last IPD release date>")
		return
	}

	// Assign command-line arguments to variables
	gene := os.Args[1]
	alleleCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Error converting '%s' to an integer: %v\n", os.Args[2], err)
		return
	}
	lastReleaseDate := os.Args[3]

	urls := defineUrls(alleleCount, gene)

	// Define the number of concurrent workers
	const numWorkers = 100

	// Create a channel to send URLs to workers
	urlsChannel := make(chan string, numWorkers)

	// Create a wait group to wait for all workers to complete
	var wg sync.WaitGroup

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range urlsChannel {
				err := downloadFile(url, &wg, lastReleaseDate)
				if err != nil {
					fmt.Printf("Error downloading URL %s: %v\n", url, err)
				}
			}
		}()
	}

	// Send URLs to the channel
	for _, url := range urls {
		urlsChannel <- url
	}
	close(urlsChannel)

	// Wait for all workers to complete
	wg.Wait()

	fmt.Println("Download completed.")
}
