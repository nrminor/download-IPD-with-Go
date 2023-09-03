package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

func determineStartingPoint(dir string, gene string, date string) []int {

	// Parse the provided date
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		fmt.Println("Error parsing date:", err)
	}

	// Make empty slice to store IDs
	var allIntegers []int

	// Define the path to the JSON file
	jsonFilePath := filepath.Join(dir, fmt.Sprintf("%s_date_lookup.json", gene))

	// Check if the file exists
	if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {
		return allIntegers
	}

	// Read the JSON file
	jsonFile, err := os.Open(jsonFilePath)
	if err != nil {
		fmt.Println("Error opening JSON file:", err)
		return allIntegers
	}
	defer jsonFile.Close()

	// Parse the JSON into a map
	byteValue, _ := io.ReadAll(jsonFile)
	var dateLookup map[string]string
	json.Unmarshal(byteValue, &dateLookup)

	// Initialize variables to keep track of the nearest date and corresponding key
	var nearestDate time.Time

	// Loop through the map to find the nearest prior date
	for _, value := range dateLookup {
		dateValue, err := time.Parse("02/01/2006", value)
		if err != nil {
			fmt.Println("Error parsing date in JSON:", err)
			continue
		}

		// Check if the date is prior and nearer to the query date
		if dateValue.Before(t) && (nearestDate.IsZero() || dateValue.After(nearestDate)) {
			nearestDate = dateValue
		}
	}

	if !nearestDate.IsZero() {
		for key, value := range dateLookup {
			dateValue, err := time.Parse("02/01/2006", value)
			if err != nil {
				continue
			}
			if dateValue.Equal(nearestDate) {
				// Convert the remaining part of the key to an integer and add it to allIntegers
				intValue, err := strconv.Atoi(key[3:])
				if err == nil {
					allIntegers = append(allIntegers, intValue)
				}
			}
		}
	}

	sort.Ints(allIntegers)

	return allIntegers
}

func buildDateLookup(id string, dateStr string, lookupDir string, gene string) error {

	jsonFilePath := filepath.Join(lookupDir, fmt.Sprintf("%s_date_lookup.json", gene))

	// Initialize file lock
	fileLock := flock.New(jsonFilePath)

	// Try to lock with a retry mechanism
	retryCount := 0
	maxRetries := 5
	sleepDuration := 2 * time.Second

	var locked bool
	var lockErr error
	for retryCount < maxRetries {
		locked, lockErr = fileLock.TryLock()
		if locked {
			break
		}
		if lockErr != nil {
			return fmt.Errorf("failed to lock the file: %v", lockErr)
		}

		// Sleep for a while before retrying
		time.Sleep(sleepDuration)
		retryCount++
	}

	if !locked {
		return fmt.Errorf("could not obtain lock after %d retries", maxRetries)
	}

	defer fileLock.Unlock()

	dateMap := make(map[string]string)

	jsonFile, err := os.Open(jsonFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to open JSON file: %v", err)
	}
	defer jsonFile.Close()

	if jsonFile != nil {
		byteValue, _ := io.ReadAll(jsonFile)
		json.Unmarshal(byteValue, &dateMap)
	}

	dateMap[id] = dateStr

	jsonData, err := json.MarshalIndent(dateMap, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	savePath := jsonFilePath
	if os.IsNotExist(err) {
		savePath = "date_lookup.json"
	}

	err = os.WriteFile(savePath, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write JSON file: %v", err)
	}

	return nil
}

func defineUrls(startingIDs []int, maxCount int, gene string) ([]string, []string) {

	// Find the maximum integer in the array
	maxInteger := math.MinInt64 // Start with the smallest possible int64
	for _, num := range startingIDs {
		if num > maxInteger {
			maxInteger = num
		}
	}

	// If the array is empty, initialize maxInteger to 0
	if maxInteger == math.MinInt64 {
		maxInteger = 0
	}

	// Append missing integers between maxInteger and maxCount
	for i := maxInteger + 1; i <= maxCount; i++ {
		startingIDs = append(startingIDs, i)
	}

	var urls []string
	var ids []string

	if gene == "MHC" || gene == "KIR" || gene == "HLA" || gene == "MHCPRO" || gene == "KIRPRO" {

		baseURL := ""
		prefix := "NHP"
		if gene == "MHC" {
			baseURL = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdmhc;id="
		} else if gene == "KIR" {
			baseURL = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdnhkir;id="
		} else if gene == "HLA" {
			baseURL = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=imgthla;id="
			prefix = "HLA"
		} else if gene == "MHCPRO" {
			baseURL = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdmhcpro;id="
		} else if gene == "KIRPRO" {
			baseURL = "https://www.ebi.ac.uk/Tools/dbfetch/dbfetch?db=ipdnhkirpro;id="
		}

		for _, i := range startingIDs {
			id := fmt.Sprintf("%s%05d", prefix, i)
			url := baseURL + id + ";style=raw"

			ids = append(ids, id)
			urls = append(urls, url)
		}
	}

	return urls, ids

}

// checkAndSaveFile reads the date 'DT' line of the given file, parses the date, and saves the file if the date is after dateStr.
func checkAndSaveFile(id string, lookupIDs []string, fileContent io.Reader, dateStr string, lookup_dir string, gene string) error {
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
		} else if strings.HasPrefix(line, ">") {
			idLine = line
		}
		if strings.HasPrefix(line, "DT") {
			// fmt.Println("Found line starting with 'DT':", line)
			dateLine = line
		}
	}

	if idLine == "ERROR 12 No entries found." {
		return nil
	}

	if strings.HasPrefix(idLine, ">") {

		tmp_str := strings.Split(idLine, " ")[0]
		idInFileStr := strings.Split(tmp_str, ":")[1]
		// define the output file name for a protein FASTA
		outputFilename := fmt.Sprintf("%s.fasta", idInFileStr)
		outputFile, err := os.Create(outputFilename)
		if err != nil {
			return err
		}
		defer outputFile.Close()
		_, err = io.Copy(outputFile, &fileContentBuffer)
		return err
	}

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

	// if an id isn't in the date lookup, add to it
	found := false
	for _, str := range lookupIDs {
		if str == idInFileStr {
			found = true
			break
		}
	}
	if !found {
		err := buildDateLookup(idInFileStr, dateInFileStr, lookup_dir, gene)
		if err != nil {
			return err
		}
	}

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

	return nil
}

func downloadFile(url string, id string, lookup_records []int, wg *sync.WaitGroup, dateStr string, lookup_dir string, gene string) error {
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
		return fmt.Errorf("FAILED TO FETCH URL AFTER RETRIES: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RECEIVED UNEXPECTED STATUS CODE: %d", resp.StatusCode)
	}

	// Create a new slice to store the string IDs
	var allIDs []string
	if len(lookup_records) > 0 {

		// define ID prefix
		prefix := "NHP"
		if strings.Contains(url, "imgthla") {
			prefix = "HLA"
		}

		// Loop through lookup_records and convert each integer to a string ID
		for _, i := range lookup_records {
			id := fmt.Sprintf("%s%05d", prefix, i)
			allIDs = append(allIDs, id)
		}
	}

	// Check and save the file if conditions are met
	return checkAndSaveFile(id, allIDs, resp.Body, dateStr, lookup_dir, gene)
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
	lookup_dir := os.Args[4]

	// Define a struct to hold URL and ID
	type UrlWithID struct {
		Url string
		ID  string
	}

	starterIDs := determineStartingPoint(lookup_dir, gene, lastReleaseDate)

	// retrieve IPD urls and ids
	urls, ids := defineUrls(starterIDs, alleleCount, gene)

	// Define the number of concurrent workers
	const numWorkers = 100

	// Create a channel to send UrlWithID structs to workers
	urlsChannel := make(chan UrlWithID, numWorkers)

	// Create a wait group to wait for all workers to complete
	var wg sync.WaitGroup

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for urlWithID := range urlsChannel {
				url, id := urlWithID.Url, urlWithID.ID
				err := downloadFile(url, id, starterIDs, &wg, lastReleaseDate, lookup_dir, gene)
				if err != nil {
					fmt.Printf("Error downloading URL %s: %v\n", url, err)
				}
			}
		}()
	}

	// Send UrlWithID structs to the channel
	for i, url := range urls {
		urlWithID := UrlWithID{Url: url, ID: ids[i]}
		urlsChannel <- urlWithID
	}
	close(urlsChannel)

	// Wait for all workers to complete
	wg.Wait()

	fmt.Println("Download completed.")
}
