package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

func GroupHashes(hashReader *csv.Reader, fileGroupChan chan<- []string) {

	lastHash := ""
	fileGroup := make([]string, 0)

	for {
		record, err := hashReader.Read()
		if err == io.EOF {
			if len(fileGroup) > 1 {
				fileGroupChan <- fileGroup
			}
			break
		} else if err != nil {
			log.Fatal(err)
		} else if len(record) != hashReader.FieldsPerRecord {
			log.Fatalf("fields/record is set to %d, but we got %d. %v\n", hashReader.FieldsPerRecord, len(record), record)
		} else {
			currHash := record[0]
			currFilename := record[2]

			if currHash != lastHash {
				if len(fileGroup) > 1 {
					fileGroupChan <- fileGroup
				}
				fileGroup = make([]string, 0, 2)
			}
			fileGroup = append(fileGroup, currFilename)
		}
	}
}

// the idea:
//
//	read the hardlinked filenames of one file of the group
//	--> should be the same filenames as in the fileGroup
func ProcessGroup(fileGroupChan <-chan []string, rootDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	//for fileGroup := range fileGroupChan {

}

func enumHardlinksOfFile(filename string) {
	buf := make([]uint16, 1024)
	filenames, err := findHardlinks(filename, &buf)
	if err != nil {
		log.Fatal(err)
	} else {
		for _, name := range filenames {
			fmt.Println(name)
		}
	}
}

func main() {

	workers := flag.Int("w", runtime.NumCPU(), "number of workers")
	inputfilename := flag.String("f", "hashes_sorted.txt", "hashes file generated with hashp.exe. CSV: hash,filesize,filename (TAB separated)")
	rootDir := flag.String("r", "\\", "root directory of the files given in inputfile")
	enumHardlinks := flag.String("e", "", "enumerate hardlinks of filename")
	flag.Parse()

	if *enumHardlinks != "" {
		enumHardlinksOfFile(*enumHardlinks)
		os.Exit(0)
	}

	fp, err := os.Open(*inputfilename)
	if err != nil {
		log.Fatal(err)
		os.Exit(4)
	}

	reader := csv.NewReader(fp)
	reader.Comma = '\t'
	reader.FieldsPerRecord = 3
	reader.ReuseRecord = true

	fileGroupChan := make(chan []string)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go ProcessGroup(fileGroupChan, *rootDir, &wg)
	}

	GroupHashes(reader, fileGroupChan)

	wg.Wait()
}
