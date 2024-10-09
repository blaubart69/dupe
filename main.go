package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

type HashGroup struct {
	hash      string
	filesize  string
	filenames []string
}

func enumHardlinksOfFile(filename string) {
	buf := make([]uint16, 2)
	filenames, err := findHardlinks(filename, &buf)
	if err != nil {
		log.Fatal(err)
	} else {
		for _, name := range filenames {
			fmt.Println(name)
		}
	}
}

func GroupHashes(hashReader *csv.Reader, fileGroupChan chan<- HashGroup) {

	defer close(fileGroupChan)

	lastHash := ""
	lastFilesize := ""
	filenameGroup := make([]string, 0)

	for {
		record, err := hashReader.Read()
		if err == io.EOF {
			if len(filenameGroup) > 1 {
				fileGroupChan <- HashGroup{
					hash:      lastHash,
					filesize:  lastFilesize,
					filenames: filenameGroup,
				}
			}
			break
		} else if err != nil {
			log.Fatal(err)
			/*	2024-10-08 Spindler
							this is check by the GO-CSV reader.
							see: reader.FieldsPerRecord = 3
				} else if len(record) != hashReader.FieldsPerRecord {
					log.Fatalf("fields/record is set to %d, but we got %d. %v\n", hashReader.FieldsPerRecord, len(record), record)
			*/
		} else {
			currHash := record[0]
			currFilesize := record[1]

			if currHash != lastHash {
				if len(filenameGroup) > 1 {
					fileGroupChan <- HashGroup{
						hash:      lastHash,
						filesize:  lastFilesize,
						filenames: filenameGroup,
					}
				}
				filenameGroup = make([]string, 0, 2)
			}

			filenameGroup = append(filenameGroup, record[2])
			lastHash = currHash
			lastFilesize = currFilesize
		}
	}
}

func findFirstExistingFile(rootDir string, files []string, hardlinkBuffer *[]uint16) (string, []string, error) {

	filesNotFound := 0
	for _, file := range files {
		if hardlinknames, err := findHardlinks(filepath.Join(rootDir, file), hardlinkBuffer); err != nil {
			if err == syscall.ERROR_FILE_NOT_FOUND {
				filesNotFound += 1
			} else {
				log.Printf("E: enumHardlinks %s: %s\n", file, err)
				return "", nil, err
			}
		} else {
			return file, hardlinknames, nil
		}
	}

	allFilenames := "  " + strings.Join(files, "\n  ")
	// when we reach this code, no files of this group exist in the filesystem
	return "", nil, fmt.Errorf("all files of the group are missing on disk: \n" + allFilenames)
}

func ProcessGroup(fileGroupChan <-chan HashGroup, rootDir string, wg *sync.WaitGroup) {
	defer wg.Done()
	hardlinkBuffer := make([]uint16, 512)
	for hashGroup := range fileGroupChan {
		if len(hashGroup.filenames) < 2 {
			log.Printf("E: INTERNAL ERROR! group has only %d filenames. Should be more than 1. But group-by is done by this program.\n", len(hashGroup.filenames))
		} else if file2linkto, hardlinks, err := findFirstExistingFile(rootDir, hashGroup.filenames, &hardlinkBuffer); err != nil {
			log.Printf("E: skipping filegroup because of: %s\n", err)
		} else {
			if len(hardlinks) == len(hashGroup.filenames) {
				log.Printf("I: all hardlinks ok. len(hardlinks) == len(fileGroup)")
			} else {
				file2linkto := filepath.Join(rootDir, file2linkto)
				log.Printf("I: will create hardlinks to file %s. number existing links (minus itself): %d\n", file2linkto, len(hardlinks)-1)
			}
		}
	}
}

func main() {

	workers := flag.Int("w", runtime.NumCPU(), "number of workers")
	inputfilename := flag.String("f", "hashes_sorted.txt", "hashes file generated with hashp.exe, BUT sorted by hash column. CSV: hash,filesize,filename (TAB separated)")
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

	fileGroupChan := make(chan HashGroup)

	var hashesRootDir string
	if strings.HasSuffix(*rootDir, string(filepath.Separator)) {
		hashesRootDir = *rootDir
	} else {
		hashesRootDir = *rootDir + string(filepath.Separator)
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go ProcessGroup(fileGroupChan, hashesRootDir, &wg)
	}

	GroupHashes(reader, fileGroupChan)

	wg.Wait()
}
