package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

var fileDir string = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\data.txt"

var outputDir = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\output_dir\\"

type subscriberData struct {
	msisdn          string
	countoforigin   string
	dob             string
	dod             string
	firstname       string
	lastname        string
	issuingCountry  string
	registrarstatus string
	status          string
	postaladdress   string
	physicaladdress string
	title           string
	companyName     string
	gender          string
	occupation      string
}

type searchResult struct {
	fileName string
	found    bool
	content  string
}

func main() {
	entries, err := os.ReadDir(outputDir)

	if err != nil {
		log.Fatal("Failed to read output directory: ", outputDir)
		os.Exit(1)
	}

	if len(entries) == 0 {
		splitFile()
	}

	sr, _ := msisdnSearch("263775551045")

	fmt.Println(sr)
}

func msisdnSearch(msisdn string) (subscriberData, error) {
	sb := subscriberData{}

	dirEntry, err := os.ReadDir(outputDir)

	c := make(chan searchResult)

	if err != nil {
		log.Fatal("Failed to read from directory: ", outputDir)
	} else {
		for _, entry := range dirEntry {
			go searchFile(entry.Name(), msisdn, c)
		}
	}

	for i := 1; i <= len(dirEntry); i++ {
		sr := <-c

		if sr.found {
			data := strings.Split(strings.ReplaceAll(sr.content, `"`, ""), "|")

			sb.msisdn = data[0]
			sb.countoforigin = data[1]
			sb.dob = data[2]
			sb.dod = data[3]
			sb.firstname = data[4]
			sb.lastname = data[5]
			sb.issuingCountry = data[6]
			sb.registrarstatus = data[7]
			sb.status = data[8]
			sb.postaladdress = data[9]
			sb.physicaladdress = data[10]
			sb.title = data[11]
			sb.companyName = data[13]
			sb.gender = data[14]
			sb.occupation = data[15]
		}
	}

	return sb, nil
}

func searchFile(fileName string, searchParam string, c chan searchResult) {
	bs, err := os.ReadFile(outputDir + fileName)

	sr := searchResult{
		fileName: fileName,
		found:    false,
	}

	if err != nil {
		log.Fatal("Failed to open file: ", fileName)
	} else {
		fileContent := string(bs)

		lines := strings.Split(fileContent, "\r\n")

		for _, line := range lines {

			if strings.Contains(line, searchParam) {
				sr.found = true
				sr.content = line
			}
		}
	}

	c <- sr
}

func splitFile() {
	file, err := os.Open(fileDir)

	if err != nil {
		log.Fatal("Error opening file: ", fileDir)
		os.Exit(1)
	}

	defer file.Close()

	info, _ := file.Stat()

	var fileSize int64 = info.Size()

	const fileChunk = 5 * (1 << 20)

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Spliting into %d parts", totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))

		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		fileName := outputDir + "data_" + strconv.FormatUint(i, 10) + ".txt"
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		fmt.Println("Split to : ", fileName)

	}

}
