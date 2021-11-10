package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
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

func main() {
	splitFile()
}

func msisdnSearch(msisdn string) (error, subscriberData) {
	return nil, subscriberData{}
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
