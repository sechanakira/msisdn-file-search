package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	layout    = "2006-01-02 15:04:05"
	fileDir   = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\data.txt"
	outputDir = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\output_dir\\"
	dsn       = "root:changeit@tcp(127.0.0.1:3306)/gorm_start?charset=utf8mb4&parseTime=True&loc=Local"
)

type SubscriberData struct {
	gorm.Model
	Msisdn          string
	CountOfOrigin   string
	Dob             time.Time
	Dod             time.Time
	FirstName       string
	LastName        string
	IssuingCountry  string
	RegistrarStatus string
	Status          string
	PostalAddress   string
	PhysicalAddress string
	Title           string
	CompanyName     string
	Gender          string
	Occupation      string
}

type searchResult struct {
	fileName string
	found    bool
	content  string
}

func main() {
	readAllAndSave()
	entries, err := os.ReadDir(outputDir)

	if err != nil {
		log.Fatal("Failed to read output directory: ", outputDir)
		os.Exit(1)
	}

	if len(entries) == 0 {
		splitFile()
	}

	sr, _ := msisdnSearch("263774344508")

	fmt.Println(sr)
}

func readAllAndSave() {
	db, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	sql, _ := db.DB()

	defer sql.Close()

	db.AutoMigrate(&SubscriberData{})

	entries, err := os.ReadDir(outputDir)

	if err != nil {
		log.Fatalln("Failed to open directory ", outputDir)
	}

	if len(entries) == 0 {
		splitFile()
	}

	c := make(chan bool)

	for _, entry := range entries {
		go readAndSaveFileContents(entry.Name(), c, db)
	}

	for i := 0; i <= len(entries); i++ {
		fmt.Println(<-c)
	}
}

func readAndSaveFileContents(fileName string, c chan bool, db *gorm.DB) {
	bs, _ := os.ReadFile(outputDir + fileName)

	fileContents := string(bs)

	lines := strings.Split(fileContents, "\r\n")

	for _, line := range lines {
		if len(line) != 0 && line != "" {
			sb := SubscriberData{}

			line = strings.ReplaceAll(line, `"`, "")
			data := strings.Split(line, "|")

			sb.Msisdn = data[0]

			if len(data) > 1 {
				sb.CountOfOrigin = data[1]
			}

			if len(data) > 2 {
				dob, _ := time.Parse(layout, data[2])
				sb.Dob = dob
			}

			if len(data) > 3 {
				dod, _ := time.Parse(layout, data[3])
				sb.Dod = dod
			}

			if len(data) > 4 {
				sb.FirstName = data[4]
			}

			if len(data) > 5 {
				sb.LastName = data[5]
			}

			if len(data) > 6 {
				sb.IssuingCountry = data[6]
			}

			if len(data) > 7 {
				sb.RegistrarStatus = data[7]
			}

			if len(data) > 8 {
				sb.Status = data[8]
			}

			if len(data) > 9 {
				sb.PostalAddress = data[9]
			}

			if len(data) > 10 {
				sb.PhysicalAddress = data[10]
			}

			if len(data) > 11 {
				sb.Title = data[11]
			}

			if len(data) > 13 {
				sb.CompanyName = data[13]
			}

			if len(data) > 14 {
				sb.Gender = data[14]
			}

			if len(data) > 15 {
				sb.Occupation = data[15]
			}

			db.Create(&sb)
		}
	}

	c <- true

}

func msisdnSearch(msisdn string) (SubscriberData, error) {
	sb := SubscriberData{}

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

			sb.Msisdn = data[0]
			sb.CountOfOrigin = data[1]
			sb.FirstName = data[4]
			sb.LastName = data[5]
			sb.IssuingCountry = data[6]
			sb.RegistrarStatus = data[7]
			sb.Status = data[8]
			sb.PostalAddress = data[9]
			sb.PhysicalAddress = data[10]
			sb.Title = data[11]
			sb.CompanyName = data[13]
			sb.Gender = data[14]
			sb.Occupation = data[15]

			dob, _ := time.Parse(layout, data[2])
			dod, _ := time.Parse(layout, data[3])

			sb.Dob = dob
			sb.Dod = dod

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
