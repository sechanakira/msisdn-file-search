package main

import (
	"fmt"
	"log"
	"math"
	"os"
)

var fileDir string = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\data.txt"

var outputDir = "C:\\Users\\QXZ1TD8\\Desktop\\SuRE\\working_dir\\output_dir"

func main() {
	splitFile()
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

}
