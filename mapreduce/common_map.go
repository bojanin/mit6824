package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// Import read files
	buf, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Panicf("File could not be read doMap %v", err)
	}

	// Holds pointers to all file encoders
	var encoders []*json.Encoder

	// Loop that adds files and their associated encoders to disk
	for i := 0; i < nReduce; i++ {
		fName := reduceName(jobName, mapTask, i)

		f, err := os.Create(fName)
		defer f.Close()
		if err != nil {
			log.Panicf("Error creating file doMap: %v\n", err)
		}

		enc := json.NewEncoder(f)
		encoders = append(encoders, enc)
	}
	keyVals := mapF(inFile, string(buf))

	for _, kv := range keyVals {
		fileIndex := ihash(kv.Key) % nReduce
		encoders[fileIndex].Encode(&kv)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
