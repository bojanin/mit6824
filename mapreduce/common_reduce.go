package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// Intermediate keyvalues aggregated from all intermediate files
	var keyValues []KeyValue

	// Loop that loads all intermediate keyvalues to memory
	for i := 0; i < nMap; i++ {

		// File opening block - self explanatory
		fName := reduceName(jobName, i, reduceTask)
		f, err := os.Open(fName)
		defer f.Close()
		if err != nil {
			log.Panicf("Error opening file doReduce: %v %v\n", fName, err)
		}

		// Decoding block - also self explanatory
		dec := json.NewDecoder(f)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Printf("invalid JSON doReduce: %v\n", err)
			}
			keyValues = append(keyValues, kv)
		}
	}

	// Dont forget to sort keys
	if !sort.IsSorted(ByKey(keyValues)) {
		sort.Sort(ByKey(keyValues))
	}

	// Create outfile
	outf, err := os.Create(outFile)
	defer outf.Close()
	if err != nil {
		log.Panicf("Error creating file doReduce: %v\n", err)
	}

	// write everything to file
	enc := json.NewEncoder(outf)
	kvs := getKvMap(keyValues)
	for k, v := range kvs {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
}

func getKvMap(kvs []KeyValue) map[string][]string {
	ret := make(map[string][]string)
	for _, kv := range kvs {
		ret[kv.Key] = append(ret[kv.Key], kv.Value)
	}
	return ret
}
