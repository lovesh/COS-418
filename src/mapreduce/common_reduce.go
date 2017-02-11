	package mapreduce

import (
	"io/ioutil"
	"fmt"
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger then combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	kvs := make(map[string] []string)
	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTaskNumber)
		dat, err := ioutil.ReadFile(fileName)
		if err != nil {
			fmt.Println("Error opening file ", fileName)
		} else {
			var items []KeyValue
			json.Unmarshal(dat, &items)
			for _, item := range items {
				k := item.Key
				v := item.Value
				vals, ok := kvs[k]
				if !ok {
					vals = []string{}
					kvs[k] = vals
				}
				kvs[k] = append(kvs[k], v)
			}
		}
	}
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	file, err := os.Create(mergeFileName)
	if err != nil {
		fmt.Println("Cannot open file ", mergeFileName)
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	enc := json.NewEncoder(file)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, kvs[key])})
	}
	file.Close()
	debug("reduceF wrote %s\n", mergeFileName)
}
