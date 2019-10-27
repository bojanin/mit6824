# Notes on Map Reduce

*[Click here to read paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

### MR
* Concurrent map reduce implementation.
* fault tolerant by restarting workers when they fail or time out

### useful files

* common_map.go executes the map function on input files in mapreduce
* common_reduce.go executes the reduce function on intermediary file produced by map to create output files.
* schedule.go schedules workers and implements fault tolerance with goroutines and channels
* wc.go mapReduce function implementations that find the key-value pairs of words and their word counts in input files