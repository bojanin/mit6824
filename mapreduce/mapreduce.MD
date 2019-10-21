# Notes on Map Reduce

*[Click here to read paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

### MR

Makes big cluster computation popular,
not efficient or flexible,
scales well

Map() and Reduce() is written by the user.

Abstract view of MapReduce
  input is divided into M files
  [diagram: maps generate rows of K-V pairs, reduces consume columns]
  Input1 -> Map -> a,1 b,1 c,1
  Input2 -> Map ->     b,1
  Input3 -> Map -> a,1     c,1
                    |   |   |
                    |   |   -> Reduce -> c,2
                    |   -----> Reduce -> b,2
                    ---------> Reduce -> a,2
  MR calls Map() for each input file, produces set of k2,v2
    "intermediate" data
    each Map() call is a "task"
  MR gathers all intermediate v2's for a given k2,
    and passes them to a Reduce call
  final output is set of <k2,v3> pairs from Reduce()
    stored in R output files
  [diagram: MapReduce API --
   map(k1, v1) -> list(k2, v2)
   reduce(k2, list(v2) -> list(k2, v3)]

Example: word count
  input is thousands of text files
  Map(k, v) where key = file_name.txt, value = all their words
    split v into words
    for each word w
      emit(w, "1")
  Reduce(k, v)
    emit(len(v)) 
    
### Load balance
Give workers tasks limited to the speed of the host servers.
Faster servers == bigger tasks, this ensures everything finishes at the same time.


### Fault tolerance
If server blows up during data processing, just restart it
output stays the same



