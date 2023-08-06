# mapreduce

## tl;dr
Program begins with the boot of a master process, which initializes and keeps track of a job's state. Workers can then be started, after which the master will assign tasks to each idle worker. When a worker finishes a task, it is back to idle, and can thus receive more tasks. The master first begins by assigning map tasks; only after all map tasks are completed will it start assigning out reduce tasks. Each worker that processes a map task will run the given map function on its input set and write to intermediate output files located on disk. Each worker that processes a reduce task will be given the location of relevant intermediate output files produced by previous map workers and run the given reduce function on those files, writing the final output to a user-specified location.

## Getting started
### Start local mongo
```
if [[ -d /tmp/mongo-testdb ]]; then rm -rf /tmp/mongo-testdb; fi && \
mkdir -p /tmp/mongo-testdb && \
mongod --dbpath /tmp/mongo-testdb
```
