# mapreduce

## tl;dr
Program begins with the boot of a master process, which initializes and keeps track of a job's state.
Workers can then be started, after which the master will assign tasks to each idle worker.
When a worker finishes a task, it is back to idle, and can thus receive more tasks.
The master first begins by assigning map tasks; only after all map tasks are completed will it start assigning out reduce tasks.
Each worker that processes a map task will run the given map function on its input set and write to intermediate output files located on disk.
Each worker that processes a reduce task will be given the location of relevant intermediate output files produced by previous map workers and run the given reduce function on those files, writing the final output to a user-specified location.

## Getting started
Due to Rust's borrowing and lifetime rules, it is hard to pass around shared state between processes.
So we maintain a persistent Mongo process throughout the job's execution.
To start one:
```
if [[ -d /tmp/mongo-testdb ]]; then rm -rf /tmp/mongo-testdb; fi && \
mkdir -p /tmp/mongo-testdb && \
mongod --dbpath /tmp/mongo-testdb
```

Then, we can start a master process:
```
cargo run --bin master <num map tasks> <num reduce tasks> <filename1> <filename2> <filename3>
```

In a separate terminal, start a worker process:
```
cargo run --bin worker
```
You can run as many worker processes as there are available tasks.
They should be able to safely run in parallel.
