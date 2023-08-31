# mapreduce

## tl;dr
MapReduce allows dividing up large tasks into smaller ones to be run by many workers in parallel.
MapReduce coordinates the progress shared by workers and the master, which is responsible for assigning tasks, checking to see if workers have died, and keeping track of the state of the entire system.
Usually MapReduce is implemented for large clusters of machines, but this implementation can run one just one machine!

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
