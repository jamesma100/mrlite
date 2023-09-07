# mrlite

__mrlite__ is a [light]weight implementation of [M]ap[R]educe that runs on a single machine by utilizing multiple cores. 
__mrlite__ allows dividing up large tasks into smaller ones to be run by many workers in parallel, while coordinating them using a master node.

## Getting started
Due to Rust's borrowing and lifetime rules, it is hard to pass around shared state between processes.
So we maintain a persistent Mongo process throughout the job's execution.
To start one:
```
if [[ -d /tmp/mongo-testdb ]]; then rm -rf /tmp/mongo-testdb; fi && \
mkdir -p /tmp/mongo-testdb && \
mongod --dbpath /tmp/mongo-testdb
```

You can now specify your map and reduce functions in `src/bin/worker.rs`. For example, the default map/reduce functions for word count would look like this:
```
fn map(contents: &str) -> Vec<KVPair> {
    let mut kv_pairs = Vec::new();
    let mut iter = contents.split_whitespace();
    loop {
        let word = iter.next();
        if word.is_none() {
            break;
        } else {
            kv_pairs.push(KVPair {
                key: word.unwrap().to_string(),
                val: 1,
            });
        }
    }
    kv_pairs
}

fn reduce(_key: String, vals: Vec<u64>) -> u64 {
    let mut total = 0;
    for val in vals {
        total += val;
    }
    total
}
```

Then, we can start a master process:
```
cargo run --bin master <n_map> <n_reduce> <filename1> <filename2> <filename3>
```

In a separate terminal, start a worker process:
```
cargo run --bin worker
```
You can run as many worker processes as there are available tasks.
They should be able to safely run in parallel.

When the entire MapReduce job is complete, you should see your output stored in `n_reduce` files named `out-<i>` where `i` is between 0 and `n_reduce`-1.
You will also see many `map-<x>-<y>` files, which are the intermediate files written during the map phase - you can ignore those.

## References
Inspired by [MIT's distributed systems course](http://nil.csail.mit.edu/6.824/2020/) and the original [MapReduce paper](http://nil.csail.mit.edu/6.824/2020/papers/mapreduce.pdf).
