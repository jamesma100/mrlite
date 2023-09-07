use mongodb::{options::ClientOptions, Client};
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::io::{BufReader, LineWriter, Read};
use std::path::Path;
use std::process;
use std::process::exit;
use tasks::task_client::TaskClient;
use tasks::TaskRequest;

pub mod tasks {
    tonic::include_proto!("tasks");
}

#[derive(Debug)]
pub struct Worker {
    id: u32,
    done: bool,
}
impl Worker {
    pub fn new(id: u32, done: bool) -> Worker {
        Worker { id, done }
    }
    pub fn get_id(&self) -> u32 {
        self.id
    }
    pub fn done(&self) -> bool {
        self.done
    }

    pub async fn boot(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Retrieve some information about the current master state that the workers
        // share. This seems kinda expensive since we are initializing a new
        // database connection per request we serve - it would be nice if these
        // values could persist per worker session.
        //
        // Can also considering making n_map and n_reduce part of the RPC response object
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap_or_else(|err| {
                eprintln!("ERROR: could not parse address: {err}");
                exit(1)
            });
        let db_client = Client::with_options(client_options).unwrap_or_else(|err| {
            eprintln!("ERROR: could not initialize database client: {err}");
            exit(1)
        });
        let db_name = "mapreduce";
        let coll_name = "state";
        let record_name = "current_master_state";

        let n_map = mongo_utils::get_val(&db_client, db_name, coll_name, record_name, "n_map")
            .await
            .unwrap();
        let n_reduce =
            mongo_utils::get_val(&db_client, db_name, coll_name, record_name, "n_reduce")
                .await
                .unwrap();

        let mut client = TaskClient::connect("http://[::1]:50051").await?;
        // create new request
        let request = tonic::Request::new(TaskRequest { id: process::id() });
        let response = client.send_task(request).await?;

        println!("RESPONSE={:?}", response);

        let is_map = response.get_ref().is_map;
        let task_name = &response.get_ref().task_name;
        let reduce_tasknum = calculate_hash(task_name) % n_reduce as u64;

        if is_map {
            let tasknum = &response.get_ref().tasknum;
            map_file(task_name, format!("map-{}-{}", tasknum, reduce_tasknum))
                .expect("ERROR: Could not complete map task.");
            mongo_utils::update_done(&db_client, "mapreduce", "map_tasks", task_name, true).await;
        } else {
            println!("DEBUG: Client received reduce task.");
            for i in 0..n_map {
                let intermediate_filename = format!("map-{}-{}", i, task_name);
                let path = Path::new(&intermediate_filename);

                if path.exists() {
                    let f = File::open(intermediate_filename)
                        .expect("ERROR: File should open read only");
                    let json: serde_json::Value =
                        serde_json::from_reader(f).expect("file should be proper JSON");
                    let mut kv_pairs = Vec::new();

                    for j in json.as_array().unwrap() {
                        kv_pairs.push(KVPair {
                            key: j["key"].as_str().unwrap().to_string(),
                            val: j["val"].as_u64().unwrap(),
                        });
                    }
                    kv_pairs.sort();

                    let len = kv_pairs.len();
                    let file = File::create(format!("out-{}", task_name))?;
                    let mut file = LineWriter::new(file);
                    let mut i = 0;

                    // Now that the key value pairs are sorted, we can easily coalesce adjacent
                    // pairs if they are equivalent
                    while i < len {
                        let mut j = i + 1;
                        while j < len && kv_pairs[j].key == kv_pairs[i].key {
                            j += 1;
                        }
                        let mut vals = Vec::new();

                        for k in i..j {
                            vals.push(kv_pairs[k].val);
                        }

                        let count = reduce(kv_pairs[i].key.clone(), vals);
                        file.write_all(format!("{} {}\n", kv_pairs[i].key, count).as_bytes())?;
                        i = j;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct KVPair {
    key: String,
    val: u64,
}

// Open file, call map function on its contents, and write results to disk
fn map_file(filepath: &str, intermediate_filename: String) -> std::io::Result<()> {
    let file = File::open(filepath)?; // for error handling
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();

    buf_reader.read_to_string(&mut contents)?;

    let kv_pairs = map(&contents);
    let mut intermediate_file = File::create(intermediate_filename)?;
    let json = serde_json::to_string(&kv_pairs)?;

    intermediate_file.write_all(json.as_bytes())?;

    Ok(())
}

// User defined map function goes here
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

// User defined reduce function goes here
fn reduce(_key: String, vals: Vec<u64>) -> u64 {
    let mut total = 0;
    for val in vals {
        total += val;
    }
    total
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut default_hasher = DefaultHasher::new();
    t.hash(&mut default_hasher);
    default_hasher.finish()
}

#[tokio::main]
async fn main() {
    // Initialize worker
    let worker: Worker = Worker::new(process::id(), false);
    worker
        .boot()
        .await
        .expect("ERROR: Could not boot worker process.");
}
