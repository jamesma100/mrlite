use mongodb::{options::ClientOptions, Client};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::process;
use std::process::exit;
use tasks::task_client::TaskClient;
use tasks::TaskRequest;

use serde_json;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

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
        Worker {
            id: id,
            done: done,
        }
    }
    pub fn get_id(&self) -> u32 {
        self.id
    }
    pub fn done(&self) -> bool {
        self.done
    }

    pub async fn boot(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("DEBUG: Worker boot called");

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

        let n_reduce =
            mongo_utils::get_val(&db_client, db_name, coll_name, record_name, "n_reduce")
                .await
                .unwrap();

        let mut client = TaskClient::connect("http://[::1]:50051").await?;
        // create new request
        let request = tonic::Request::new(TaskRequest { num_tasks: 100 });

        let response = client.send_task(request).await?;
        println!("RESPONSE={:?}", response);
        let is_map = response.get_ref().is_map;
        if is_map {
            let file_name = &response.get_ref().file_name;
            let tasknum = &response.get_ref().tasknum;
            let reduce_tasknum = calculate_hash(file_name) % n_reduce as u64;
            map_file(file_name, format!("map-{}-{}", tasknum, reduce_tasknum))
                .expect("ERROR: Could not complete map task.");
        }
        Ok(())
    }
}

fn map_file(filepath: &str, intermediate_filename: String) -> std::io::Result<()> {
    // Open file, call map function on its contents, and write results to disk
    let file = File::open(filepath)?; // for error handling
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;
    let kv_pairs = map(&contents);
    let j = serde_json::to_string(&kv_pairs).unwrap();
    let mut intermediate_file = File::create(&intermediate_filename)?;
    intermediate_file.write_all(j.as_bytes())?;

    Ok(())
}

fn map(contents: &str) -> HashMap<&str, u32> {
    let mut kv_pairs = HashMap::new();
    let mut iter = contents.split_whitespace();
    loop {
        let word = iter.next();
        if word == None {
            break;
        } else {
            kv_pairs.insert(word.unwrap(), 1);
        }
    }
    kv_pairs
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
    worker.boot().await.expect("ERROR: Could not boot worker process.");
}
