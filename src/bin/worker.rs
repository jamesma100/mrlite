use mongo_utils::{
    create_collection, drop_collection, get_task, get_val, init_map_tasks, init_master_state,
    update_assigned, update_count,
};
use mongodb::bson::doc;
use mongodb::{options::ClientOptions, Client};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::process;
use tasks::task_client::TaskClient;
use tasks::TaskRequest;

pub mod tasks {
    tonic::include_proto!("tasks");
}

#[derive(Debug)]
pub struct Worker {
    name: String,
    done: bool,
}
impl Worker {
    pub fn new(name: String, done: bool) -> Worker {
        Worker {
            name: name,
            done: done,
        }
    }
    pub fn done(&self) -> bool {
        self.done
    }

    pub async fn boot(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("worker boot called");

        // Retrieve some information about the current master state that the workers
        // share. This seems kinda expensive since we are initializing a new
        // database connection per request we serve - it would be nice if these
        // values could persist per worker session.
        //
        // Can also considering making n_map and n_reduce part of the RPC response object
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap();
        let db_client = Client::with_options(client_options).unwrap();
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
        let request = tonic::Request::new(TaskRequest { num_tasks: 100 });

        let mut response = client.send_task(request).await?;
        println!("RESPONSE={:?}", response);
        let is_map = response.get_mut().is_map;
        if is_map {
            let file_name = &response.get_mut().file_name;
            let hash = calculate_hash(file_name) % n_reduce as u64;
            println!("file_name: {}", file_name);
            println!("hash: {}", hash);
        }
        Ok(())
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut default_hasher = DefaultHasher::new();
    t.hash(&mut default_hasher);
    default_hasher.finish()
}

#[tokio::main]
async fn main() {
    // initialize worker
    let worker_name = format!("{}-{}", "worker", process::id());
    let worker: Worker = Worker::new(worker_name, false);
    worker.boot().await;
}
