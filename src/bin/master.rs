extern crate mongo_utils;

use mongo_utils::update_assigned;
use mongodb::{options::ClientOptions, Client};
use std::collections::HashMap;
use std::env;
use std::process::exit;
use std::str::FromStr;
use tasks::task_server::{Task, TaskServer};
use tasks::{TaskRequest, TaskResponse};
use tonic::{transport::Server, Request, Response, Status};
pub mod tasks {
    tonic::include_proto!("tasks");
}

#[derive(Debug, Default)]
pub struct TaskService {}

#[tonic::async_trait]
impl Task for TaskService {
    async fn send_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<TaskResponse>, Status> {
        println!("DEBUG: Master got a request: {:?}", request);

        // Initialize client handler
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap_or_else(|err| {
                eprintln!("ERROR: could not parse address: {err}");
                exit(1)
            });
        let client = Client::with_options(client_options).unwrap_or_else(|err| {
            eprintln!("ERROR: could not initialize client: {err}");
            exit(1)
        });
        let db = client.database("mapreduce");

        // Get existing map tasks from database
        let coll = db.collection::<mongodb::bson::Document>("map_tasks");

        // Loop over all tasks looking for an idle one to assign
        let distinct = coll.distinct("name", None, None).await;
        for key in distinct.unwrap() {
            let res =
                mongo_utils::get_task(&client, "mapreduce", "map_tasks", key.as_str().unwrap())
                    .await;

            // If not assigned, hand out this task
            if !(res.1.unwrap()) {
                let response_filename = &res.0.unwrap();
                let tasknum = res.3.unwrap();
                let reply = TaskResponse {
                    file_name: response_filename.to_string(),
                    is_assigned: false,
                    is_map: true,
                    tasknum: tasknum,
                };
                update_assigned(&client, "mapreduce", "map_tasks", response_filename, true).await;

                return Ok(Response::new(reply));
            } else {
                continue;
            }
        }

        // No avaialble tasks found; either mapreduce is done or all map tasks are still in progress
        return Err(Status::not_found("No valid task found."));
    }
}

#[derive(Debug)]
pub struct Master<'a> {
    name: &'a str,
}
impl Master<'_> {
    pub fn new(name: &str) -> Master {
        Master { name: name }
    }
    pub fn get_name(&self) -> &str {
        self.name
    }
    pub async fn boot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "[::1]:50051".parse()?;
        let task_service = TaskService::default();

        Server::builder()
            .add_service(TaskServer::new(task_service))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let n_map: i64 = FromStr::from_str(&args[1]).unwrap();
    let n_reduce: i64 = FromStr::from_str(&args[2]).unwrap();
    let mut map_tasks: HashMap<String, (bool, bool)> = HashMap::new();
    // let mut reduce_tasks: HashMap<String, (bool, bool)> = HashMap::new();

    for i in 3..args.len() {
        map_tasks.insert(args[i].clone(), (false, true));
    }

    let master: Master = Master::new("mymaster");

    let client_options = ClientOptions::parse("mongodb://localhost:27017")
        .await
        .unwrap_or_else(|err| {
            eprintln!("ERROR: could not parse address: {err}");
            exit(1)
        });
    let client = Client::with_options(client_options).unwrap_or_else(|err| {
        eprintln!("ERROR: could not initialie client: {err}");
        exit(1)
    });

    mongo_utils::create_collection(&client, "mapreduce", "state").await;
    mongo_utils::init_master_state(
        &client,
        "mapreduce",
        "state",
        "current_master_state",
        n_map,
        n_reduce,
    )
    .await;

    mongo_utils::init_map_tasks(&client, "mapreduce", "map_tasks", &map_tasks).await;

    master.boot().await.expect("ERROR: Could not boot master process.");

    // // Poll master every 5 seconds to check completion status
    // let five_seconds = time::Duration::from_millis(5000);
    // while !master.done() {
    //     println!("master not done!");
    //     thread::sleep(five_seconds);
    // }

    Ok(())
}
