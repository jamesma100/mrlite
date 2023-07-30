extern crate mongo_utils;

use colored::Colorize;
use mongo_utils::create_collection;
use mongodb::bson::doc;
use mongodb::{options::ClientOptions, Client};
use std::collections::HashMap;
use std::str::FromStr;
use std::{env, thread, time};
use tasks::task_server::{Task, TaskServer};
use tasks::{TaskRequest, TaskResponse};
use tonic::{transport::Server, Request, Response, Status};

// types tonic generated based on proto/tasks.proto
// so the `use` statements can reference them
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
        println!("master got a request: {:?}", request);

        let req = request.into_inner();

        let reply = TaskResponse {
            file_name: "sample_file".to_string(),
            task_num: 1,
            n_reduce: 3,
            n_map: 4,
            is_map: true,
        };

        Ok(Response::new(reply))
    }
}

#[derive(Debug)]
pub struct Master {
    name: String,
    done: bool,
    n_map: u32,
    n_reduce: u32,
    map_tasks_left: u32,
    // hashmap for storing the status of tasks / files
    // 0: unassigned
    // 1: assigned
    // tuple def: (assigned, is_map)
    map_tasks: HashMap<String, (bool, bool)>,
    reduce_tasks: HashMap<String, (bool, bool)>,
}
impl Master {
    pub fn new(
        name: String,
        done: bool,
        n_map: u32,
        n_reduce: u32,
        map_tasks_left: u32,
        map_tasks: HashMap<String, (bool, bool)>,
        reduce_tasks: HashMap<String, (bool, bool)>,
    ) -> Master {
        Master {
            name: name,
            done: done,
            n_map: n_map,
            n_reduce: n_reduce,
            map_tasks_left: map_tasks_left,
            map_tasks: map_tasks,
            reduce_tasks: reduce_tasks,
        }
    }
    pub fn done(&self) -> bool {
        self.done
    }

    pub async fn boot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "[::1]:50051".parse()?;
        println!("addr: {}", addr);
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

    let n_map: u32 = FromStr::from_str(&args[1]).unwrap();
    let n_reduce: u32 = FromStr::from_str(&args[2]).unwrap();
    let mut map_tasks: HashMap<String, (bool, bool)> = HashMap::new();
    let mut reduce_tasks: HashMap<String, (bool, bool)> = HashMap::new();

    for i in 3..args.len() {
        map_tasks.insert(args[i].clone(), (false, true));
    }

    println!("{}", "starting master".bright_red());

    let mut master: Master = Master::new(
        "mymaster".to_string(),
        false,
        n_map,
        n_reduce,
        n_map,
        map_tasks,
        reduce_tasks,
    );
    println!("printing master: {:?}", master);
    let addr = "[::1]:50051";

    let client_options = ClientOptions::parse("mongodb://localhost:27017")
        .await
        .unwrap();
    let client = Client::with_options(client_options).unwrap();

    let db_name = "my_database";
    let coll_name = "my_collection";

    mongo_utils::create_collection(&client, db_name, coll_name).await;
    master.boot().await;
    println!(
        "{}: {}",
        "master created".bright_red(),
        master.name.bright_red()
    );

    let five_seconds = time::Duration::from_millis(5000);
    while !master.done() {
        println!("master not done!");
        thread::sleep(five_seconds);
    }

    Ok(())
}
