use std::{thread, time};
use colored::Colorize;
use tonic::{transport::Server, Request, Response, Status};
use tasks::task_server::{Task, TaskServer};
use tasks::{TaskRequest, TaskResponse};

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
}
impl Master {
	pub fn new(name: String, done: bool, n_map: u32, n_reduce: u32, map_tasks_left:u32) -> Master {
		Master {
			name: name,
			done: done,
			n_map: n_map,
			n_reduce: n_reduce,
			map_tasks_left: map_tasks_left,
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
async fn main() {
	println!("{}", "starting master".bright_red());

	let master: Master = Master::new(
		"mymaster".to_string(),
		false,
		10,
		10,
		10,
	);
	master.boot().await;
	println!("{}: {}", "master created".bright_red(), master.name.bright_red());
	
	let five_seconds = time::Duration::from_millis(5000);
	while !master.done() {
		println!("master not done!");
		thread::sleep(five_seconds);
	}
}
