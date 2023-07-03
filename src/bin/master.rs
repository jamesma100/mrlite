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
}
impl Master {
	pub fn new(name: String, done: bool) -> Master {
		Master {
			name: name,
			done: done,
		}
	}
	pub fn done(&self) -> bool {
		self.done
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

fn main() {
	println!("{}", "starting master".bright_red());

	let master: Master = Master::new(
		"mymaster".to_string(),
		false,
	);
	master.boot();
	println!("{}: {}", "master created".bright_red(), master.name.bright_red());
	
	let five_seconds = time::Duration::from_millis(5000);
	while !master.done() {
		println!("master not done!");
		thread::sleep(five_seconds);
	}
}
