use tasks::task_client::TaskClient;
use tasks::TaskRequest;
use std::process;

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
		let mut client = TaskClient::connect(
			"http://[::1]:50051"
		).await?;

		// create new request
		let request = tonic::Request::new(
			TaskRequest {
				num_tasks: 100,	
			}
		);

		let response = client.send_task(request).await?;
		println!("RESPONSE={:?}", response);
		Ok(())

	}
} 

#[tokio::main]
async fn main() {
	println!("hello world");
	// initialize worker
	let worker_name = format!("{}-{}", "worker", process::id());
	let worker: Worker = Worker::new(
		worker_name,
		false,
	);
	worker.boot().await;
}
