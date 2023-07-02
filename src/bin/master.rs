use std::{thread, time};
use colored::Colorize;
pub mod debug;


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
} 

fn main() {
	println!("{}", "starting master".bright_red());

	let master: Master = Master::new(
		"mymaster".to_string(),
		false,
	);
	println!("{}: {}", "master created".bright_red(), master.name.bright_red());
	
	let five_seconds = time::Duration::from_millis(5000);
	while !master.done() {
		println!("master not done!");
		thread::sleep(five_seconds);
	}
}
