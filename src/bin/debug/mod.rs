use colored::Colorize;

pub fn print_red(s: &str, arg: &str) {
	println!("{}:{}", s.bright_red(), arg.bright_red())
}

pub fn print_green(s: String) {
	println!("{}", s.bright_green())
}

pub fn print_yellow(s: String) {
	println!("{}", s.bright_yellow())
}
