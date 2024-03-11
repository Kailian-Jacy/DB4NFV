use std::{cell::UnsafeCell, collections::HashMap, fs::{self, File}, io::Write, path::Path, sync::atomic::Ordering, thread, time::{Duration, SystemTime}};

use once_cell::sync::OnceCell;

use crate::{config::CONFIG, worker::construct_thread::GRACEFUL_SHUTDOWN};

pub struct Metrics {
	pub ts: u64,
	pub content: String,
}

struct History {
	logs: Vec<Metrics>,
	written: usize,
}

trait Report {
	fn report(&self) -> String;
}

pub struct ThreadLogger {
	role: ThreadRole,
	history: UnsafeCell<History>,
	counter: HashMap<String, UnsafeCell<i32>>,
}

unsafe impl Sync for ThreadLogger {}

impl ThreadLogger {
	pub fn new_worker() -> Self{
		ThreadLogger{
			role: ThreadRole::EXECUTOR,
			history: History{ logs: Vec::with_capacity(100000), written: 0}.into(),
			counter: HashMap::from([
				(String::from("evnode.accept"), UnsafeCell::new(0)),
				(String::from("evnode.abort"), UnsafeCell::new(0)),
			]),
		}	
	}
	pub fn new_constructor() -> Self{
		ThreadLogger{
			role: ThreadRole::CONSTRUCTOR,
			history: History{ logs: Vec::with_capacity(100000), written: 0}.into(),
			counter: HashMap::from([
				(String::from("evnode.let_occupy"), UnsafeCell::new(0)),
				(String::from("evnode.enqueue"), UnsafeCell::new(0)),
				(String::from("rare_condition.claimed_when_counting."), UnsafeCell::new(0)),
			]),
		}	
	}
	pub fn new_vnf() -> Self{
		ThreadLogger{
			role: ThreadRole::VNF,
			history: History{ logs: Vec::with_capacity(100000), written: 0}.into(),
			counter: HashMap::new()
		}	
	}
	pub fn log(&self, m: Metrics) {
		unsafe {
            let history = &mut *self.history.get();
            history.logs.push(m);
        }
	}
	pub fn inc(&self, entry: &str) {
		unsafe {
            let cnt = &mut *self.counter[entry].get();
            *cnt += 1;
        }
	}
}

pub static MONITOR: OnceCell<Vec<ThreadLogger>> = OnceCell::new();

#[derive(Debug)]
enum ThreadRole{
	EXECUTOR,
	CONSTRUCTOR,
	MONITOR,
	VNF
}

pub fn init() {
	// Check if log directory exists, create if not.
    let log_dir = CONFIG.read().unwrap().log_dir.clone(); // Assuming log_dir exists in your config.
    if !Path::new(&log_dir).exists() {
        match fs::create_dir_all(&log_dir) {
            Ok(_) => println!("Log directory created: {}", log_dir),
            Err(err) => panic!("Failed to create log directory: {}", err),
        }
    }

	let mut monitors: Vec::<ThreadLogger> = Vec::new();
	monitors.push(ThreadLogger::new_constructor());

	(1..CONFIG.read().unwrap().vnf_threads_num + 1)
		.into_iter().for_each(
			|_| monitors.push(ThreadLogger::new_vnf())
		);

    let worker_thread_ends = CONFIG.read().unwrap().worker_threads_num // Workers.
        + CONFIG.read().unwrap().vnf_threads_num // vnfs.
        + 1; // Construct thread.
	(CONFIG.read().unwrap().vnf_threads_num + 1..worker_thread_ends)
		.into_iter().for_each(
			|_| monitors.push(ThreadLogger::new_worker())
		);
	let _ = MONITOR.set(monitors);
}

pub fn monitor_thread(tid: usize){
	let his_file_path = format!("{}/history.csv", CONFIG.read().unwrap().log_dir);
	let cnt_file_path = format!("{}/cnt.csv", CONFIG.read().unwrap().log_dir);

	// Open log file for writing.
    let mut his_log = match File::create(&his_file_path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to create log file {}: {}", his_file_path, err);
            return;
        }
    };

    let mut cnt_log = match File::create(&cnt_file_path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to create log file {}: {}", cnt_file_path, err);
            return;
        }
    };

	// Collect data and write to log file every 1 second.
    loop {
        if GRACEFUL_SHUTDOWN.load(Ordering::SeqCst) {
            println!("Monitor thread shutdown. ");
            break;
        }
		dump_log(&mut his_log, &mut cnt_log);

        thread::sleep(Duration::from_secs(1));
    }
}

pub fn dump_log(history_l: &mut File, cnt_l: &mut File){
	// Write data to log file.
	MONITOR.get().unwrap().iter().enumerate().for_each(
		|(tid, tl)|{
			tl.counter.iter().for_each(|(k, v)|{
				if let Err(err) = history_l.write_fmt(format_args!(
					"{},{:?},{},{}", 
					tid,tl.role,k,unsafe{*v.get()}
				)) {
					eprintln!("Failed to write to count log file {}", err);
				}
			});
			let h =  unsafe { &*tl.history.get() };
				h.logs[h.written..].iter().for_each(|m|{
				if let Err(err) = cnt_l.write_fmt(format_args!(
					"{},{:?},{},{}", 
					tid,tl.role,m.ts,m.content
				)){
					eprintln!("Failed to write to history log file {}", err);
				}
			})
		}
	);
}