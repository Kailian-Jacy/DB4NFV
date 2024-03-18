use serde::{Serialize, Deserialize};
use serde_json::Result;
use std::fs::File;
use std::fs;
use std::io::{self, BufReader};
use structopt::StructOpt;
use std::path::PathBuf;
use lazy_static::lazy_static;
use std::sync::RwLock;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    // Define VNF thread nums.
    pub vnf_threads_num: u16, // Deprecated. Defined in VNF config.
    // Define your configuration fields here
    pub worker_threads_num: u16,
    // Waiting queue uses a cyclic buffer. This defines the size.
    pub waiting_queue_size: u128,
    // Max transaction event out of order time.
    pub transaction_out_of_order_time_ns: u128,
    // Database ringbuffer size.
    pub ringbuffer_size: usize,
    // Database ringbuffer full to panic or continue execution.
    pub ringbuffer_full_to_panic: bool,
    // Transaction Pooling Size.
    pub transaction_pooling_size: usize,
    // Max state record size.
    pub max_state_records: usize,
    // Max event batch size. Used when worker thread fetch unfinished events.
    pub max_event_batch: usize,
    // Verbose output.
    pub verbose: bool,
    // If enable monitor thread.
    pub monitor_enabled: bool,
    // Monitor logging path. Default to be current path.
    pub log_dir: String,
}

lazy_static! {
    pub static ref CONFIG: RwLock<Config> = RwLock::new(Config::default());
}

impl Default for Config {
    fn default() -> Self {
        Config {
            vnf_threads_num: 3,
            worker_threads_num: 3,
            waiting_queue_size: 4096,
            transaction_out_of_order_time_ns: 100,
            ringbuffer_size: 10000,
            max_state_records: 10000,
            ringbuffer_full_to_panic: false,
            transaction_pooling_size: 10000,
            verbose: true,
            monitor_enabled: true,
            max_event_batch: 10,
            log_dir: String::from("./perf"),
        }
    }
}

#[derive(StructOpt)]
#[structopt(name = "config-reader", about = "Reads configuration from JSON file.")]
pub struct Cli {
    #[structopt(short, long, parse(from_os_str))]
	// TODO: May require fix.
    file: Option<PathBuf>,
}

fn dump_config_template(file_path: &str) -> io::Result<()> {
    let default_config = Config::default();
    let serialized_config = serde_json::to_string_pretty(&default_config)?;

    fs::write(file_path, serialized_config)?;

    println!("Config template has been created at '{}'", file_path);
    Ok(())
}

fn read_config_from_file(file_path: &str) -> Result<Config> {
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);

    // Deserialize the JSON into the Config struct
    let config: Config = serde_json::from_reader(reader)?;

    Ok(config)
}

pub fn init(file_path: PathBuf) {
    // Check if the config file exists, if not, create a template
    if !file_path.exists() {
        dump_config_template(file_path.to_str().unwrap()).unwrap();
    }

    // Read the configuration from the JSON file
	let mut glb = CONFIG.write().unwrap();
	*glb = read_config_from_file(file_path.to_str().unwrap())
        .expect("Config parsing failure.");
    drop(glb);

    if CONFIG.try_read().unwrap().verbose {
	    println!("== Config Inited.");
        println!("{:?}", CONFIG.read().unwrap());
    }
    fast_log::init(fast_log::Config::new().console().chan_len(Some(100000))).unwrap();

    let core_ids = core_affinity::get_core_ids().unwrap();
    if  CONFIG.read().unwrap().worker_threads_num + 
        CONFIG.read().unwrap().vnf_threads_num + // Vnf threads.
        1 + // Main or construction thread. 
        1 // Monitor thread
        > 
        core_ids.len() as u16
    {
        panic!("No sufficient cores for pointed thread nums. Total {:?}", core_ids.len());
    }
}