use std::sync::atomic::Ordering;
use std::path::PathBuf;
use std::env;
use std::thread;

mod ds;
mod config;
mod worker;
mod utils;
mod external;
mod database;
mod monitor;
mod tpg;

use database::{
    api::Database, 
    simpledb::{self, SimpleDB}
};
use monitor::monitor as metrics;
use external::ffi::{self, all_variables};
use tpg::tpg::{Tpg, TPG};
use worker::{
    worker_threads::execute_thread,
    construct_thread::construct_thread,
    construct_thread::GRACEFUL_SHUTDOWN,
};
use rayon::prelude::*;

fn main() {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();

    // Get the file path from command line arguments or use default
    let file_path = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("./config.json")
    };

    config::init(file_path);
    utils::bind_to_cpu_core(0);
    metrics::init();

    ffi::init_sfc(0, Vec::new());

    // Initiate Database.
    let mut db = SimpleDB::new();
    db.add_table("default", all_variables());
    let _ = simpledb::DB.set(db);  
    
    /*
        TODO: Initialize db tables as well as tpg hash maps.
    */
    let _ = TPG.set(Tpg::new(ffi::all_variables()));

    /*
        Spawn Vnf threads and bind to core.
     */
    let vnf_guard = 
        thread::spawn(move || {
            // VNF thread bind in runtime.
            ffi::vnf_thread(0, Vec::new());
    });

    /*
        Spawn TSPE worker threads and bind to core.
     */
    let worker_thread_ends = config::CONFIG.read().unwrap().worker_threads_num // Workers.
        + config::CONFIG.read().unwrap().vnf_threads_num // vnfs.
        + 1; // Construct thread.
    let guards: Vec<_> = (config::CONFIG.read().unwrap().vnf_threads_num + 1..worker_thread_ends)
        .into_par_iter().map(|tid| {
            thread::spawn(move || {
                utils::bind_to_cpu_core(tid as usize);
                execute_thread(tid as usize)
            })
    }).collect();

    /*
        Spawn monitor thread. if required.
     */
    let monitor_guards = thread::spawn(move || {
        // utils::bind_to_cpu_core();
        metrics::monitor_thread(worker_thread_ends as usize);
    });

    // Register a handler for graceful shutdown
    ctrlc::set_handler(move || {
        println!("Exiting. Please wait till all tasks finished.");
        GRACEFUL_SHUTDOWN.store(true, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    // Main thread work as construct thread.
    construct_thread(-1);

    for guard in guards {
        guard.join().unwrap();
    };
    vnf_guard.join().unwrap();
    monitor_guards.join().unwrap();
}