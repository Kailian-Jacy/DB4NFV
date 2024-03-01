// mod tpg_builder;
use std::path::PathBuf;
use std::env;
use std::thread;

mod ds;
mod config;
mod worker;
mod utils;
mod external;
mod database;
mod tpg;

use database::{
    api::Database, 
    simpledb::{self, SimpleDB}
};
use external::ffi::{self, all_variables};
use tpg::tpg::{Tpg, TPG};
use worker::{
    worker_threads::execute_thread,
    construct_thread::construct_thread,
};

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

    ffi::init();
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
    let vnf_guards = (1..=config::CONFIG.read().unwrap().vnf_threads_num).map(|tid| 
        thread::spawn(move || {
            utils::bind_to_cpu_core(tid as usize);
            ffi::vnf_thread(0, Vec::new());
        }));

    /*
        Spawn TSPE worker threads and bind to core.
     */
    let worker_thread_ends = config::CONFIG.read().unwrap().worker_threads_num
        + config::CONFIG.read().unwrap().vnf_threads_num + 1;
    let guards = (config::CONFIG.read().unwrap().vnf_threads_num + 1..worker_thread_ends).map(|tid| {
        thread::spawn(move || {
            utils::bind_to_cpu_core(tid as usize);
            execute_thread(tid as usize)
        })
    });

    // Main thread work as construct thread.
    construct_thread(-1);

    for guard in vnf_guards {
        guard.join().unwrap();
    };
    for guard in guards {
        guard.join().unwrap();
    };

    // TODO: Graceful shutdown.
}