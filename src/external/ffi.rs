use std::mem;

use once_cell::sync::OnceCell;
use serde::Deserialize;
use crate::{config::CONFIG, ds::transactions::{Txn, TXN_TEMPLATES}};
 
#[derive(Deserialize)]
pub struct TxnMessage {
	pub type_idx:  u16,
	pub ts: u64,
	pub txn_req_id: u64
}

use libloading::{Library, library_filename, Symbol};

static LIB: OnceCell<Library> = OnceCell::new();

pub fn init(){
	unsafe {
		println!("Loading Dynamic Library from {}", CONFIG.read().unwrap().vnf_runtime_path);
    	let _ = LIB.set(libloading::Library::new(CONFIG.read().unwrap().vnf_runtime_path.as_str()).unwrap());
	}
}

fn deposit_transaction(a: String){
	let msg: TxnMessage = serde_json::from_str(a.as_str()).unwrap();
	if !msg.type_idx as usize >= TXN_TEMPLATES.get().unwrap().len() {
		// Err(String::from("required index invalid."))
		panic!("required index invalid.");
	};
	match super::pipe::PIPE_IN.get().unwrap().send(msg){
		Ok(_) => (),
		Err(e) => println!("Error sending message to pipe: {}", e),
	}
}

pub(crate) fn init_sfc(argc: i32, argv: Vec<String>) {
	// Call the unsafe extern function and receive the resulting JSON string
	let json_string  = unsafe {
        let func: libloading::Symbol<unsafe extern fn(i32, Vec<String>) -> String> = LIB.get().unwrap().get(b"Init_SFC").unwrap();
    	func(argc, argv)
    };

    // Parse the JSON string into template.
	let mut txns: Vec<Txn> = Txn::from_string(&json_string)
		.expect("Error parsing transmitted txn template");

	let all_txn_templates = txns.iter_mut().map(|txn|{
        txn.process_txn();
		mem::take(txn)
    }).collect();

	let _ = TXN_TEMPLATES.set(all_txn_templates);

	// Parse all variables from the SFC.
	/*
		Here we compose transactions template from the mesasage from SFC init.
		1. Sort to accelerate the key finding.
		2. Add write key to reads to add constraint to "writing to the same key."
	 */
}

pub(crate) fn all_variables() -> Vec<&'static str> { // WARN: lifecycle.
	let mut ret: Vec<&'static str> = Vec::new();
	TXN_TEMPLATES.get().unwrap().iter().for_each(|txn|{
		let mut all_reads: Vec<&String> = txn.es.iter()
            .flat_map(|en| en.reads.iter())
            .collect();

        // Sort and deduplicate reads
        all_reads.sort();
        all_reads.dedup();
        
        ret.extend(all_reads.iter().map(|s| s.as_str()));
	});
	ret
}

pub(crate) fn vnf_thread(c: i32, v: Vec<String>) {
	unsafe {
        let func: libloading::Symbol<unsafe extern fn(i32, Vec<String>)> = LIB.get().unwrap().get(b"VNFThread").unwrap();
    	func(c, v);
    }
}

pub(crate) fn execute_event(txn_req_id: u64, sa_idx: i32, value: String, param_count: i32) -> (bool, String) {
	unsafe {
    	let func: libloading::Symbol<unsafe extern fn(u64, i32, Vec<u8>, i32) -> String> = LIB.get().unwrap().get(b"execute_sa_udf").unwrap();
		// TODO. Parse out res. Double return value.
		(true, func(txn_req_id, sa_idx, value.bytes().collect(), param_count))
    }
}

pub(crate) fn txn_finished_sign(txn_req_id: u64) -> i32 {
	unsafe {
    	let func: libloading::Symbol<unsafe extern fn(u64) -> i32> = LIB.get().unwrap().get(b"txn_finished").unwrap();
		func(txn_req_id)
    }
}

