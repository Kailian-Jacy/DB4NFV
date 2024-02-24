#[cxx::bridge]
mod ffi {
/*
	This file serves as the api list exposed by DB4NFV in the form of FFI to C/C++.
*/

extern "Rust"{
	/* DepositTransaction receives transaction parameters from Cpp.
		This function is usually non-blocking when running; It just push the requested txns and return;
		When the queue is full, namely waiting transactions reaching the bound of config::CONFIG.waiting_queue_size,
			It will block until any slots becomes available to coordinate the traffic.
	*/ 
	fn deposit_transaction(a: String); 
}

/* 
	Interface that needs to be implemented by C++ VNF runtime.
	TODO.
 */

// #[namespace("your_namespace")] // Replace "your_namespace" with the actual namespace in your C++ code
unsafe extern "C++" {
    include!("DB4NFV/include/ffi.h"); // Include the path to your C++ header file
	pub fn Init_SFC(argc: i32, argv: Vec<String>) -> String;
	pub fn VNFThread(c: i32, v: Vec<String>);
	pub fn execute_sa_udf(txnReqId_jni: i64, saIdx: i32, value: Vec<u8>, param_count: i32) -> String;
	pub fn txn_finished(txnReqId_jni: i64) -> i32;
}

// #[derive(Deserialize)]
// struct SFCTemplate{
// 	txns: Vec<TxnTemplate>,
// }

// impl SFCTemplate {
//     fn to_events(&self) -> Vec<ev::Event> {
// 		self.txns.iter().flat_map(|txn| txn.events).collect()
//     }
// }

// #[derive(Deserialize)]
// struct TxnTemplate{
// 	events: Vec<ev::Event>,
// }

}

use crossbeam::atomic::AtomicCell;
use serde::Deserialize;
use crate::{
	ds::{
		self, transactions::{Txn, TXN_TEMPLATES},
	}, 
	tpg::txn_node::{TxnNode, TxnStatus},
};
use std::{collections::HashMap, error::Error, mem, sync::{Arc, RwLock}};
 
#[derive(Deserialize)]
pub struct TxnMessage {
	pub type_idx:  u16,
	pub ts: u64,
	pub txn_req_id: u64
}

fn deposit_transaction(a: String){
	let msg: TxnMessage = serde_json::from_str(a.as_str()).unwrap();
	if !msg.type_idx as usize >= TXN_TEMPLATES.len() {
		// Err(String::from("required index invalid."))
		panic!("required index invalid.");
	};
	match super::pipe::PIPE_IN.get_mut().unwrap().send(msg){
		Ok(_) => (),
		Err(e) => println!("Error sending message to pipe: {}", e),
	}
}

pub(crate) fn init_sfc(argc: i32, argv: Vec<String>) {
	// Call the unsafe extern function and receive the resulting JSON string
    let json_string = unsafe { ffi::Init_SFC(argc, argv) };

    // Parse the JSON string into template.
	let mut txns: Vec<Txn> = Txn::from_string(&json_string)
		.expect("Error parsing transmitted txn template");

	txns.iter_mut().map(|mut txn|{
        txn.process_txn();
		TXN_TEMPLATES.push(mem::take(txn));
    });

	// Parse all variables from the SFC.
	/*
		Here we compose transactions template from the mesasage from SFC init.
		1. Sort to accelerate the key finding.
		2. Add write key to reads to add constraint to "writing to the same key."
	 */
}

pub(crate) fn all_variables() -> Vec<&'static str> { // WARN: lifecycle.
	let mut ret: Vec<_>;
	TXN_TEMPLATES.iter().for_each(|txn|{
		let mut all_reads: Vec<String> = txn.es.iter()
            .flat_map(|en| en.reads.iter().cloned())
            .collect();

        // Sort and deduplicate reads
        all_reads.sort();
        all_reads.dedup();
        
        ret.extend(all_reads.iter().map(|s| s.as_str()));
	});
	ret
}

pub(crate) fn vnf_thread(c: i32, v: Vec<String>) {
    unsafe { ffi::VNFThread(c, v) };
}

pub(crate) fn execute_event(txn_req_id: i64, sa_idx: i32, value: String, param_count: i32) -> (bool, String) {
	// TODO. Parse out res. Double return value.
	unsafe { (true, ffi::execute_sa_udf(txn_req_id, sa_idx, value.bytes().collect(), param_count)) }
}

pub(crate) fn txn_finished_sign(txn_req_id: i64) -> i32 {
	unsafe { ffi::txn_finished(txn_req_id) }
}

