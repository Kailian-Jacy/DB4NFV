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
	pub fn execute_sa_udf(txnReqId_jni: u64, saIdx: i32, value: Vec<u8>, param_count: i32) -> String;
	pub fn txn_finished(txnReqId_jni: u64) -> i32;
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

use std::mem;

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use crate::{config::CONFIG, ds::transactions::{Txn, TXN_TEMPLATES}};
use crate::ds::events as ev;

// {
// 	"app": [{
// 		"name": "SLApp",
// 		"transactions": [{
// 			"StateAccesses": [{
// 				"consistency_requirement": "",
// 				"has_write": true,
// 				"reads": ["balance"],
// 				"stateName": "deposit_sa",
// 				"write": "balance"
// 			}]
// 		}, {
// 			"StateAccesses": [{
// 				"consistency_requirement": "",
// 				"has_write": true,
// 				"reads": ["balance"],
// 				"stateName": "src_transfer_sa",
// 				"write": "balance"
// 			}, {
// 				"consistency_requirement": "",
// 				"has_write": true,
// 				"reads": ["balance"],
// 				"stateName": "dst_transfer_sa",
// 				"write": "balance"
// 			}]
// 		}]
// 	}]
// }

// Define the intermediate struct for parsing
#[derive(Debug, Deserialize, Serialize)]
struct AppData {
    #[serde(rename = "app")]
    app: Vec<AppInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AppInfo {
    name: String,
    #[serde(rename = "transactions")]
    transactions: Vec<TransactionData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TransactionData {
    #[serde(rename = "StateAccesses")]
    state_accesses: Vec<ev::Event>,
}


#[derive(Deserialize)]
pub struct TxnMessage {
	pub type_idx:  u16,
	pub ts: u64,
	pub txn_req_id: u64,
	// When transaction created, they pointes the column to read; Here in runtime, they point out which row to read.
	// indexes are displayed in vector for each event.
	pub reads_idx: vec<vec<usize>>,
	pub write_idx: vec<usize> 
}

pub fn deposit_transaction(a: String){
	let msg: TxnMessage = serde_json::from_str(a.as_str()).unwrap();
	if !msg.type_idx as usize >= TXN_TEMPLATES.get().unwrap().len() {
		// Err(String::from("required index invalid."))
		panic!("required index out of range.");
	};
	match super::pipe::PIPE_IN.get().unwrap().send(msg){
		Ok(_) => (),
		Err(e) => println!("Error sending message to pipe: {}", e),
	}
}


pub(crate) fn init_sfc(argc: i32, argv: Vec<String>) {
	// Call the unsafe extern function and receive the resulting JSON string
	let json_string = 
		ffi::Init_SFC(argc, argv);

	if CONFIG.read().unwrap().debug_mode {
		println!("{}", json_string);
	}

	let apps = serde_json::from_str::<AppData>(&json_string)
		.unwrap();

	// Convert the intermediate struct into the final Txn struct
    let mut txns: Vec<Txn> = Vec::new();
    for app_info in apps.app {
        for transaction_data in app_info.transactions {
            let txn = Txn {
                es: transaction_data.state_accesses,
                all_reads_length: 0,
            };
            txns.push(txn);
        }
    }

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
	ffi::VNFThread(c, v)
}

pub(crate) fn execute_event(txn_req_id: u64, sa_idx: i32, value: String, param_count: i32) -> (bool, String) {
	(true, ffi::execute_sa_udf(txn_req_id, sa_idx, value.into(), param_count))
}

// TODO. Return result. ILLEGAL, SUCCESS, ABORTED.
pub(crate) fn txn_finished_sign(txn_req_id: u64) -> i32 {
	ffi::txn_finished(txn_req_id)
}

