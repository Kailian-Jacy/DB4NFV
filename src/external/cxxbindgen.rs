#[cxx::bridge]
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