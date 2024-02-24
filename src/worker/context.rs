use std::sync::Arc;

use object_pool::Pool;

use crate::{
	config, database::{
		api::Database, 
		simpledb
	}, ds, external::ffi, tpg::tpg
};

pub struct Context {
	pub tid: i16, 
	pub tpg: Arc<tpg::Tpg>, 
	pub db: Arc<simpledb::SimpleDB>,
	pub txn_msg_pipe: Option<std::sync::mpsc::Receiver<ffi::TxnMessage>>,
}

impl Context {
	pub fn new(tid:i16 , tpg: Arc<tpg::Tpg>, db: Arc<simpledb::SimpleDB>, pipe: Option<std::sync::mpsc::Receiver<ffi::TxnMessage>> ) -> Self {
		Self {
			tid: 0, 
			tpg, 
			db,
			txn_msg_pipe: pipe,
			// txn_pool: Pool::new( 
			// 	config::CONFIG.read().unwrap().worker_threads_num as usize, 
			// 	|| ds::transactions::Txn::new
			// ),
		}
	}
}