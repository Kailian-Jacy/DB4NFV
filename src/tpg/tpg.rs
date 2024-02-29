use std::sync::{mpsc::*, Arc, Mutex, RwLock, Weak};
use std::sync::mpsc::Receiver;
use std::collections::HashMap;

use once_cell::sync::OnceCell;

use super::{
	txn_node::TxnNode,
	ev_node::EvNode,
};

pub(crate) static TPG: OnceCell<Tpg> = OnceCell::new();

// Tpg itself. applies to both Txn and Events.
pub struct Tpg{
	pub ready_queue_out: Mutex<Receiver<Arc<EvNode>>>,
	pub ready_queue_in: Sender<Arc<EvNode>>,
	pub state_last_modify: RwLock<HashMap<String, Option::<(Weak<EvNode>,Arc<TxnNode>)>>>,
}

impl Tpg{
    pub fn new(v: Vec<&str>) -> Self {
		let (tx, rx) = channel();
		Tpg{
			ready_queue_in: tx,
			ready_queue_out: Mutex::new(rx),
			state_last_modify: RwLock::new(v.iter().map(|&s| {(String::from(s), None)}).collect()),
		}
	}
}
