use std::hash::Hash;
use std::os::linux::raw::stat;
use std::sync::{mpsc::*, Arc, Mutex, RwLock, Weak};
use std::sync::mpsc::Receiver;
use std::collections::HashMap;

use once_cell::sync::OnceCell;

use crate::config::CONFIG;

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
    pub fn new(keys: Vec<&str>) -> Self {
		let mut state_map = HashMap::<String, Option::<(Weak<EvNode>,Arc<TxnNode>)>>::new();
		keys.iter().for_each(
			|&k| (0..CONFIG.read().unwrap().max_state_records).into_iter().for_each(
				|j| {state_map.insert(format!("{}_{}", k, j), None);}
			)
		);

		let (tx, rx) = channel();
		Tpg{
			ready_queue_in: tx,
			ready_queue_out: Mutex::new(rx),
			state_last_modify: RwLock::new(state_map),
		}
	}
}
