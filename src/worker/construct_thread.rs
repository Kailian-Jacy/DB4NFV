use crate::external::{ffi, pipe};
use crate::tpg::tpg::TPG;
use crate::tpg::{
	txn_node::*,
	ev_node::*,
};

use std::sync::mpsc::TryRecvError;

use crate::config::CONFIG;

// This worker thread constructs TPG streamingly.
// TODO. Slab memory allocation to reduce the allocation time.
pub fn construct_thread(_: i16){
	// TODO. Add support for multi-table later.
	let table_name = "default";

	// TODO. Sort.
	let timeout_margin = CONFIG.read().unwrap().transaction_out_of_order_time_ns;
	let mut timeout_waiting_queue = Vec::<TxnNode>::new();

	let txn_msg_queue = pipe::init();

	// TODO. Graceful shutdown.
	loop { // Outer loop. For each valid transaction.
		let tn;
		// Message receiver.
		loop { // Inner loop. Take out txn from queue and order it. Take out latest transacation each time.
			let new_txn_msg: ffi::TxnMessage;
			match txn_msg_queue.try_recv() {
				Ok(res) => {new_txn_msg = res},
				Err(err) => match err {
					TryRecvError::Empty => {
						// Continue if the receiver is empty
						continue;
					}
					_ => {
						// Panic for any other error
						panic!("Error receiving message: {:?}", err);
					}
				},
			};
			if CONFIG.read().unwrap().debug_mode {
				let op_tn = TxnNode::from_message(new_txn_msg.clone());
				if op_tn.is_none(){
					ffi::txn_finished_sign(new_txn_msg.txn_req_id); // TODO. Inform illegal.
						println!("[DEBUG] invalid txn msg: {:?}", new_txn_msg);
					continue
				} else {
					tn = op_tn.unwrap();
				}
			} else {
				let req = new_txn_msg.txn_req_id;
				let op_tn = TxnNode::from_message(new_txn_msg);
				if op_tn.is_none(){
					ffi::txn_finished_sign(req);
					continue
				} else {
					tn = op_tn.unwrap();
				}
			}
			// Wait until timeout and mark as ready.
			// if (utils::current_time_ns() - tn.body.ts) < timeout_margin {
				// TODO. reverse out.
				// let insert_index = timeout_waiting_queue.iter().position(|node| node.body.ts > new_txn.timestamp);
				// // Assert that there is no DataPoint with the same timestamp
				// assert!(insert_index.is_none(), "A txn with the same timestamp already exists.");
				// // Create a new DataPoint with the provided timestamp and value.
				// match insert_index {
				// 	Some(index) => self.states_records_map.get_mut(key).unwrap().insert(index, new_data_point),
				// 	None => self.states_records_map.get_mut(key).unwrap().push(new_data_point),
				// }
			// }
			// 1. Take out txn from the template. Use memory loop. TODO. Produce tn from txnMessage fetch from allocated buffer.

			break;
		}
		// Set link between events nodes to later ones.
		// Set link between this txn and its parents.
		tn.set_links(&TPG.get().unwrap().state_last_modify);

		// if ready, into ready_queue. Else will be visited by ancestors.
		tn.ev_nodes.read().iter().for_each(|ev_node| {
			if ev_node.no_waiting() {
				ev_node.status.store(EventStatus::INQUEUE);
			    TPG.get().unwrap().ready_queue_in.send(ev_node.clone()).unwrap();
			} else {
				ev_node.status.store(EventStatus::WAITING);
			}
		});
	}	
	// Graceful Shutdown.
}
