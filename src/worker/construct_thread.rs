use crate::database::api::Database;
use crate::external::ffi;
use crate::tpg::{
	txn_node::*,
	ev_node::*,
};

use std::sync::mpsc::TryRecvError;

use crate::config::CONFIG;

use super::context;

// This worker thread constructs TPG streamingly.
// TODO. Slab memory allocation to reduce the allocation time.
pub fn construct_thread(ctx: context::Context){
	// TODO. Add support for multi-table later.
	let table_name = "default";
	let timeout_margin = CONFIG.read().unwrap().transaction_out_of_order_time_ns;
	let mut timeout_waiting_queue = Vec::<TxnNode>::new();

	// TODO. Graceful shutdown.
	loop { // Outer loop. For each valid transaction.
		let mut tn;
		// Message receiver.
		loop { // Inner loop. Take out txn from queue and order it. Take out latest transacation each time.
			let mut new_txn_msg: ffi::TxnMessage;
			match ctx.txn_msg_pipe.unwrap().try_recv() {
				Ok(new_txn) => {},
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
			tn = TxnNode::from_message(new_txn_msg);
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
		tn.set_links(&ctx.tpg.state_last_modify);

		// if ready, into ready_queue. Else will be visited by ancestors.
		tn.ev_nodes.iter().for_each(|ev_node| {
			if ev_node.ready() {
				ev_node.status.store(EventStatus::INQUEUE);
			    ctx.tpg.ready_queue_in.send(ev_node.clone()).unwrap();
			} else {
				ev_node.status.store(EventStatus::WAITING);
			}
		});
	}	
	// Graceful Shutdown.
}
