use crate::external::{ffi, pipe};
use crate::monitor::monitor;
use crate::tpg::tpg::TPG;
use crate::tpg::{
	txn_node::*,
	ev_node::*,
};
use crate::utils;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;

use crate::config::CONFIG;

pub static GRACEFUL_SHUTDOWN: AtomicBool = AtomicBool::new(false);

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
		if GRACEFUL_SHUTDOWN.load(Ordering::SeqCst) == true{
			println!("Construct thread shutdown. ");
			break
		}
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
			if CONFIG.read().unwrap().verbose {
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

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[0].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},dispatched_from_vnf,{}", tn.txn_req_id, tn.ts),
			});

			monitor::MONITOR.get().unwrap()[0].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},sorting_done,{}", tn.txn_req_id, utils::current_time_ns()),
			});
		}

		// Set link between events nodes to later ones.
		// Set link between this txn and its parents.
		tn.set_links(&TPG.get().unwrap().state_last_modify);

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[0].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},linked_to_tpg,{}", tn.txn_req_id, utils::current_time_ns()),
			});
		}

		// if ready, into ready_queue. Else will be visited by ancestors.
		tn.ev_nodes.read().iter().for_each(|ev_node| {
			ev_node.status.store(EventStatus::WAITING); // Possibly claimed during counting.

			ev_node.read_from.iter().enumerate().for_each(|(idx, last)|{
				if last.read().is_none() {
					ev_node.is_read_from_fulfilled[idx].store(true);
				} else {
					ev_node.is_read_from_fulfilled[idx].store(
						last.read().as_ref().unwrap().upgrade().unwrap().status.load() == EventStatus::ACCEPTED
					);
				}
			});

			if CONFIG.read().unwrap().monitor_enabled {
				monitor::MONITOR.get().unwrap()[0].inc("evnode.let_occupy");
				monitor::MONITOR.get().unwrap()[0].log(monitor::Metrics{
					ts: utils::current_time_ns(),
					content: format!("{},ready_to_be_fetched,{}", tn.txn_req_id, utils::current_time_ns()),
				});
			}

			// Has fulfilled according to detection.
			if ev_node.no_waiting() {
				// Try to fetch into queue.
				match ev_node.status.compare_exchange(EventStatus::WAITING, EventStatus::INQUEUE) {
					Ok(_) =>  {
						monitor::MONITOR.get().unwrap()[0].inc("evnode.enqueue");
						TPG.get().unwrap().ready_queue_in.send(ev_node.clone()).unwrap();
					},
					// Has been claimed by worker threads.
					Err(state) => {
						debug_assert!(state == EventStatus::CLAIMED);
						monitor::MONITOR.get().unwrap()[0].inc("rare_condition.evnode.claimed_when_counting.");
						ev_node.status.store(EventStatus::WAITING);
					}
				}
			}
		});
	}	
	// Graceful Shutdown.
}
