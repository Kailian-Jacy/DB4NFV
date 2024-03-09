use crate::{config::CONFIG, database::{api::Database, simpledb::DB}, tpg::{ev_node::{EvNode, EventStatus}, tpg::TPG}};
use std::sync::Arc;

// These worker threads traverse through TPG and execute the operations.
// TODO. db shall be used as shared.
pub fn execute_thread(tid: usize){
	let mut evn_option : Option<Arc<EvNode>> = None; // Option: if we have migrated from related one, we don't need to fetch from queue.
	loop {
		if evn_option.is_none() {
			// Loop to get from the ready queue. Listen from graceful shutdown.
			let ev_gd = TPG.get().unwrap().ready_queue_out.try_lock();
			if ev_gd.is_err() {
				continue
			}
			match ev_gd.unwrap().try_recv() {
					Ok(ev) => evn_option = Some(ev),
					Err(err) => {
						match err {
							std::sync::mpsc::TryRecvError::Empty => continue,
							std::sync::mpsc::TryRecvError::Disconnected => {
								println!("Channel closed. Worker {} exit.", tid);
							},
						}
					},
				}
		}

		if CONFIG.read().unwrap().debug_mode {
			println!("[DEBUG] evn claimed on thread {}", tid);
		}

		let evn = evn_option.as_ref().unwrap();
		match evn.status.load() {
			EventStatus::INQUEUE => {
				debug_assert!(evn.ready());
				evn.status.store(EventStatus::CLAIMED); // Claimed by this worker thread.
			},
			EventStatus::CONSTRUCT | EventStatus::WAITING => panic!("bug."),
			EventStatus::CLAIMED | EventStatus::ACCEPTED | EventStatus::ABORTED => {
				println!("Claimed event into queue.");
				continue;
			},
		}

		// Assertion checking the dependency has been satistifed.

		// Fetch required states;
		debug_assert!(
			evn.read_from.len() 
			== evn.reads.len()
		);
		let values = evn.reads
			.iter().enumerate().map(
				|(idx, r)| {
					if evn.read_from[idx].read().is_none() {
						// TODO: Router to default value.
						let zero_byte = '\0' as u8;
						vec![zero_byte]
					} else {
						let ts = (*evn.read_from[idx].read()).as_ref().unwrap().upgrade().unwrap()
							.txn.upgrade().unwrap()
							.ts;
						(DB.get().unwrap())
						.get_version("default", r, ts)
					}
				}
			).collect();

		// Call the Cpp execution func.
		let (abortion, v) = evn.execute(&values, values.len() as i32);
		/*
			Re-check the status here again. Abortion could have happended between last check and now.
			Swap out, check it, and put it back.
		 */
		 // TODO.
		if (!abortion) && evn.status.load() == EventStatus::CLAIMED {
			evn.accepted();
			evn.write_back(&v, DB.get().unwrap());
			evn_option = evn.get_next_option_push_others_ready(&TPG.get().unwrap().ready_queue_in);
			if evn_option.is_some() {
				evn_option.as_ref().unwrap().status.store(EventStatus::CLAIMED);
			}
			continue;
		} else {
			if !abortion {
				// Swapped non-waiting state.
				println!("Rare condition: operation switch to abortion when execution.")
			}
			evn.notify_txn_abort();
		}
	}
}