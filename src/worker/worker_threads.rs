use crate::{database::{api::Database, simpledb::DB}, ds::events, external::ffi::execute_event, tpg::{ev_node::{EvNode, EventStatus}, tpg::{self, TPG}}};
use std::{fmt::write, str, sync::Arc};

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
					Err(_) => continue,
				}
		}

		let evn = evn_option.as_ref().unwrap();

		// Assertion checking the dependency has been satistifed.
		debug_assert!(evn.ready());

		// Fetch required states;
		let ts = evn.txn.upgrade().unwrap().ts;
		let values = evn.reads
			.iter().map(|e| (DB.get().unwrap()).get_version("default", e, ts)).collect();

		// Call the Cpp execution func.
		let (res, v) = evn.execute(&values, values.len() as i32);
		/*
			Re-check the status here again. Abortion could have happended between last check and now.
			Swap out, check it, and put it back.
		 */
		 // TODO.
		if res && evn.status.swap(EventStatus::ABORTED) == EventStatus::WAITING {
			evn.accepted();
			evn.write_back(&v, DB.get().unwrap());
			evn_option = evn.get_next_option_push_others_ready(&TPG.get().unwrap().ready_queue_in);
			evn_option.as_ref().unwrap().status.store(EventStatus::CLAIMED);
			continue;
		} else {
			evn.notify_txn_abort();
		}
	}
}