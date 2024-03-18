use crate::{config::CONFIG, database::{api::Database, simpledb::DB}, monitor::monitor, tpg::{ev_node::{EvNode, EventStatus}, tpg::TPG}, utils, worker::construct_thread::GRACEFUL_SHUTDOWN};
use std::sync::{atomic::Ordering, Arc};

// These worker threads traverse through TPG and execute the operations.
// TODO. db shall be used as shared.
pub fn execute_thread(tid: usize){
	utils::report_cpu_core(format!("Executor {}", tid).as_str());

	let mut evn_option_next : Option<Arc<EvNode>> = None; 
	let mut evn_option : Option<Arc<EvNode>>; // Option: if we have migrated from related one, we don't need to fetch from queue.
	loop {
		evn_option = evn_option_next;
		evn_option_next = None;

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
						std::sync::mpsc::TryRecvError::Empty => {
							if GRACEFUL_SHUTDOWN.load(Ordering::SeqCst) == true{
								println!("Executor thread {} shutdown. ", tid);
								break
							}
						},
						std::sync::mpsc::TryRecvError::Disconnected => {
							println!("Channel closed. Worker {} exit.", tid);
						},
					}
				},
			}
			if evn_option.is_none() {
				continue
			}
		}

		if CONFIG.read().unwrap().verbose {
			println!("[DEBUG] evn claimed on thread {}", tid);
		}

		let evn = evn_option.as_ref().unwrap();

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},ev_claimed,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
			});
		}

		match evn.status.load() {
			EventStatus::INQUEUE => {
				debug_assert!(evn.ready());
				evn.status.store(EventStatus::CLAIMED); // Claimed by this worker thread.
			},
			EventStatus::CONSTRUCT => panic!("bug."),
			// When in queue, it's resetted and set WAITING.
			EventStatus::WAITING => {
				println!("Worker Queue: Rare condition. Reset event in queue.");
				debug_assert!({
					evn.no_waiting() == false
				});
				continue;
			},
			EventStatus::CLAIMED | EventStatus::ACCEPTED | EventStatus::ABORTED => {
				println!("Worker Queue: Rare condition. claimed event into queue.");
				continue;
			},
		}
		// Fetch required states;
		debug_assert!(
			evn.read_from.len() 
			== evn.reads.len()
		);

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},fetching_value,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
			});
		}

		let values = evn.reads
			.iter().enumerate().map(
				|(idx, r)| {
					if (*evn.read_from[idx].read())
						.is_none() {
						// TODO: Router to default value.
						let zero_byte = '\0' as u8;
						vec![zero_byte]
					} else {
						debug_assert!((*evn.read_from[idx].read())
							.as_ref().unwrap().upgrade().unwrap()
							.status.load() == EventStatus::ACCEPTED
						);
						let ts = (*evn.read_from[idx].read()).as_ref().unwrap().upgrade().unwrap()
							.txn.upgrade().unwrap()
							.ts;
						(DB.get().unwrap())
							.get_version("default", r, ts)
					}
				}
			).collect();

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},fetch_value_done,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
			});
		}

		// Call the Cpp execution func.
		let (abortion, v) = evn.execute(&values, values.len() as i32);

		if CONFIG.read().unwrap().monitor_enabled {
			monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
				ts: utils::current_time_ns(),
				content: format!("{},execution_done,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
			});
		}

		/*
			Re-check the status here again. Abortion could have happended between last check and now.
			Swap out, check it, and put it back.
		 */
		if !abortion {
			debug_assert!(evn.status.load() == EventStatus::CLAIMED ); // Not possible to reset during working.
			evn.accept();
			if CONFIG.read().unwrap().monitor_enabled {
				monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
					ts: utils::current_time_ns(),
					content: format!("{},accept,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
				});
				monitor::MONITOR.get().unwrap()[tid].inc("evnode.accept");
			}
			evn.write_back(&v, DB.get().unwrap());
			evn_option_next = evn.get_next_option_push_others_ready(&TPG.get().unwrap().ready_queue_in);
			continue;
		} else {
			if !abortion {
				// Swapped non-waiting state.
				println!("Rare condition: operation switch to abortion when execution.");
			}
			if CONFIG.read().unwrap().monitor_enabled {
				monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
					ts: utils::current_time_ns(),
					content: format!("{},abort,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
				});
				monitor::MONITOR.get().unwrap()[tid].inc("evnode.abort");
			}
			evn.notify_txn_abort();
		}
	}
}