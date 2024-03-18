use crate::{config::CONFIG, database::{api::Database, simpledb::DB}, monitor::monitor, tpg::{ev_node::{EvNode, EventStatus}, tpg::TPG}, utils, worker::construct_thread::GRACEFUL_SHUTDOWN};
use std::{mem, sync::Arc};

// These worker threads traverse through TPG and execute the operations.
/*
	Worker Threads fetches events from multiple places to decrease race condition:
	- Events that had just ready by last accepted events.
	- Events from Local queue.
	- Events from Global queue.
 */
pub fn execute_thread(tid: usize){
	utils::report_cpu_core(format!("Executor {}", tid).as_str());

	let lq_cap = CONFIG.read().unwrap().max_event_batch;
	let mut local_queue = Vec::<Option<Arc<EvNode>>>::with_capacity(
		lq_cap	
	);
	(0..lq_cap).into_iter().for_each(|_| local_queue.push(None));
	let mut local_queue_size = 0;

	let mut exit = false;

	let mut evn_option_next : Option<Arc<EvNode>> = None; 
	let mut evn_option : Option<Arc<EvNode>>; // Option: if we have migrated from related one, we don't need to fetch from queue.
	// Event disposal loop.
	loop {
		// Use just ready first.
		evn_option = evn_option_next;
		evn_option_next = None;

		if evn_option.is_none() {
			// Use local queue first.
			if local_queue_size != 0 {
				evn_option = mem::take(&mut local_queue[local_queue_size - 1]);
				debug_assert!(evn_option.is_some());
				local_queue_size -= 1;
			} else {
				// Use global queue then.
				let ev_gd = TPG.get().unwrap().ready_queue_out.try_lock();
				if ev_gd.is_err() {
					continue
				}
				loop {
					// Local queue full. Continue to execute.
					if local_queue_size == lq_cap { 
						if CONFIG.read().unwrap().monitor_enabled {
							monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
								ts: utils::current_time_ns(),
								content: format!("{},thread local queue size,{},{}", tid, utils::current_time_ns(), local_queue_size),
							});
						}
						break 
					}
					match ev_gd.as_ref().unwrap().try_recv() {
						Ok(ev) => {
							if CONFIG.read().unwrap().monitor_enabled {
								monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
									ts: utils::current_time_ns(),
									content: format!("{},ev_claimed,{},{}", ev.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), ev.idx),
								});
							}
							local_queue[local_queue_size] = Some(ev);
							local_queue_size += 1;
							if CONFIG.read().unwrap().verbose {
								println!("[DEBUG] Evnode claimed by thread {}, thread queue size: {}", tid, local_queue_size);
							}
						},
						Err(err) => {
							match err {
								// Loop to get from the ready queue. Listen from graceful shutdown.
								std::sync::mpsc::TryRecvError::Empty => {
									if unsafe { GRACEFUL_SHUTDOWN } == true{
										exit = true;
										println!("Executor thread {} shutdown. ", tid);
									}
									// Global queue empty. Dispose those in local queue or continue waiting.
									break
								},
								_ => {
									println!("Channel paniced. Worker {} exit.", tid);
								},
							}
						},
					}
				}
			}
			if exit { break } // Disposal ends.
			if evn_option.is_none() { continue } // Global queue also empty. Go back to query.
		}

		let evn = evn_option.as_ref().unwrap();

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
				println!("Worker Queue: Rare condition. claimed event being disposed again.");
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
						debug_assert!({
							let r = evn.read_from[idx].read()
								.as_ref().unwrap().upgrade().unwrap()
								.status.load();
							if r != EventStatus::ACCEPTED 
								&& r != EventStatus::ABORTED // Aborted cell also holds copied valid result.
							{
								panic!("assertion: {:?}", r );
							}
							true
						});
						let ts = (*evn.read_from[idx].read()).as_ref().unwrap().upgrade().unwrap()
							.txn.upgrade().unwrap()
							.ts;
						DB.get().unwrap()
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
			match evn.status.compare_exchange(EventStatus::CLAIMED, EventStatus::ACCEPTED){
				Ok(_) => {} // No abortion happens. Just go on execution.
				Err(original) => {
					debug_assert!(
						original == EventStatus::ABORTED // Has just been aborted during execution.
						|| original == EventStatus::WAITING // Parent has been aborted during execution.
					); 
					continue; // Find another to go on.
				}
			}
			evn.notify_txn_accept();
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
			if CONFIG.read().unwrap().monitor_enabled {
				monitor::MONITOR.get().unwrap()[tid].log(monitor::Metrics{
					ts: utils::current_time_ns(),
					content: format!("{},abort,{},{}", evn.txn.upgrade().unwrap().txn_req_id, utils::current_time_ns(), evn.idx),
				});
				monitor::MONITOR.get().unwrap()[tid].inc("evnode.abort");
			}
			evn.notify_txn_abort(); // All these nodes are aborted and reverted back.
			(*evn.txn.upgrade().unwrap().ev_nodes.read())
				.iter().for_each(|aborted_evn|{
					debug_assert!(aborted_evn.status.load() == EventStatus::ABORTED);
					let n = aborted_evn.get_next_option_push_others_ready(&TPG.get().unwrap().ready_queue_in);
					if n.is_some(){
						local_queue.push(n);
					}
				})
		}
	}
}