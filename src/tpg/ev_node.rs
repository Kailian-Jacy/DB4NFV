use std::sync::{mpsc::*, Arc, Mutex, RwLock, Weak};
use std::sync::mpsc::Receiver;
use std::collections::HashMap;
use crossbeam::atomic::AtomicCell;

use crate::database::api::Database;
use crate::database::simpledb::{self, SimpleDB};
use crate::ds::events::Event;
use crate::external::ffi;
use crate::tpg::txn_node::TxnStatus;

use super::txn_node::TxnNode;

pub struct EvNode{
	// Topology
	// 
	/*
		Read_from:
		- Size fixed after building. Comes from the readed keys.
		- Could be None, when father txn useless and collected.
		- Corresponding to the is_read_from_fulfilled vector.
	 */
	pub read_from: Vec<Option<Weak<EvNode>>>,       
	/*
		read_by is the set of evNode who use the result of this EvNode. Comes from:
		1. Tpg construction. 
		2. Abortion. Grandson reads my result if son aborted.
		About the weak reference: Weak reference is guaranteed to be valid. It could only be result 
	 */
	pub read_by: RwLock<Vec<Option<Weak<EvNode>>>>, // Could be updated during running.

	// Meta
	pub txn: Weak<TxnNode>,
	pub status: AtomicCell<EventStatus>,
	
	// A vector is used to solve the multi-thread visiting.
	pub(in crate::tpg) is_read_from_fulfilled: Vec<AtomicCell<bool>>,

	// States to read.
	pub reads: Vec<String>,
	pub write: String,
	pub has_write: bool,

	// For debugging.
	has_storage_slot: bool,

	// Router to execute function.
	idx: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStatus{
	CONSTRUCT,
	INQUEUE,
	// Waiting marks the unfinished dependent events counts.
	WAITING,
	CLAIMED,
	ACCEPTED,
	ABORTED,
	// No Garbage. It would be collected along with the whole transaction.
}

// impl Clone for EvNode {
// 	fn clone(&self) -> Self {
// 		Self {
// 			read_from: self.read_from.clone(),
// 			read_by: self.read_by.clone(),
// 			txn: self.txn.clone(),
// 			status: self.status,
// 			unfinished_read_from: AtomicCell::new(self.unfinished_read_from.load()),
// 			reads: self.reads.clone(),
// 			write: self.write.clone(),
// 			idx: self.idx,
// 		}
// 	}
// }

impl EvNode {
	// Only used to create node from template.
	pub(in crate::tpg) fn from_template(event: &Event, idx: i32, txn: Weak<TxnNode>) -> Self {
		// Template
		let reads_length = event.reads.len();
        
        // Pre-allocate read_from and is_read_from_fulfilled vectors
        let read_from = vec![None; reads_length];
        let is_read_from_fulfilled = std::iter::repeat_with(|| AtomicCell::new(false))
            .take(reads_length)
            .collect();

        EvNode {
            read_from,
            read_by: RwLock::new(Vec::new()),
            txn,
            status: AtomicCell::new(EventStatus::CONSTRUCT),
            is_read_from_fulfilled,
            reads: event.reads.clone(),
            write: event.write.clone(),

            idx,
            has_write: event.has_write,
            has_storage_slot: false,
        }
	}

	pub fn ready(&self) -> bool{
		// Thread safe. Reusable.
		self.status.load() == EventStatus::WAITING 
			&& self.is_read_from_fulfilled.iter().all(|i| i.load() == true)
	}

	// Wrapper. Calling execution handler.
	pub fn execute(&self, values: Vec<String>, cnt: i32) -> (bool, String) {
		let value = values
			.join(";");
		ffi::execute_event(
			self.txn.upgrade().unwrap().txn_req_id as i64, 
			self.idx, 
			value, 
			cnt
		) // For now, let param_count as the same as query.
	}

	pub fn accepted(&self) {
		/*
			This function is just single threaded:
			- Only one thread calling for one EvNode each time.
		 */
		debug_assert!(self.status.load() == EventStatus::WAITING);
		self.status.swap(EventStatus::ACCEPTED);

		// Inform event accepted.
		self.txn.upgrade().unwrap().event_accepted();
		// Trigger txn commit handler.
		self.txn.upgrade().unwrap().try_commit();

	}

	pub fn write_back<T: Database>(&self, value: &String, db: Arc<T>) {
		if self.has_storage_slot {
			db.write_version(
				"default", 
				self.write.as_str(), 
				self.txn.upgrade().unwrap().ts, 
				value,
			);
		} else {
			db.push_version(
				"default", 
				self.write.as_str(), 
				self.txn.upgrade().unwrap().ts, 
				value,
			);
		}
	}

	// Add a new evNode reading this node's result.
	pub fn add_read_by(&self, son: &Arc<EvNode>){
		// Add to self read by.
		self.read_by.write().unwrap().push(Some(Arc::downgrade(son)));
		// Add to txn read by.
		if !self.txn.upgrade().unwrap().read_by
			.read().unwrap()
			.iter().any(
				|to| to.is_some_and(
					|ref tn| Arc::ptr_eq(&tn.upgrade().unwrap(), &son.txn.upgrade().unwrap())
			)) // Check if already inside.
		{
			self.txn.upgrade().unwrap().read_by
				.write().unwrap()
				.push(Some(son.txn.clone()))
		}
	}

	pub fn get_next_option_push_others_ready(&self, pipe: &Sender<Arc<EvNode>>) -> Option<Arc<EvNode>> {
		// Traverse sons to inform acceptance, and return the next node.
		let next_candidates: Vec<_> = self.read_by.read().unwrap().iter().enumerate().filter(|(idx, &node)| 
			{
				node.unwrap().upgrade().unwrap().parent_accepted(*idx); // Inform parent ready.
				node.unwrap().upgrade().unwrap().ready()  // Test who is ready. Will ignore those are under construction.
			}
		).collect();
		// Compare TS and select the minimum as the next.
		let min_evnode = *next_candidates
			.iter()
			.min_by_key(|&& evnode| evnode.1.unwrap()
				.upgrade().unwrap().txn
				.upgrade().unwrap().ts
			).unwrap().1;
		// Assign the next. Push others to queue.
		if min_evnode.is_some() {
			for evnode in &next_candidates {
				if !std::ptr::eq(evnode.1.as_ref().unwrap(), min_evnode.as_ref().unwrap()) {
					pipe.send((*evnode.1).unwrap().upgrade().unwrap()).unwrap();
				}
			}
			Some(min_evnode.unwrap().upgrade().unwrap())
		} else {
			None
		}
	}

	/*
		Abort the operation and then notify the whole transaction.
		This function called when:
		- CLAIMED evnode and WAITING transaction.
		When abort we:
		1. Mark 
	 */
	pub fn notify_txn_abort(&self){
		debug_assert!(self.status.load() == EventStatus::CLAIMED && self.txn.upgrade().unwrap().status.load() == TxnStatus::WAITING);
		// Trigger txn abortion.
		self.txn.upgrade().unwrap().abort();
		self.abort();
	}

	// Called by transaction or the evnode itself. The actual behavior for abortion.
	/*
		This functin needs to be:
		- Thread safe and reusable. Multiple abortion could happen at the same time.
		This function could be called:
		- CLAIMED evNode and WAITING txn.
		- ABORTED evNode and ABORTED txn.
		- ACCEPTED evNode and WAITING/Aborted txn.
		This function should do:
		1. Add the dependency for its read_by evNodes by 1. (Reenterable, atomic so thread safe)
		1.1 Recursively trigger the reset of these evNodes. Use their read_by for routing. (Reenterable, thread safe) 
		2. Add read_from dependency to the last state editor. (Omit for now. We construct write-write as strong constraint for now.)
		2.1 Cover the staged state result with the nearest copy of state. (Needs to be Reenterable. TODO.)
		3. Decrease the denepdency for its read_by nodes by 1, possibly trigger the redo of these objects. (Reenterable, atomic so thread safe)
		For all those operations, we use atomic EvNode.status change as the border to execute redo or stop writing.
		- If an operation has been marked CLAIMED, we believe its state change has not happened yet (TODO). Abortion thread set it to be WAITING to stop writing but no other intervention.
		- If an operation has been marked ACCEPTED, we believe its state change has happened. So we do recover works.
		- If an operation has been marked ABORTED, we believe its state has been properly handled. We do nothing about it.
	 */
	pub	fn abort(&self) {
		debug_assert!(
			self.status.load() == EventStatus::CLAIMED
				|| self.status.load() == EventStatus::ABORTED
				|| self.status.load() == EventStatus::ACCEPTED
		);
		debug_assert!(
			self.txn.upgrade().unwrap().status.load() == TxnStatus::ABORTED 
				|| self.txn.upgrade().unwrap().status.load() == TxnStatus::WAITING
		);
		self.status.store(EventStatus::ABORTED);
		// TODO. Preallocate the vector.
		let mut stack = vec![Arc::new(*self)];
		while stack.len() != 0 {
			let parent = stack.pop().unwrap();
			for (idx, son) in parent.read_by.read().unwrap().iter().enumerate(){
				let node = son.unwrap().upgrade().unwrap(); // Son could not be none.
				match node.status.load() {
					EventStatus::ACCEPTED => {
						// State shift has been made. Recover the state shift and dive in.
						node.status.store(EventStatus::WAITING);
						debug_assert!(node.is_read_from_fulfilled[idx].swap(false)); // Orginally must be true. Set false now.
						unsafe{
							simpledb::DB.unwrap()
								.reset_version("default", &self.write, self.txn.upgrade().unwrap().ts);
						}
						stack.push(node); // It could produce wrong result to be used by sons.
					}
					EventStatus::CLAIMED => {
						// No state shift happened. Withdraw readiness.
						node.status.store(EventStatus::WAITING);
						debug_assert!(node.is_read_from_fulfilled[idx].swap(false)); // Orginally must be true. Set false now.
					},
					EventStatus::WAITING => {
						// No state shift happened. No change.
						debug_assert!(node.is_read_from_fulfilled[idx].load() == false); // Not executed yet.
						continue;
					},
					EventStatus::ABORTED => continue, // Ends here. Has been operated by other abortion thread.
					EventStatus::INQUEUE | EventStatus::CONSTRUCT => panic!("bug."),
				}
			}
		};
		if self.has_write{
			unsafe {
				simpledb::DB.unwrap()
					.copy_last_version("default", &self.write, self.txn.upgrade().unwrap().ts); // Copy last state result only happens for aborted nodes. For those redo ones, just set empty.
			}
		}
	}

	// Notification from parents in read_from.
	fn parent_accepted(&self, idx: usize) {
		debug_assert!(self.status.load() == EventStatus::WAITING);
	    self.is_read_from_fulfilled[idx].swap(true);
	}

}