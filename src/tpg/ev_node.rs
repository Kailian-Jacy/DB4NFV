use std::sync::{mpsc::*, Arc, RwLock, Weak};
use crossbeam::atomic::AtomicCell;

use crate::database::api::Database;
use crate::database::simpledb::{self};
use crate::ds::events::Event;
use crate::external::ffi;
use crate::tpg::txn_node::TxnStatus;
use crate::utils::ShouldSyncCell;

use super::txn_node::TxnNode;

#[derive(Debug)]
pub struct EvNode{
	// Topology
	// 
	/*
		Read_from:
		- Size fixed after building. Comes from the readed keys.
		- Could be None, when father txn useless and collected.
		- Corresponding to the is_read_from_fulfilled vector.
	 */
	pub read_from: Vec<ShouldSyncCell<Option<Weak<EvNode>>>>,       
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
	pub is_read_from_fulfilled: Vec<AtomicCell<bool>>,

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
	pub(in crate::tpg) fn from_template(event: &Event, idx: i32, txn: Weak<TxnNode>, reads_idx: Vec<usize>, write_idx: usize) -> Option<Self> {
		// Template
		let reads_length = event.reads.len();
        
        // Pre-allocate read_from and is_read_from_fulfilled vectors
        let mut read_from = Vec::with_capacity(reads_length);
		for _ in 0..reads_length {
		    read_from.push(ShouldSyncCell::new(None));
		}
        let is_read_from_fulfilled = std::iter::repeat_with(|| AtomicCell::new(false))
            .take(reads_length)
            .collect();

		if reads_idx.len() < event.reads.len() {
			None
		} else {
			Some(EvNode {
				read_from,
				read_by: RwLock::new(Vec::new()),
				txn,
				status: AtomicCell::new(EventStatus::CONSTRUCT),
				is_read_from_fulfilled,
				reads: event.reads
					.iter().enumerate()
					.map(|(idx, k)| format!("{}_{}", k, reads_idx[idx])).collect(),
				write: format!("{}_{}", event.write.clone(), write_idx),

				idx,
				has_write: event.has_write,
				has_storage_slot: false,
			})
		}
	}

	pub fn ready(&self) -> bool{
		// Thread safe. Reusable.
		match self.status.load() {
			EventStatus::WAITING | EventStatus::INQUEUE => {},
			_ => {return false},
		} 
		self.no_waiting()
	}

	// Nobody is waiting. May used before enqueue, since state is CONSTRUCT.
	pub fn no_waiting(&self) -> bool {
		self.is_read_from_fulfilled.iter().all(|i| i.load() == true)
	}

	// Wrapper. Calling execution handler.
	pub fn execute(&self, values: &Vec<Vec<u8>>, cnt: i32) -> (bool, Vec<u8>) {
		let mut value = Vec::new();

		for (i, vec) in values.iter().enumerate() {
			if i != 0 {
				value.push(b';'); // Push the semicolon separator if not the first vector
			}
			value.extend_from_slice(vec); // Append the bytes from the current vector
		}
		let txn_req_id = self.txn.upgrade().unwrap().txn_req_id;
		ffi::execute_event(
			txn_req_id,
			self.idx, 
			value, 
			cnt
		) // For now, let param_count as the same as query.
	}

	pub fn accept(&self) {
		/*
			This function is just single threaded:
			- Only one thread calling for one EvNode each time.
		 */
		debug_assert!(self.status.load() == EventStatus::CLAIMED);
		self.status.store(EventStatus::ACCEPTED);

		// Inform event accepted.
		self.txn.upgrade().unwrap().event_accepted();
		// Trigger txn commit handler.
		self.txn.upgrade().unwrap().try_commit();

	}

	pub fn write_back<T: Database>(&self, value: &Vec<u8>, db: &T) {
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
				|to| to.as_ref().is_some_and(
					|ref tn| Arc::ptr_eq(&tn, &son.txn.upgrade().unwrap())
			)) // Check if already inside.
		{
			self.txn.upgrade().unwrap().read_by
				.write().unwrap()
				.push(Some(son.txn.upgrade().unwrap().clone()))
		}
	}

	pub fn get_next_option_push_others_ready(&self, pipe: &Sender<Arc<EvNode>>) -> Option<Arc<EvNode>> {
		debug_assert!(self.status.load() == EventStatus::ACCEPTED);

		// Traverse sons to inform acceptance, and return the next node.
		let next_candidates: Vec<Arc<EvNode>> = (*self.read_by.read().unwrap())
			.iter().enumerate().filter(|(idx, node)| 
			{
				// Test who is ready. Will ignore those are under construction.
				node.as_ref().is_some_and(|n| {
					n.upgrade().unwrap().parent_accepted(*idx); // Inform parent ready.
					n.upgrade().unwrap().ready()
				})
			})
			.map(|(_, node)| 
				{
					node.as_ref().unwrap().upgrade().unwrap()
				}
			).collect();
		// Compare TS and select the minimum as the next.
		let min_evnode = next_candidates
			.iter()
			.min_by_key(|&evnode| 
				evnode.txn.upgrade().unwrap().ts
			);
		// Assign the next. Push others to queue.
		if min_evnode.is_some(){
			for evnode in &next_candidates {
				if !std::ptr::eq(evnode, *min_evnode.as_ref().unwrap()) {
					pipe.send(evnode.clone()).unwrap();
				}
			}
			// This evnode has been selected as min. Try lock with CAS to claim that node.
			match min_evnode.unwrap().status.compare_exchange(EventStatus::WAITING, EventStatus::CLAIMED) {
				Ok(_) =>  {
					Some(min_evnode.unwrap().clone())
				},
				// Has been claimed by Construct threads.
				Err(state) => {
					debug_assert!(state == EventStatus::INQUEUE);
					// Possible to be others. If it's enqueued long time ago and be claimed by others. But rare.
					None
				}
			}
		} else {
			None
		}
	}

	/*
		Abort the operation and then notify the whole transaction.
		This function called when:
		- CLAIMED evnode and WAITING transaction.
	 */
	pub fn notify_txn_abort(&self){
		debug_assert!(self.status.load() == EventStatus::CLAIMED 
			&& self.txn.upgrade().unwrap().status.load() == TxnStatus::WAITING);
		// Trigger txn abortion.
		self.txn.upgrade().unwrap().abort();
		// self.abort(); // Txn abortion includes self abortion.
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
		- If an operation has been marked INQUEUE, we believe no change has ever happened. Just set it to be WAITING.
		- If an operation has been marked CLAIMED, reset it to be WAITING may cause two threads claiming one evnode at the same time. So we stop and loop waiting. It must be rare condition.
		- If an operation has been marked ACCEPTED, we believe its state change has happened. So we do recover works.
		- If an operation has been marked ABORTED, we believe its state has been properly handled. We do nothing about it.
	 */
	pub	fn abort(&self) {
		debug_assert!( self.status.load() != EventStatus::CONSTRUCT ); // A node can be aborted anytime but not construct. The whole txn is not ready to work.
		debug_assert!( self.txn.upgrade().unwrap().status.load() == TxnStatus::ABORTED );
		self.status.store(EventStatus::ABORTED);
		// TODO. Preallocate the vector.
		let mut stack: Vec<Arc<EvNode>> = Vec::with_capacity(20);
		// Shit... I just want a Arc<EvNode> of self...
		stack.push(self.txn.upgrade().unwrap().ev_nodes.read()[self.idx as usize].clone());
		while stack.len() != 0 {
			let parent = stack.pop().unwrap();
			for (idx, son) in parent.read_by.read().unwrap().iter().enumerate(){
				let node = son.as_ref().unwrap().upgrade().unwrap(); // Son could not be none.
				match node.status.load() {
					EventStatus::ACCEPTED => {
						// State shift has been made. Recover the state shift and dive in.
						node.status.store(EventStatus::WAITING);
						let origin = node.is_read_from_fulfilled[idx].swap(false);
						debug_assert!(origin); // Orginally must be true. Set false now.
						simpledb::DB.get().unwrap()
							.reset_version("default", &self.write, self.txn.upgrade().unwrap().ts);
						// Reset later dependent nodes.
						stack.push(node); // It could produce wrong result to be used by sons.
					}
					EventStatus::ABORTED => continue, // Ends here. Has been operated by other abortion thread.
					EventStatus::CLAIMED => {
						println!("Event Abortion: Rare condition. A claimed node needs to be reset.");
						// Busy wait till that thread release Event. 
						while node.status.load() != EventStatus::CLAIMED {}
					},
					EventStatus::WAITING => {
						// No state shift happened. No change.
						debug_assert!(node.is_read_from_fulfilled[idx].load() == false); // Not executed yet.
						continue;
					},
					EventStatus::INQUEUE => {
						node.status.store(EventStatus::WAITING); // Just set waiting.
						let origin = node.is_read_from_fulfilled[idx].swap(false);
						debug_assert!(origin); // Orginally must be true. Set false now.
						// Nothing to push. It's not done yet.
					}
					EventStatus::CONSTRUCT => panic!("bug."),
				}
			}
		};
		// Copy last state result only happens for aborted nodes. For those redo ones, just set empty.
		if self.has_write{
			simpledb::DB.get().unwrap()
				.copy_last_version("default", &self.write, self.txn.upgrade().unwrap().ts); 
		}
	}

	// Notification from parents in read_from.
	fn parent_accepted(&self, idx: usize) {
		debug_assert!(
			match self.status.load(){
				EventStatus::CONSTRUCT | EventStatus::WAITING | EventStatus::INQUEUE => true,
				_ => { println!("{:?}", self.status.load()); false }
			}
		);
	    self.is_read_from_fulfilled[idx].swap(true);
	}

}