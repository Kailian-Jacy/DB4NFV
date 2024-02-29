use std::cell::{Cell, RefCell};
use std::sync::{Arc, RwLock, Weak};
use std::collections::HashMap;
use crossbeam::atomic::AtomicCell;

use crate::database::api::Database;
use crate::database::simpledb;
use crate::ds::events::Event;
use crate::ds::transactions::TXN_TEMPLATES;
use crate::external::ffi::{self, TxnMessage};
use crate::tpg::ev_node::{EvNode, EventStatus};
use crate::utils::ShouldSyncCell;

// Node linked to Construct TPG.
pub struct TxnNode{
	// TPG Links.
	/*
		read_from:
		- Size fixed after building. Comes from joined keys. Template should all be empty.

	 */
	pub read_from: Vec<ShouldSyncCell<Option<Arc<TxnNode>>>>,           			// No Lock needed. Set after initialization. This link used for marking parent txn as garbage and release at correct time.
	pub read_by: RwLock<Vec<Option<Weak<TxnNode>>>>,	            // Needs Lock. read_by updates dynamically. This link used for try_commit this txn.
	pub cover: HashMap<String, ShouldSyncCell<Option<Arc<TxnNode>>>>,	            // No Lock needed. Only being edited in construction. 
	pub covered_by: RwLock<HashMap<String, Option<Weak<TxnNode>>>>, // RwLock needed. Read by many, could written by many. 

	// Meta.
	pub status: AtomicCell<TxnStatus>,
	pub ev_nodes: ShouldSyncCell<Vec<Arc<EvNode>>>, 	// Holds EvNode ownership. Consider switch to exclusive ownership.  Now the RWLock is used to setup loopback reference.
	pub txn_req_id: u64,
	// pub joined_states_to_read: Vec<&'a str>, // Reference to String. Since from ev_nodes.
	pub ts: u64,

	// State count
	uncommitted_parents: AtomicCell<u16>, // When WAITING.
	unfinished_events: AtomicCell<u16>,   // When WAITING.
}

impl Drop for TxnNode {
	// When reference goes to 0, Drop happens.
	/*
		A transaction marked as drop means its resources are totally out-of-date.
		- The result it produced has been covered by other committed transaction.
		- Those transaction requiring the result of this transaction has been committed.
		- Tpg state_last_modify records does not hold reference to this TxnNode.
		So drop happens. Txn should release all the resource it holds:
		- All versions stored in database.
		During drop, since all of its parents has been released, including parents writing the same key, 
			release versoin should just increase the start cursor by 1 to finish releasing.
	 */
	fn drop(&mut self) {
		debug_assert!(self.status.load() == TxnStatus::COMMITED);
		debug_assert!(self.read_from.iter().all(|tn| tn.read().is_none())
						&& self.cover.iter().all(|(k, v)| v.read().is_none())
					);
		self.ev_nodes.write().iter().for_each(|en| {
			simpledb::DB.get().unwrap().release_version("default",&en.write, self.ts);
		});
	}	
}

impl TxnNode{
	/*
		Construct TxnNode from message and template.
	 */
	pub fn from_message(msg: TxnMessage) -> Arc<Self> {
		let tpl = &TXN_TEMPLATES.get().unwrap()[msg.type_idx as usize];
		// TODO. Allocate from manager.
		let ta = Arc::new(TxnNode{
				read_from: Vec::with_capacity(tpl.all_reads_length),  // Of the same size.h
				read_by: RwLock::new(Vec::new()), // Empty and to construct.
				cover: tpl.es
					.iter().filter(|&e| e.has_write )
					.map(|e| (e.write.clone(), ShouldSyncCell::new(None)))
					.collect(),
				covered_by: RwLock::new(HashMap::new()),

				status: AtomicCell::new(TxnStatus::WAITING),
				ev_nodes: ShouldSyncCell::new(Vec::new()),
				txn_req_id: msg.txn_req_id,

				ts: 0,
				uncommitted_parents: AtomicCell::new(0),
				unfinished_events: AtomicCell::new(0),
			});
		let ev_nodes: Vec<Arc<EvNode>> = tpl.es.iter().enumerate()
			.map(|(idx, en)| Arc::new(
				EvNode::from_template(
					en,
					idx as i32,
					Arc::downgrade(&ta.clone()),
				)
			))
			.collect();
		let mut ev_nodes_place = ta.ev_nodes.write();
		*ev_nodes_place = ev_nodes;
		drop(ev_nodes_place);
		ta
	}

	// set links on tpg for eventNodes. This function is dangerous.
	pub fn set_links(&self, tb: &RwLock<HashMap<String, Option<(Weak<EvNode>,Arc<TxnNode>)>>>) {
		/*
			add_dependency executes steps in sequence to ensure safety:
			1. Find the parent. Append to self.read_from;
			2. Init the completion vector according to the state;
			3. Set link from the parent to self;
			4. Decide the evNode to go to queue or to be landed by node.
			Unverified.
		 */
		debug_assert!(self.status.load() == TxnStatus::WAITING);
		self.ev_nodes.read().iter().map(|en| {
			debug_assert!(en.status.load() == EventStatus::CONSTRUCT);
			// Set read_from and parent read_by.
			let last_modify_hashmap = tb.read().unwrap();
			// Anyway, update.
			en.reads.iter().enumerate().for_each(|(idx, r)|{
				let last_option = last_modify_hashmap.get(r.as_str());
				if last_option.is_some() {
					// 	Find parent. Do have last.
					let last = last_option.unwrap().as_ref().unwrap();
					// Must linking to non-garbage txn.
					debug_assert!(last.1.status.load() != TxnStatus::GARBAGE);
					// TODO. Check if allocation.
					let mut e = en.read_from[idx].write();
					*e = Some(last.0.clone());
					en.is_read_from_fulfilled[idx].store(
						last.0.upgrade().unwrap().status.load() == EventStatus::ACCEPTED
					);
					last.0.upgrade().unwrap().add_read_by(&en);
				} else {
					let mut e = en.read_from[idx].write();
					*e = None;
				}
			});
			drop(last_modify_hashmap);
			if en.has_write {
				let self_arc =  en.txn.upgrade().unwrap().clone();
				let mut last_modify_hashmap = tb.write().unwrap();
				// Set self.cover and parent self.covered_by.
				if let Some(Some(last)) = last_modify_hashmap.get(en.write.as_str()){
					// Someone wrote. record and update list.
					let mut re = self.cover[&en.write].write(); // Clone and remove later.
					*re = Some(last.1.clone());
					last.1.add_covered_by(&en.write, &self_arc);
				} else {};
				// Update to state_last_modify anyway.
				/*
					Here we assign a reference count to prevent the txn removed. 
					Explicitly clone.
				 */
				tb.write().unwrap().insert(en.write.clone(), Some((Arc::downgrade(en),self_arc)));
			}
		});
	}

	// Safe function. Only happens to WAITING txn.
	pub fn event_accepted(&self) {
		debug_assert!(self.status.load() == TxnStatus::WAITING);
		debug_assert!(self.unfinished_events.load() > 0);
		self.unfinished_events.fetch_sub(1);
	}

	// Add_covered_by adds txn that writes the same key as constraint.
	pub fn add_covered_by(&self, key: &String, tn: &Arc<TxnNode>) {
		debug_assert!({ !self.covered_by // impossible to be added twice.
			.read().unwrap()
			.get(key).is_none()
		});
		self.covered_by.write().unwrap()
			.insert(key.clone(), Some(Arc::downgrade(tn)));
	}

	// Safe function.
	pub fn father_committed(&self) {
		debug_assert!(
			self.status.load() == TxnStatus::WAITING
				|| self.status.load() == TxnStatus::ABORTED // Aborted transaction also passes commitment to descedants.
		);
		debug_assert!(self.uncommitted_parents.load() > 0);
		self.uncommitted_parents.fetch_sub(1);
	}

	// Try to commit this transaction. Only happens to WAITIING/ABORTED transaction. Remind, ABORTED txn could also be committed.
	pub fn try_commit(&self) -> bool {
		/*
			When to trigger this function:
			1. When the last of the event inside this transaction all finised. (Waiting/Aborted)
			2. When the parents of this txn all committed. (Waiting)
			This function is:
			1. Thread safe to call.
			2. Would not be effected by appending new transactions. Only dependent to parents.
			3. Would not be effected by removing parents. Its parents can't be removed before my commitment.
			When this function be called:
			1. Could be called by multiple threads at the same time.
			2. At most only one would succeed and apply any change.
		 */
		debug_assert!(
			self.status.load() == TxnStatus::WAITING
			|| self.status.load() == TxnStatus::ABORTED
		);
		// match self.status.load() {
		// 	TxnStatus::WAITING => {},
		// 	_ => return false,
		// }
		if !(self.unfinished_events.load() == 0 && self.uncommitted_parents.load() == 0) {
			return false
		}
		// Having decide this txn can be commmitted.
		self.status.store(TxnStatus::COMMITED);

		// Continue to apply other changes.
		
		// Record decrement on sons.
		for son in self.read_by.read().unwrap().iter() {
			let node = son.as_ref().unwrap().upgrade().unwrap(); // Son could not have been released. 
			node.father_committed();
			// We don not think a network situation would allow so much txn linked to cause stack overflow. So recursion here.
			node.try_commit();
		}

		// Release unused reference to release parentsRemove the linking to decrease reference count in either condition.
		self.read_from.iter().for_each(|parent| {
			debug_assert!({
				parent.read().is_none() 
				 || parent.read().as_ref().unwrap().status.load() == TxnStatus::COMMITED
			}); // Could not be waiting or garbaged.
			let mut wp = parent.write();
			*wp = None;
		});
		self.cover.iter().for_each(|(_, w_parent)|{
			debug_assert!({
				w_parent.read().is_none() 
				 || w_parent.read().as_ref().unwrap().status.load() == TxnStatus::COMMITED
			}); // Could not be waiting or garbaged.
			let mut wp = w_parent.write();
			*wp = None;
		});

		debug_assert!({
			self.read_from.iter().all(|r| r.read().is_none()) 
			&& self.cover.iter().all(|(_, r)| r.read().is_none()) 
		});

		// Inform the runtime that the txn has been processed.
		ffi::txn_finished_sign(self.txn_req_id);

		true
	}

	// // Judge if it's garbage. If it is, remove the related versions from database.
	// pub fn is_garbage(&self) -> bool {
	// 	/*
	// 		When to trigger this function:
	// 		1. When any of its descendants transaction commits.
	// 		2. When any of its descendants aborted and nested descendants committed.
	// 		When we can mark it as garbage:
	// 		1. All the state it writes has been covered by other committed transaction. 
	// 			If any of these transaction aborted, search down its writing covering descendants.
	// 		2. All the transactions reading what it produced has been aborted or committed.
	// 		This function is:
	// 		1. Thread safe to call.
	// 		2. Would not be effected by appending new transactions.
	// 		3. Would not be effected by removing parents.
	// 		4. Would not be effected by atomic abortion.
	// 	 */
	// 	match self.status.load() {
	// 		TxnStatus::COMMITED => {}
	// 		_ => return false
	// 	}
	// 	// Check writing coverage.
	// 	for (key, w_son) in self.covered_by.read().unwrap().iter() { 
	// 		let node = w_son.as_ref().unwrap().upgrade().unwrap(); // No descendants should be released before me.
	// 		match node.status.load() {
	// 			TxnStatus::COMMITED => {},
	// 			TxnStatus::ABORTED => {
	// 				// Search down in iteration.
	// 				let mut c = node;
	// 				let mut has_end = false;
	// 				loop {
	// 					let d = c.covered_by.read().unwrap().get(key);
	// 					match d {
	// 						None => {return false}, // Nobody's covering writing this key.
	// 						Some(n) => {
	// 							let n = n.as_ref().unwrap().upgrade().unwrap(); // No descendants should be released before him.
	// 							match n.status.load() {
	// 								TxnStatus::COMMITED => { has_end = true; break },
	// 							    TxnStatus::ABORTED => { c = n; continue }, // Search next iteration.
	// 								_ => { return false }
	// 							}
	// 						}
	// 					}
	// 				}
	// 			},
	// 			_ => return false
	// 		}
	// 	}
	// 	// Check reading coverage.
	// 	for son in self.read_by.read().unwrap().iter() {
	// 		let node = son.as_ref().unwrap().upgrade().unwrap(); // No descendants should be released before me.
	// 		match node.status.load() {
	// 			TxnStatus::COMMITED => {},
	// 			TxnStatus::ABORTED => {},
	// 			_ => {return false}
	// 		}
	// 	}
	// 	// Should be marked true.
	// 	true
	// }

	// Transaction abortion. 
	pub fn abort(&self) {
		/*
			This function: 
			- Only happens in waiting transactions. Triggered by evNode that arose abortion.
			This function: 
			1. Needs to be thread safe. Multiple abortion process could be overlapped.
			This function does:
			1. Notify opts to abort.
			2. Set self status to be abortion. 
			3. Try to commit.
		 */
		debug_assert!(self.status.load() == TxnStatus::WAITING);
		self.status.store(TxnStatus::ABORTED);
		self.ev_nodes
			.read().iter().map(|e|e.abort());
		self.unfinished_events.store(0);
		self.try_commit();
	}

}

// impl Clone for TxnNode{
// 	fn clone(&self) -> Self {
// 		TxnNode {
// 			ances: self.ances.clone(),
// 			desce: self.desce.clone(),
// 			status: AtomicCell::new(self.status.load()),
// 			ev_nodes: self.ev_nodes.clone(),
// 			txn_req_id: self.txn_req_id,
// 		}
// 	}
// }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
	// Waiting marks the unfinished dependent txns counts.
	WAITING,	
	// Committed returns value. 
	// Remind: To pass commitment down, we'll also mark aborted txn to be committed when its parents all committed.
	COMMITED, 
	ABORTED,
	GARBAGE,
}
