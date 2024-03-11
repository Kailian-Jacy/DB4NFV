use std::mem;
use std::sync::{Arc, RwLock, Weak};
use std::collections::HashMap;
use crossbeam::atomic::AtomicCell;

use crate::database::api::Database;
use crate::database::simpledb;
use crate::ds::transactions::TXN_TEMPLATES;
use crate::external::ffi::{self, TxnMessage};
use crate::tpg::ev_node::{EvNode, EventStatus};
use crate::utils::ShouldSyncCell;

// Node linked to Construct TPG.
#[derive(Debug)]
pub struct TxnNode{
	// TPG Links.
	/*
		These links composes the whole TPG.
		When those nodes are in use, they are in the style of cyclic counted reference (Arc) to keep alive.
		When one end in the link decides the other end is useless, it release the count reference.
		When some node is discarded by every neighbor, it got collected by drop().
	 */
	/*
		read_from: This link used for marking parent txn as garbage and release at correct time.
		- No Lock needed. Set after initialization. 
		- Construct by construct_thread with add_link function.
		- Release when son is committed so its parents resources must be useless.
	 */
	pub read_from: Vec<ShouldSyncCell<Option<Arc<TxnNode>>>>,           			
	pub read_from_index_map: HashMap<String, usize>,
	/*
		read_by: This link used to reach its son and keep its son 
		- Needs Lock. read_by updates dynamically. This link used for try_commit this txn.
		- Construct dynamically, when later txn constructs, they add link to this older txn.
		- Never drops by hand. Only drops when the parent dropped. 
	 */
	pub read_by: RwLock<Vec<Option<Arc<TxnNode>>>>,	        
	/*
		Cover: This link used to reach its write parent and keep its write parent. (Parent txn writing the same state.)
		- No Lock needed. 
		- Construct by construct_thread. Only being edited in construction.  
		- Release when son is committed so its parents resources must be useless.
	 */
	pub cover: HashMap<String, ShouldSyncCell<Option<Arc<TxnNode>>>>,
	/*
		Covered_by: This link used to reach its write Son and keep its write Son. (Son txn writing the same state.)
		- RwLock needed. Single thread insert (Constructor), multiple thread write (Set none), multiple thread reads.
		- Construct dynamically, when later txn constructs, they add link to this older txn.
		- Never drops by hand. Only drops when the parent dropped. 
	 */
	pub covered_by: RwLock<HashMap<String, Option<Arc<TxnNode>>>>, 

	// Meta.
	pub status: AtomicCell<TxnStatus>,
	pub ev_nodes: ShouldSyncCell<Vec<Arc<EvNode>>>, 	// Holds EvNode ownership. Consider switch to exclusive ownership.  Now the RWLock is used to setup loopback reference.
	pub txn_req_id: u64,
	pub ts: u64,

	// State count
	unfinished_events: AtomicCell<u16>,   // When WAITING.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
	// Waiting marks the unfinished dependent txns counts.
	WAITING,	
	// Committed returns value. 
	// Remind: To pass commitment down, we'll also mark aborted txn to be committed when its parents all committed.
	COMMITED, 
	ABORTED,
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
		// TODO. A waiting txn hitted here.
		debug_assert!(self.status.load() == TxnStatus::COMMITED); // Aborted becomes COMMITTED in the end.
		debug_assert!(self.read_from.iter().all(|tn| tn.read().is_none())
						&& self.cover.iter().all(|(_k, v)| v.read().is_none())
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
	pub fn from_message(mut msg: TxnMessage) -> Option<Arc<Self>> {
		let tpl = &TXN_TEMPLATES.get().unwrap()[msg.type_idx as usize];
		let mut index_map = HashMap::new();
		let mut read_from_array = Vec::new();
		tpl.es.iter().enumerate().for_each(|(e_idx, e)| 
			e.reads.iter().enumerate().for_each(|(r_idx, state)| {
				index_map.insert(format!("{}_{}", state, msg.reads_idx[e_idx][r_idx]), read_from_array.len());
				read_from_array.push(ShouldSyncCell::new(None));
			})
		);
		let ta = Arc::new(TxnNode{
				read_from: read_from_array,
				read_from_index_map: index_map,
				read_by: RwLock::new(Vec::new()), // Empty and to construct.
				cover: tpl.es
					.iter().enumerate().filter(|(_, e)| e.has_write )
					.map(|(idx, e)| (format!("{}_{}", e.write, msg.write_idx[idx]), ShouldSyncCell::new(None)))
					.collect(),
				covered_by: RwLock::new(HashMap::new()),

				status: AtomicCell::new(TxnStatus::WAITING),
				ev_nodes: ShouldSyncCell::new(Vec::new()),
				txn_req_id: msg.txn_req_id,

				ts: msg.ts,
				// uncommitted_parents: AtomicCell::new(0),
				unfinished_events: AtomicCell::new(tpl.es.len() as u16),
			});
		if msg.reads_idx.len() < tpl.es.len() || msg.write_idx.len() < tpl.es.len() {
			return None
		}
		let mut ev_nodes = Vec::new();
		for (idx, en) in tpl.es.iter().enumerate() {
			ev_nodes.push(
				Arc::new(
					EvNode::from_template(
						en,
						idx as i32,
						Arc::downgrade(&ta.clone()),
						mem::take(&mut msg.reads_idx[idx]),
						msg.write_idx[idx],
					)?
				)
			)
		}
		let mut ev_nodes_place = ta.ev_nodes.write();
		*ev_nodes_place = ev_nodes;
		drop(ev_nodes_place);
		// if CONFIG.read().unwrap().debug_mode {
		// 	println!("[DEBUG] Received txn {:?}", ta);
		// }
		Some(ta)
	}

	// set links on tpg for eventNodes. This function is dangerous. Only call from construct thread.
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
		self.ev_nodes.read().iter().for_each(|en| {
			debug_assert!(en.status.load() == EventStatus::CONSTRUCT); 
			// Set read_from and parent read_by.
			let last_modify_hashmap = tb.read().unwrap();
			// Anyway, update.
			en.reads.iter().enumerate().for_each(|(idx, state)|{
				let last_option = last_modify_hashmap.get(state.as_str())
					.unwrap_or_else(|| 
						{panic!("State has no slot in last_modify_hashmap: {:?}", state)}
					);
				if last_option.is_some() {
					// 	Find parent. Do have last.
					let (last_en, last_tn) = last_option.as_ref().unwrap();
					// TODO. Check if allocation.
					// Set this event.read_from
					let mut e = en.read_from[idx].write();
					*e = Some(last_en.clone());
					// Set read by for both parent evNode and txnNode.
					last_en.upgrade().unwrap().add_read_by(en);
					// Write to correspondent
					let mut wr = self.read_from_by_state(state).write(); 
					if (*wr).is_none() {
						*wr = Some(last_tn.clone());
					}
				} else {
					let mut e = en.read_from[idx].write();
					*e = None;
				}
			});
			drop(last_modify_hashmap);
			if en.has_write {
				let self_arc =  en.txn.upgrade().unwrap().clone();
				// Set self.cover and parent self.covered_by.
				let last_modify_hashmap = tb.read().unwrap();
				let last_option = last_modify_hashmap.get(&en.write)
					.unwrap_or_else(|| 
						{panic!("State has no slot in last_modify_hashmap: {:?}", &en.write)}
					);
				if let Some((_, last_txn)) = last_option {
					// Someone wrote. record and update list.
					let mut re = self.cover[&en.write].write(); // Clone and remove later.
					*re = Some(last_txn.clone());
					last_txn.add_covered_by(&en.write, &self_arc);
				}
				// Update to state_last_modify anyway.
				/*
					Here we assign a reference count to prevent the txn removed. 
					Explicitly clone.
				*/
				drop(last_modify_hashmap);
				let mut last_modify_hashmap = tb.write().unwrap();
				last_modify_hashmap.insert(en.write.clone(), Some((Arc::downgrade(en),self_arc)));
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
	pub fn add_covered_by(&self, key: &String, son: &Arc<TxnNode>) {
		debug_assert!({ self.covered_by // impossible to be added twice. So this slot must be none.
			.read().unwrap()
			.get(key).is_none()
		});
		self.covered_by.write().unwrap()
			.insert(key.clone(), Some(son.clone()));
	}

	// Add_read_by adds txn that reads my result.
	pub fn read_from_by_state(&self, state: &str) -> &ShouldSyncCell<Option<Arc<TxnNode>>>{
		&self.read_from[self.read_from_index_map[state]]
	}

	pub fn no_waiting(&self) -> bool {
		self.read_from.iter().all(|tn| tn.read().is_none())
	}

	// Safe function.
	pub fn father_committed(&self, father: &TxnNode) {
		debug_assert!(
			self.status.load() == TxnStatus::WAITING
				|| self.status.load() == TxnStatus::ABORTED // Aborted transaction also passes commitment to descedants.
		);
		// Find father himself in the son's reading list. To decrease the uncommitted parents count.
		if !self.read_from.iter().any(|tn_op	| {
			if tn_op.read().as_ref().is_some_and(|tn| tn.ts == father.ts ) {
				// Set to None.
				let mut w = tn_op.write();
				*w = None;
				true
			} else {
				false
			}
		}) {
			panic!("committed father not in son read_from.");
		}
		// debug_assert!(self.uncommitted_parents.load() > 0);
		// self.uncommitted_parents.fetch_sub(1);
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
		if !(self.unfinished_events.load() == 0 && self.no_waiting()) {
			return false
		}
		// Having decide this txn can be commmitted.
		self.status.store(TxnStatus::COMMITED);

		// Continue to apply other changes.
		
		// At this time read from should all be none.
		// self.read_from.iter().for_each(|parent| {
		// 	debug_assert!({
		// 		parent.read().is_none() 
		// 		 || parent.read().as_ref().unwrap().status.load() == TxnStatus::COMMITED
		// 	}); // Could not be waiting or garbaged.
		// 	let mut wp = parent.write();
		// 	*wp = None;
		// });
		self.cover.iter().for_each(|(_, w_parent)|{
			debug_assert!({
				w_parent.read().is_none() 
				 || w_parent.read().as_ref().unwrap().status.load() == TxnStatus::COMMITED
			}); // Could not be waiting or garbaged.
			let mut wp = w_parent.write();
			*wp = None;
		});

		debug_assert!({
			self.read_from.iter().all(|state| state.read().is_none()) 
			&& self.cover.iter().all(|(_, state)| state.read().is_none()) 
		});

		// Inform the runtime that the txn has been processed.
		// TODO. Judge by self.status to reply ILLEGAL, SUCCESS, or what.
		ffi::txn_finished_sign(self.txn_req_id);

		// Perform commitment on dependent sons. This step should be the last part of commitment, since we need commitment to be in order.
		for son in self.read_by.read().unwrap().iter() {
			let node = son.as_ref().unwrap(); // Son should not have been released. 
			node.father_committed(self);
			// We do not think a network situation would allow so much txn linked to cause stack overflow. So recursion here.
			node.try_commit();
		}

		true
	}

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
			.read().iter().for_each(
				|e| 
				if e.status.load() != EventStatus::ABORTED { e.abort() }
			);
		self.unfinished_events.store(0);
		// Abortion txn also commits. Since the later transactions dates back to check if commitable.
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
