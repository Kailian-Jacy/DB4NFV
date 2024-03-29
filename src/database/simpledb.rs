use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::fmt::Debug;
use crate::config::CONFIG;
use crate::database::api;
use crate::ds::ringbuf::{self, RingBuf};

pub(crate) static DB: OnceCell<SimpleDB> = OnceCell::new();

// Multi-version states engine
pub struct SimpleDB {
	tables: HashMap<String, Table>,
}

impl api::Database for SimpleDB {
	fn new() -> Self {
		SimpleDB { tables: HashMap::new() }
	}

	fn add_table(&mut self, to_add_table: &str, keys: Vec<&str>) {
		self.tables.insert(String::from(to_add_table), Table::empty_init(keys));
	}

	fn reset_version(&self, table: &str, key: &str, ts: u64) {
		self.tables[table].reset_version(key, ts);	
    }

	fn write_version(&self, table: &str, key: &str, ts: u64, value: &Vec<u8>) {
		self.tables[table].write_version(key, ts, value);	
    }

	fn push_version(&self, table: &str, key: &str, ts: u64, value: &Vec<u8>) {
		self.tables[table].push_version(key, ts, value);	
    }

	fn copy_last_version(&self, table: &str, key: &str, ts: u64, has_slot: bool) {
		self.tables[table].copy_last_version(key, ts, has_slot);	
    }

	fn release_version(&self, table: &str, key: &str, ts: u64) {
		self.tables[table].release_version(key, ts);	
    }

	fn get_version(&self, table: &str, key: &str, ts: u64) -> Vec<u8> {
		self.tables[table].get_version(key, ts)
    }

}

struct Table {
	states: HashMap<String, usize>,
	// records: HashMap<String, Vec<DataPoint<String>>>,
	records: Vec<ringbuf::RingBuf<DataPoint<Vec<u8>>>>,
}

#[derive(Default, Clone, Debug)]
struct DataPoint<T: Default> {
	ts: u64,
	value: T,
	state: DataPointState,
}


// Debug state. Could just remove.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataPointState {
	// DEFAULT,
	NORMAL,
	// ABORTED,	// Aborted modes, but copying last valid result. TODO: Possibly remove it?
	EMPTY, // resetted or not used.
}

impl Default for DataPointState {
	fn default() -> Self {
		DataPointState::EMPTY
	}	
}

impl<T: Default + Clone > ringbuf::RingBufContent for DataPoint<T> {}

impl Table {
	fn empty_init(keys: Vec<&str>) -> Self {
		let mut states = HashMap::new();
		let mut records = Vec::<ringbuf::RingBuf<DataPoint<Vec<u8>>>>::new();
		let states_per_key = CONFIG.read().unwrap().max_state_records;
		keys.iter().enumerate().for_each(|(i, &k)|  {
			for j in (0..states_per_key).into_iter() {
				// For each key, we have states_per_key rows. each contains a series of time stamp.
				states.insert(format!("{}_{}", k, j), i * states_per_key + j);
				records.push(RingBuf::new(CONFIG.read().unwrap().ringbuffer_size as usize, Some(CONFIG.read().unwrap().ringbuffer_full_to_panic)));
			}
			if CONFIG.read().unwrap().verbose{
				println!("[DEBUG] key {} initiated for {} times.", k, states_per_key);
			}
		});
		Table{
			states: states,
			records: records,
		}
	}

	// Called by accepted operations that being reset by aborted ancestors.
	fn reset_version(&self, key: &str, ts: u64){
	// This only called on obj with normal states.
		debug_assert!(self.states.contains_key(key));
		let obj = self.records[self.states[key]]
			.object_as_ordered(Box::new(move |dp: &DataPoint<Vec<u8>>| dp.ts.cmp(&ts)));
		// This only called on obj to be aborted. Should have been written NORMAL result.
		debug_assert!({
			obj.is_some() && obj.as_ref().unwrap().state == DataPointState::NORMAL
		});
		obj.unwrap().state = DataPointState::EMPTY;
	}

	// Copy last version happens when operations are aborted, so it fetches the resulf of last valid record.
	/*
		This function may happens to:
		1. ACCEPTED evnode. In the same transaction as abortion evnode.
		2. WAITING evnode. 
	 */
	fn copy_last_version(&self, key: &str, ts: u64, has_slot: bool){
	// This only called on obj to be aborted. Should have been written NORMAL result.
		debug_assert!(self.states.contains_key(key));
		let r = &self.records[self.states[key]];
		// Find position of the current.
		if !has_slot {
			// TODO. Default value.
			let mut value = vec![0];
			if r.len() != 0 {
				// Search back.
				let to_copy_op = r.search_back( Box::new(
						|t| {t.state == DataPointState::NORMAL}
					), r.len() - 1); // The last index of ringbuf is len-1.
				if to_copy_op.is_some() {
					value = to_copy_op.unwrap().read().unwrap().value.clone()
				};

			}
			self.push_version(
				key, ts, &value
			);
		} else {
			let (idx, new) = self.records[self.states[key]]
				.ref_as_ordered(Box::new(move |dp: &DataPoint<Vec<u8>>| dp.ts.cmp(&ts))).unwrap();
			let to_copy_op = r.search_back( Box::new(
					|t| {t.state == DataPointState::NORMAL}
				), idx);
			let value = if to_copy_op.is_none() {
				// Dated back to 0. Use default value.
					vec![0]
				} else {
					to_copy_op.unwrap().read().unwrap().value.clone()
				};
			let mut new_w = new.write().unwrap();
			new_w.value = value;
			new_w.state = DataPointState::NORMAL;
		}
		// WARNING: TODO Abortion deprecated here.
	}
	
	// Stage version inserts the version at the end. The t is guaranteed to be the last version.
	fn push_version(&self, key: &str, ts: u64, value: &Vec<u8>) {
		// Insert dataPoint into vectors. Keep correct order.
		debug_assert!(self.states.contains_key(key));
		debug_assert!({ // Make sure it's not repeatedly pushed.
			let obj = self.records[self.states[key]]
				.object_as_ordered(Box::new(move |dp: &DataPoint<Vec<u8>>| dp.ts.cmp(&ts)));
			obj.is_none()
		});
		debug_assert!(value.len() != 0);
		debug_assert!({ // Make sure is increasing order.
			let t = &self.records[self.states[key]];
			let n = t.last_clone();
			n.is_none()	|| n.is_some_and(|dp| dp.ts < ts)
		});
		// Create a new DataPoint with the provided timestamp and value.
		let new_data_point = DataPoint {
			ts: ts,
			value: value.clone(),
			state: DataPointState::NORMAL,
		};
		self.records[self.states[key]].push(new_data_point);
	}

	// Stage version inserts the version at in the middle. We find it first.
	fn write_version(&self, key: &str, ts: u64, value: &Vec<u8>) {
		// Insert dataPoint into vectors. Keep correct order.
		debug_assert!(self.states.contains_key(key));
		let obj_ref = self.records[self.states[key]]
			.ref_as_ordered(Box::new(move |dp: &DataPoint<Vec<u8>>| dp.ts.cmp(&ts)));
		debug_assert!(value.len() != 0);
		debug_assert!({
			obj_ref.unwrap().1
				.read().unwrap()
				.state == DataPointState::EMPTY
		});
		obj_ref.unwrap().1.write().unwrap().state = DataPointState::NORMAL;
		obj_ref.unwrap().1.write().unwrap().value = value.clone();
	}

	fn release_version(&self, key: &str, ts: u64){
		// Remove datapoint from ringbuf.
		debug_assert!(self.states.contains_key(key));
		debug_assert!(
			if !self.records[self.states[key]].first_clone().unwrap().ts == ts{
				self.records[self.states[key]].dump();
				false
			} else {
				true
			}
		); // Should be the very first of the key.
		let mut w = self.records[self.states[key]].ref_as_ordered(
			Box::new(move |dp| dp.ts.cmp(&ts) )
		).expect("bug").1.write().unwrap();
		w.state = DataPointState::EMPTY;
		self.records[self.states[key]].discard_before(1);
	}

	fn get_version(&self, key: &str, ts: u64) -> Vec<u8>{
		// Get datapoint from ringbuf.
		debug_assert!(self.states.contains_key(key));
		let obj_op = self.records[self.states[key]]
			.object_as_ordered(Box::new(move |dp: &DataPoint<Vec<u8>>| dp.ts.cmp(&ts)));
		debug_assert!({
			if obj_op.is_none(){
				self.records[self.states[key]].dump();
				false
			} else {
				true
			}
		});
		debug_assert!(obj_op.as_ref().unwrap().state == DataPointState::NORMAL);
		// Remove Later. Now we consider get visited the wrong place where initiated value used. Consider to add states to debug.
		debug_assert!(obj_op.as_ref().unwrap().value.len() != 0);
		obj_op.unwrap().value.clone()
	}
}

#[cfg(test)]
mod test{
	// Keep target and implemented traits visisble to reach the traits method.
	use crate::database::api::*;
	use super::*;

	#[test]
    fn test_add_table() {
        let mut db = SimpleDB::new();
        db.add_table("table1", vec!["key1", "key2"]);
        assert!(db.tables.contains_key("table1"));
    }
}