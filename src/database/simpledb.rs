use once_cell::sync::OnceCell;
use std::collections::HashMap;
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

	fn write_version(&self, table: &str, key: &str, ts: u64, value: &String) {
		self.tables[table].write_version(key, ts, value);	
    }

	fn push_version(&self, table: &str, key: &str, ts: u64, value: &String) {
		self.tables[table].push_version(key, ts, value);	
    }

	fn copy_last_version(&self, table: &str, key: &str, ts: u64) {
		self.tables[table].copy_last_version(key, ts);	
    }

	fn release_version(&self, table: &str, key: &str, ts: u64) {
		self.tables[table].release_version(key, ts);	
    }

	fn get_version(&self, table: &str, key: &str, ts: u64) -> String {
		self.tables[table].get_version(key, ts)
    }

}

struct Table {
	states: HashMap<String, usize>,
	// records: HashMap<String, Vec<DataPoint<String>>>,
	records: Vec<ringbuf::RingBuf<DataPoint<String>>>,
}

#[derive(Clone)]
struct DataPoint<T: Default> {
	ts: u64,
	value: T,
	state: DataPointState,
}

// Debug state. Could just remove.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataPointState {
	DEFAULT,
	NORMAL,
	ABORTED,
	SHOULDBEEMPTY,
}

impl<T: Default + Clone> ringbuf::RingBufContent for DataPoint<T> {
	fn new() -> Self {
		Self {
			ts: 0,
			value: Default::default(),
			state: DataPointState::DEFAULT,
		}
	}
}

impl Table {
	fn empty_init(keys: Vec<&str>) -> Self {
		let mut states = HashMap::new();
		let mut records = Vec::<ringbuf::RingBuf<DataPoint<String>>>::new();
		let states_per_key = CONFIG.read().unwrap().max_state_records;
		keys.iter().enumerate().for_each(|(i, &k)|  {
			for j in states_per_key {
				// For each key, we have states_per_key rows. each contains a series of time stamp.
				states.insert(format!("{}_{}", key, j), i * states_per_key + j);
				records.push(RingBuf::new(CONFIG.read().unwrap().ringbuffer_size as usize, Some(CONFIG.read().unwrap().ringbuffer_full_to_panic)));
			}
		});
		Table{
			states: states,
			records: records,
		}
	}
	fn reset_version(&self, key: &str, ts: u64){
	// This only called on obj with normal states.
		debug_assert!(self.states.contains_key(key));
		let obj = self.records[self.states[key]]
			.object_as_ordered(Box::new(move |dp: &DataPoint<String>| dp.ts.cmp(&ts)));
		// This only called on obj to be aborted. Should have been written NORMAL result.
		debug_assert!({
			obj.is_some() && obj.as_ref().unwrap().state == DataPointState::NORMAL
		});
		obj.unwrap().state = DataPointState::SHOULDBEEMPTY;
	}

	fn copy_last_version(&self, key: &str, ts: u64){
	// This only called on obj to be aborted. Should have been written NORMAL result.
		debug_assert!(self.states.contains_key(key));
		let obj = self.records[self.states[key]]
			.object_as_ordered(Box::new(move |dp: &DataPoint<String>| dp.ts.cmp(&ts)));
		debug_assert!({
			obj.is_some() && obj.as_ref().unwrap().state == DataPointState::NORMAL
		});
		obj.unwrap().state = DataPointState::ABORTED;
	}

	// Stage version inserts the version at the end. The t is guaranteed to be the last version.
	fn push_version(&self, key: &str, ts: u64, value: &str) {
		// Insert dataPoint into vectors. Keep correct order.
		debug_assert!(self.states.contains_key(key));
		debug_assert!({
			let obj = self.records[self.states[key]]
				.object_as_ordered(Box::new(move |dp: &DataPoint<String>| dp.ts.cmp(&ts)));
			obj.is_none() || obj.unwrap().state != DataPointState::ABORTED
		});
		debug_assert!({
			let pos = self.records[self.states[key]].position_as_ordered(Box::new(move |dp| dp.ts.cmp(&ts)));
			match pos{
				Some(i) => i == self.records[self.states[key]].len()-1,
				None => false,
			}
		});
		// Create a new DataPoint with the provided timestamp and value.
		let new_data_point = DataPoint {
			ts: ts,
			value: value.to_string(),
			state: DataPointState::NORMAL,
		};
		self.records[self.states[key]].push(new_data_point);
	}

	// Stage version inserts the version at in the middle. We find it first.
	fn write_version(&self, key: &str, ts: u64, value: &str) {
		// Insert dataPoint into vectors. Keep correct order.
		debug_assert!(self.states.contains_key(key));
		let obj_ref = self.records[self.states[key]]
			.ref_as_ordered(Box::new(move |dp: &DataPoint<String>| dp.ts.cmp(&ts)));
		debug_assert!({
			obj_ref.unwrap().1
				.read().unwrap()
				.state == DataPointState::SHOULDBEEMPTY
		});
		// Create a new DataPoint with the provided timestamp and value.
		let new_data_point = DataPoint {
			ts: ts,
			value: value.to_string(),
			state: DataPointState::NORMAL,
		};
		obj_ref.unwrap().1.write().unwrap().state = DataPointState::NORMAL;
		obj_ref.unwrap().1.write().unwrap().value = String::from(value);
		// TODO: Search back.
	}

	fn release_version(&self, key: &str, ts: u64){
		// Remove datapoint from ringbuf.
		debug_assert!(self.states.contains_key(key));
		debug_assert!(self.records[self.states[key]].peek(0).unwrap().ts == ts); // Should be the very first of the key.
		self.records[self.states[key]].discard_before(1);
	}

	fn get_version(&self, key: &str, ts: u64) -> String{
		// Get datapoint from ringbuf.
		debug_assert!(self.states.contains_key(key));
		let obj = self.records[self.states[key]]
			.object_as_ordered(Box::new(move |dp: &DataPoint<String>| dp.ts.cmp(&ts)))
			.unwrap(); // Shoud not be none.
		debug_assert!(obj.state == DataPointState::NORMAL);
		obj.value.clone()
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