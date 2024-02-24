use std::cell;
use std::collections::HashMap;
use std::sync::Arc;
use crate::config::CONFIG;
use crate::database::api;
use crate::tpg::tpg;
use crate::ds::ringbuf::{self, RingBuf};

// Multi-version states engine
pub struct SimpleDB {
	tables: HashMap<String, Table>,
}

impl api::Database for SimpleDB {
	fn new() -> Self {
		SimpleDB { tables: HashMap::new() }
	}

	fn add_table(&self, to_add_table: &str, keys: Vec<&str>) {
		self.tables.insert(String::from(to_add_table), Table::empty_init(keys));
	}

	/* Stage management.
		Commit_version commits the certain version of certain variables and discard records. Called when txn Commit.
		Stage_version adds temporary records for certain variables. Called when operation executes.
		Revert_to_version discards the later records of some variables.  Called when ABORTION happens.
	*/ 
	fn commit_version(&self, table: &str, key: &str, t: u64) {
		debug_assert!(self.tables.contains_key(table));
		self.tables
			.get_mut(table)
			.unwrap()
			.commit_version(key, t);
	}
	fn stage_version(&self, table: &str, key: &str, t: u64, value: &str, modifier: &Arc<tpg::EvNode>) {
		debug_assert!(self.tables.contains_key(table));
		self.tables
			.get_mut(table)
			.unwrap()
			.stage_version(key, t, value, modifier);
	}
	fn revert_to_version(&self, table: &str, key: &str, t:u64) {
		debug_assert!(self.tables.contains_key(table));
		self.tables
			.get_mut(table)
			.unwrap()
			.revert_to_version(key, t);
	}

	/* State fetch.
		Get fetches any version of the variable. Marked by time stamp. Panic if not exists.
		Latest fetches the latest state and its time stamp.
		Solid fetches the commited state of some variable and its time stamp.
	*/
	// fn get(&self, table: &str, key: &str, t:u64) -> Option<&str> {
	// 	debug_assert!(self.tables.contains_key(table), "Table not found.");
	// 	self.tables[table].records[*self.tables[table].states.get(key)?]
	// 		.object_as_ordered(|dp| dp.ts.cmp(&t))
	// 		.map(|dp| dp.value.as_str())
	// }
	fn latest(&self, table: &str, key: &str) -> Option<api::StateWithTS> {
		debug_assert!(self.tables.contains_key(table), "Table not found.");
		let tbl = &self.tables[table];
		let rec = &tbl.records[tbl.states[key]];
		let dp = rec.peek(rec.len() - 1)?.clone();
		Some(api::StateWithTS{
			value: dp.value.as_str(),
			ts: dp.ts,	
		})
	}
	fn solid(&self, table: &str, key: &str) -> Option<api::StateWithTS> {
		debug_assert!(self.tables.contains_key(table), "Table not found.");
		let tbl = self.tables[table];
		let rec = tbl.records[tbl.states[key]];
		let dp = rec.peek(0)?;
		Some(api::StateWithTS{
			value: dp.value.as_str(),
			ts: dp.ts,	
		})
	}
	// fn dependency_list(&self, table: &str, read_state: Vec<&str>) -> Vec<Arc<tpg::EvNode>>{
	// 	debug_assert!(self.tables.contains_key(table), "Table not found.");
	// 	let tbl = self.tables[table];
	// 	read_state.iter().map(|& k| {
	// 		tbl.records[tbl.states[k]]
	// 			.peek(tbl.records[tbl.states[k]].len() - 1)
	// 			.unwrap()
	// 			.modifier.clone().unwrap()
	// 	}).collect()
	// }
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
	// modifier: Option<Arc<tpg::EvNode>>,
}

impl<T: Default + Clone> ringbuf::RingBufContent for DataPoint<T> {
	fn new() -> Self {
		Self {
			ts: 0,
			value: Default::default(),
			// modifier: None,
		}
	}
}

// impl<T: Default> PartialEq for DataPoint<T> {
//     fn eq(&self, other: &Self) -> bool {
//         self.ts == other.ts
//     }
// }
// impl<T: Default> Eq for DataPoint<T> {}
// impl<T: Default> PartialOrd for DataPoint<T> {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
// impl<T: Default> Ord for DataPoint<T> {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.ts.cmp(&other.ts)
//     }
// }

impl Table {
	fn empty_init(keys: Vec<&str>) -> Self {
		let mut states = HashMap::new();
		let mut records = Vec::<ringbuf::RingBuf<DataPoint<String>>>::new();
		keys.iter().enumerate().for_each(|(idx, &k)| {
		    states[&String::from(k)] = idx;
			records.push(RingBuf::new(CONFIG.read().unwrap().ringbuffer_size as usize, Some(CONFIG.read().unwrap().ringbuffer_full_to_panic)));
		});
		Table{
			states: states,
			records: records,
		}
	}
	fn commit_version(&mut self, key: &str, t: u64) {
        // Assert that the key exists in the records HashMap
        debug_assert!(self.states.contains_key(key), "Key not found in records.");
        let index = self.records[self.states[key]]
			.position_as_ordered(Box::new(|dp: &DataPoint<String>| {dp.ts.cmp(&t)}))
			.unwrap_or_else(|| panic!("No DataPoint with timestamp {} found for key {}", t, key));
        self.records[self.states[key]].discard_before(index);
    }
	// Stage version inserts the version at the end. The t is guaranteed to be the last version.
	fn stage_version(&mut self, key: &str, t: u64, value: &str, modifier: &Arc<tpg::EvNode> ) {
		// Insert dataPoint into vectors. Keep correct order.
		debug_assert!(self.states.contains_key(key));
		// Check if the last one.
		debug_assert!({
			let pos = self.records[self.states[key]].position_as_ordered(Box::new(|dp| dp.ts.cmp(&t)));
			match pos{
				Some(i) => i == self.records[self.states[key]].len()-1,
				None => false,
			}
		});
		// Create a new DataPoint with the provided timestamp and value.
		let new_data_point = DataPoint {
			ts: t,
			value: value.to_string(),
			// modifier: Some(modifier.clone()),
		};
		self.records[self.states[key]].push(new_data_point);
	}
	fn revert_to_version(&mut self, key: &str, t:u64) {
		 // Assert that the key exists in the records HashMap
        assert!(self.states.contains_key(key), "Key not found in records.");
        // Find the index of the DataPoint with the provided timestamp
        let index = self.records[self.states[key]]
			.position_as_ordered(Box::new(|dp| dp.ts.cmp(&t)))
			.unwrap_or_else(|| panic!("No DataPoint with timestamp {} found for key {}", t, key));
        // Conserve the vector until the index
        self.records[self.states[key]].truncate_from(index + 1);
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