use std::sync::Arc;
use crate::tpg::tpg;

// Trait database defines multi-version database interface.
/*
	A multi-version database.
 */
pub trait Database {
	fn new() -> Self;

	// Initialization with table names.
	fn add_table(&self, to_add_table: &str, keys: Vec<&str>);

	/* Stage management.
		Stage_version adds temporary records for certain variables. Called when operation(event) executes.
		Commit_version commits the certain version of certain variables and discard records. Called when txn Commit.
		Revert_to_version discards the later records of some variables.  Called when ABORTION happens.
	*/ 
	fn stage_version(&self, table: &str, key: &str, t: u64, value: &str, modifier: &Arc<tpg::EvNode>);
	fn commit_version(&self, table: &str, key: &str, t: u64);
	fn revert_to_version(&self, table: &str, key: &str, t:u64);

	/* State fetch.
		// Get fetches any version of the variable. Marked by time stamp. Panic if not exists.
		Latest fetches the latest state and its time stamp.
		Solid fetches the commited state of some variable and its time stamp.
	*/
	// fn get(&self, table: &str, key: &str, t:u64) -> Option<&str>;
	fn latest(&self, table: &str, key: &str) -> Option<StateWithTS>;
	fn solid(&self, table: &str, key: &str) -> Option<StateWithTS>;

	/* Dependency provider.
		Dependency_list 
	 */
	// fn dependency_list(&self, table: &str, read_state: Vec<&str>) -> Vec<Arc<tpg::EvNode>>;
}

pub struct StateWithTS<'a> {
	pub value: &'a str,
	pub ts: u64,
}
