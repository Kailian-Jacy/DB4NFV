pub trait Database {
	// Init.
	fn new() -> Self;
	fn add_table(&mut self, to_add_table: &str, keys: Vec<&str>);

	// Writing.
	fn reset_version(&self, table: &str, key: &str, ts: u64); // Debug api. Could just remove.
	fn write_version(&self, table: &str, key: &str, ts: u64, value: &Vec<u8>); // At certain version.
	fn push_version(&self, table: &str, key: &str, ts: u64, value: &Vec<u8>); // Be sure to insert at certain result.
	fn copy_last_version(&self, table: &str, key: &str, ts: u64, has_slot: bool);
	fn release_version(&self, table: &str, key: &str, ts: u64);

	// Reading.
	fn get_version(&self, table: &str, key: &str, ts: u64) -> Vec<u8>;
}