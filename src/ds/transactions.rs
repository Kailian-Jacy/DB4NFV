use std::collections::HashMap;

use crate::ds::events as ev;
use once_cell::sync::OnceCell;
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct Txn{
	/*
		Fields passed from FFI __init_sfc. 
	 */
	pub es: Vec<ev::Event>,
	// To be added.

    pub all_reads_index_map: HashMap<String, usize>,
	pub all_reads_length: usize,
}

pub static TXN_TEMPLATES: OnceCell::<Vec<Txn>> = OnceCell::new();

impl Txn {
	// Deserialization function to parse the string into a vector of Txn
    pub fn from_string(input: &str) -> Result<Vec<Self>, serde_json::Error> {
        serde_json::from_str(input)
    }

	// Function to process each Txn and calculate all_reads_length, populate all_writes, and check for duplicate writes
    pub fn process_txn(&mut self) {
        // Initialize variables
        let mut all_reads = Vec::new();
        let mut all_writes = HashMap::new();

        // Iterate through each event in the transaction
        self.es.iter_mut().enumerate().for_each(|(idx,event)| {
			// To make close if cancel write dependency.
			event.reads.push(event.write.clone());
			event.reads.sort();
			event.reads.dedup();
			event.sa_idx = idx;

            // Add reads to all_reads
            all_reads.extend(event.reads.iter().cloned());
            // Record write field in all_writes
            all_writes.insert(event.write.clone(), ());
            // Check for duplicate writes
            // if all_writes.get(&event.write).is_some() {
            //     panic!("Duplicate writes detected for key: {}", event.write);
            // }
            // Write check is delayed to runtime.
		});

        all_reads.dedup();

        // Set all_reads_length
        self.all_reads_length = all_reads.len();
        self.all_reads_index_map = all_reads.iter()
            .enumerate().map(|(idx, s)| (s.clone(), idx)).collect();
    }
}
