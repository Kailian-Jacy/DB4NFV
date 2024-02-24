use std::collections::HashMap;

use crate::tpg::tpg;
use crate::utils::*;
use crate::ds::events as ev;
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct Txn{
	/*
		Fields passed from FFI __init_sfc. 
	 */
	pub es: Vec<ev::Event>,
	// To be added.

	pub all_reads_length: usize,
}

pub static TXN_TEMPLATES: Vec<Txn> = Vec::<Txn>::new();

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
        self.es.iter().enumerate().map(|(idx,event)| {
			// To make close if cancel write dependency.
			event.reads.push(event.write);
			event.reads.sort();
			event.reads.dedup();
			event.sa_idx = idx;

            // Add reads to all_reads
            all_reads.extend(event.reads.iter().cloned());
            // Record write field in all_writes
            all_writes.insert(event.write.clone(), event.clone());
            // Check for duplicate writes
            if let Some(existing_event) = all_writes.get(&event.write) {
                panic!("Duplicate writes detected for key: {}", event.write);
            }
		});

        all_reads.dedup();

        // Set all_reads_length
        self.all_reads_length = all_reads.len();
    }
}
