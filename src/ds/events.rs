use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Event{
	pub reads: Vec<String>,
	pub write: String,
	pub has_write: bool,

	#[serde(skip)]
	pub sa_idx: usize,
}
