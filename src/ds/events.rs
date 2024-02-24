use serde::Deserialize;

#[derive(Deserialize)]
pub struct Event{
	pub reads: Vec<String>,
	pub write: String,
	pub has_write: bool,
	pub sa_idx: usize,
}
