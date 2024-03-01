use crate::config::CONFIG;

use std::sync::mpsc::*;
use once_cell::sync::OnceCell;

use super::ffi;

// TODO: How to expose a lazy inited object?
pub(crate) static PIPE_IN: OnceCell<SyncSender<ffi::TxnMessage>> = OnceCell::new();

pub fn init() -> Receiver<ffi::TxnMessage> {
	// TODO: Verify really blocks when the channel is full?
	let (tx, rx) = sync_channel(CONFIG.read().unwrap().waiting_queue_size as usize);
	PIPE_IN.set(tx.clone()).expect("Failed to set pipe.");
	rx
}
