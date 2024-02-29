use std::{sync::{RwLock, RwLockReadGuard, RwLockWriteGuard}, time::{SystemTime, UNIX_EPOCH}};

use core_affinity::CoreId;

// Some data structure in our system guarantees thread safety by itself. Mark it to be "Will be no conflict" and detect bug.
// IT WOULD PANIC if any waiting happens.
pub(crate) struct ShouldSyncCell<T>{
    body: RwLock<T>
}

unsafe impl<T> Send for ShouldSyncCell<T> {}
unsafe impl<T> Sync for ShouldSyncCell<T> {}

impl<T> ShouldSyncCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            body: RwLock::new(value)
        }
    }
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.body.try_read().unwrap()
    }
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.body.try_write().unwrap()
    }
}

// Function to get current time in nanoseconds
pub fn current_time_ns() -> u64{
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let nanoseconds = since_the_epoch.as_nanos() as u64;
    nanoseconds
}

// Function to convert nanoseconds to SystemTime
pub fn ns_to_system_time(nanoseconds: u64) -> SystemTime {
    let duration = std::time::Duration::from_nanos(nanoseconds);
    UNIX_EPOCH + duration
}

pub fn bind_to_cpu_core(c: usize){
    let core_ids = core_affinity::get_core_ids().unwrap();
    assert!(c < core_ids.len());
    let res = core_affinity::set_for_current(
        core_ids[c]
    );
    if !res {
        panic!("Bingding failed.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_time_conversion() {
        // Get current time in nanoseconds
        let current_time = current_time_ns();
        println!("Current time in nanoseconds: {}", current_time);

        // Convert nanoseconds back to SystemTime
        let system_time = ns_to_system_time(current_time);
        println!("System time: {:?}", system_time);
        
        // Assert that the system time is after the UNIX epoch
        assert!(system_time > UNIX_EPOCH);
    }
}
