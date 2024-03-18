use std::{sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard}, time::{SystemTime, UNIX_EPOCH}};
use lazy_static::lazy_static;

use core_affinity::CoreId;


// Some data structure in our system guarantees thread safety by itself. Mark it to be "Will be no conflict" and detect bug.
// IT WOULD PANIC if any waiting happens.
#[derive(Debug)]
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

lazy_static! {
    static ref AVAILABLE_CPUS: Vec<CoreId> = {
        core_affinity::get_core_ids()
            .expect("Failed to get core IDs")
    };
}

pub fn bind_to_cpu_core(tid: usize){
    let to_bind = AVAILABLE_CPUS[tid];
    let res = core_affinity::set_for_current(to_bind);
    if !res {
        panic!("Bingding failed.")
    }
}

pub fn report_cpu_core(name: &str){
    // Retrieve the current thread id
    let core_id = affinity::get_thread_affinity().unwrap();
    println!("Current thread {} is binded to CPU core {:?}", name, core_id);
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
