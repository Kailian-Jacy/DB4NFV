use std::fmt::Debug;
use std::ops::Index;
use std::sync::RwLock;
use std::mem;
use std::cmp::Ordering;
use crossbeam::atomic::AtomicCell;

/*
	Design of RingBuf:
	Target:
	- Thread safe when visiting different cell.
	- High performance from cache alignment and lockless.
 */
pub struct RingBuf<T>
	where T: RingBufContent + Debug
{
	cap: usize,
	start: AtomicCell<usize>,
	end: AtomicCell<usize>, // Point to the first place that is empty.
	// Now we are using RwLock. Since Refcell with sync is RwLock.
	// TODO. We consider to replace it with unsafe code. Copy it out, assign to the follower, and then set without Lock.
	buf: Vec<RwLock<T>>,
	full2panic: bool,
}

// Ring buf content requires to have Clone and default. 
/*
	Clone: when some cell records aborted content, it's marked as the same content as last version record.
	Default: when ringbuf created, the very start content of each cell.
 */
pub trait RingBufContent: Clone + Default {}

impl<T> RingBuf<T>
	where T: RingBufContent + Debug
{
	#[inline]
	fn end(&self) -> usize {
		self.end.load()
	}
	#[inline]
	fn start(&self) -> usize {
		self.start.load()
	}
	// Total memory usage.
	#[inline]
	pub fn size(&self) -> usize {
		self.cap * mem::size_of::<T>()
	}
	pub fn len(&self) -> usize {
		if self.end() >= self.start() {
			(self.end() - self.start()) as usize
		} else {
			(self.cap - self.start() + self.end()) as usize
		}
	}
	#[inline]
	pub fn cap(&self) -> usize	{
		self.cap
	}
	// Won't panic if full2panic is not true
    pub fn new(cap: usize, full2panic: Option<bool>) -> Self {
		assert!(cap > 0);
		let mut v = Vec::with_capacity(cap);
		// TODO: Possibly with Vec.from_raw_parts.
		for _ in 0..cap {
			v.push(RwLock::new(T::default()));
		}
        Self {
            cap: cap,
            start: AtomicCell::new(0),
            end: AtomicCell::new(0),
            buf: v,
			full2panic: full2panic.unwrap_or(false),
        }
    }
	pub fn push(&self, item: T) {
		// Push in tail. Designed to be pushed in increasing order.
		// debug_assert!({
		// 	if self.end() != (self.start() + 1) % self.cap {
		// 		if self.end() == 0 {
		// 			item.cmp(&self.buf[self.cap - 1]) == 
		// 				Ordering::Greater
		// 		} else {
		// 			item.cmp(&self.buf[(self.end() - 1) % self.cap]) == 
		// 				Ordering::Greater
		// 		}
		// 	} else {
		// 		true
		// 	}
		// });
		*self.buf[self.end() as usize].write().unwrap() = item;
		self.end.store((self.end() + 1) % self.cap as usize);

		// End should be smaller than start. When end = start, that means all full or all empty.
		if self.end() == self.start() {
			if self.full2panic {
				panic!("ring buf full.")
			} else {
				println!("[CRITICAL] ring buf full.")
			}
		}
	}

	// Pop last.
	pub fn pop(self) -> Option<T> {
	    if self.start() == self.end() {
			None
	    } else {
			let r = Some(self.buf[(self.end() - 1) % self.cap as usize].read().unwrap().clone());
			if self.end() == 0 {
				self.end.swap(self.cap - 1);
			}  else {
				self.end.fetch_sub(1);
			}
		    r
	    }
	}

	// We tent not to provide index api. Since the starting point is drifting. 
	// We suggest using timestamp to search.
	// pub fn index(&self, idx: usize) -> Option<&RwLock<T>> {
	//     // if self.end() - self.start() - 1 < idx {
	//     if ( self.start() + idx ) % self.cap >= self.end() {
	// 		None
	//     } else {
	// 		Some(self.buf.index((self.start() + idx) % self.cap))
	//     }
	// }

	// Clone to peek the head
	pub fn first_clone(&self) -> Option<T> {
		if self.start() == self.end() {
			None
		} else {
			Some(self.buf[self.start()].read().unwrap().clone())
		}
	}

	// Clone to peek the tail
	pub fn last_clone(&self) -> Option<T> {
		if self.start() == self.end() {
			None
		} else {
			Some(self.buf[self.end() - 1].read().unwrap().clone())
		}
	}

	// Dump used for debugging. Print content for checking;
	pub fn dump(&self){
		println!("ringbuf.start {}; ringbuf.end {}.", self.start(), self.end());
		for ele in (self.start()..self.end()).into_iter() {
			println!("ringbuf content {}: {:?}", ele, self.buf[ele].read().unwrap());
		}
	}
	// Search back. Used when dating back to last valid version of state.
	pub fn search_back(&self, f: Box<dyn Fn(&T) -> bool>, from_idx: usize) -> Option<&RwLock<T>> {
		let mut idx = (self.start() + from_idx) % self.cap;
		loop {
			if f(self.buf[idx].read().as_ref().unwrap()) {
				// Found.
				return Some(self.buf.index(idx));
			} else {
				// Not found
				if idx == self.start() { return None }
				if idx == 0 { idx = self.cap - 1 }
					else { idx = idx - 1 }
			}
		}
	}
	// Truncate from the tail.
	pub fn truncate_from(&self, index: usize) {
		debug_assert!(index < self.len() as usize);
		self.end.store((self.start() + index) % self.cap);
	}
	// Truncate from the head.
	pub fn discard_before(&self, index: usize) {
		debug_assert!(index < self.len() as usize);
		self.start.store((self.start() + index) % self.cap);
	}
	/*
		The user guarantee the ringbuffer content is increasingly ordered. So as to improve the searching efficiency.
	 */
	// Search in the buffer and return the value.
	#[inline]
	pub fn position_as_ordered(&self, f: Box<dyn Fn(&T) -> std::cmp::Ordering>) -> Option<usize> 
	{
		Some(self.ref_as_ordered(f)?.0)
	}
	#[inline]
	pub fn object_as_ordered(&self, f: Box<dyn Fn(&T) -> std::cmp::Ordering>) -> Option<T> 
	{
		Some(self.ref_as_ordered(f)?.1.read().unwrap().clone())
	}
	// Use linear searching to debug. No evidance show binary search error.
	// pub fn ref_as_ordered(&self, f: Box<dyn Fn(&T) -> std::cmp::Ordering>) -> Option<(usize, &RwLock<T>)> {
	// 	if self.start() < self.end() {
    //         self.binary_search(self.start(), self.len(),  &f)
    //     } else if self.start() > self.end() {
    //         let (first_half, _) = self.buf.split_at(self.end());
    //         let first_half_len = first_half.len();
    //         let found = self.binary_search(0, first_half_len, &f);
    //         if found.is_some() {
    //             found
    //         } else {
	// 			let (_, second_half) = self.buf.split_at(self.end());
	// 			let second_half_len = second_half.len();
    //             self.binary_search(self.start(), second_half_len, &f)
    //         }
    //     } else {
	// 		panic!("bug.")
	// 	}
	// }
	pub fn ref_as_ordered(&self, f: Box<dyn Fn(&T) -> std::cmp::Ordering>) -> Option<(usize, &RwLock<T>)> {
		let (start, len) = (self.start(), self.len());
		for idx in 0..len {
			if f(self.buf[start + idx].read().as_ref().unwrap()) == Ordering::Equal {
				return Some((idx, &self.buf[start + idx]))
			}
		}
		None
	}
	// fn binary_search(&self, start_idx: usize, len: usize, f: &Box<dyn Fn(&T) -> std::cmp::Ordering>) -> Option<(usize, &RwLock<T>)> {
    //     let mut left = 0;
    //     let mut right = len - 1;

    //     while left <= right {
    //         let mid = left + (right - left) / 2;
    //         let idx = (start_idx + mid) % self.cap;
    //         let cmp = f(&self.buf[idx].read().unwrap());

    //         match cmp {
    //             Ordering::Equal => return Some((idx, &self.buf[idx])),
    //             Ordering::Greater => right = mid - 1,
    //             Ordering::Less => left = mid + 1,
    //         }
    //     }
    //     None
    // }
}


#[cfg(test)] 
mod test {
use super::*;

impl RingBufContent for i32 {}

// #[test]
// fn test_ring_buffer() {
//     // Create a new ring buffer with a capacity of 5 and panic on full disabled
//     let ring_buffer = RingBuf::new(5, None);

//     // Convert the ring buffer into an Arc to share across threads
//     let shared_ring_buffer = Arc::new(ring_buffer);

//     // Spawn a thread to push items into the ring buffer
//     let push_thread = {
//         let shared_ring_buffer = Arc::clone(&shared_ring_buffer);
//         thread::spawn(move || {
//             for i in 1..=10 {
//                 // Push items into the ring buffer
//                 shared_ring_buffer.push(i);
//                 println!("Pushed item: {}", i);
//                 thread::sleep(Duration::from_secs(1));
//             }
//         })
//     };

//     // Spawn a thread to pop items from the ring buffer
//     let pop_thread = {
//         let shared_ring_buffer = Arc::clone(&shared_ring_buffer);
//         thread::spawn(move || {
//             for _ in 1..=5 {
//                 // Pop items from the ring buffer
//                 if let Some(item) = shared_ring_buffer.pop() {
//                     println!("Popped item: {}", item);
//                 }
//                 thread::sleep(Duration::from_secs(2));
//             }
//         })
//     };

//     // Join the threads to wait for them to finish
//     push_thread.join().unwrap();
//     pop_thread.join().unwrap();
// }
}