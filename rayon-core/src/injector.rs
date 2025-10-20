use crossbeam_deque::Steal;
use crossbeam_utils::atomic::AtomicCell;
use std::sync::atomic::{AtomicUsize, Ordering};

const ARRAY_SIZE: usize = 65536;

pub(crate) struct ArrayInjector {
    map: Box<[AtomicUsize; ARRAY_SIZE]>,
    index: AtomicCell<(usize, usize)>,
}
unsafe impl Sync for ArrayInjector {}
unsafe impl Send for ArrayInjector {}

impl ArrayInjector {
    pub(crate) fn new() -> Self {
        let queue = std::array::from_fn(|_| AtomicUsize::new(0));
        Self {
            map: Box::new(queue),
            index: AtomicCell::new((0, 0)),
        }
    }

    pub(crate) fn push<T>(&self, task: T) -> bool {
        match self.index.fetch_update(|(adder, idx)| {
            if idx + 1 >= ARRAY_SIZE {
                None
            } else {
                Some((adder + 1, idx + 1))
            }
        }) {
            Ok((_, prev_idx)) => {
                // 写入数据
                unsafe {
                    let slot = self.map.get_unchecked(prev_idx + 1);
                    let job_ref_ptr = Box::into_raw(Box::new(task)) as usize;
                    // println!("write idx: {}", prev_idx + 1);
                    slot.store(job_ref_ptr, Ordering::Release);
                    // println!("write ptr: {}", job_ref_ptr);
                }
                true
            }
            Err(_) => {
                println!("ArrayInjector cap not enough {}", ARRAY_SIZE);
                false
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.index.load().1 == 0
    }

    pub(crate) fn steal<T>(&self) -> Steal<Box<T>> {
        match self.index.fetch_update(|(adder, idx)| {
            if idx <= 0 {
                None
            } else {
                Some((adder + 1, idx - 1))
            }
        }) {
            Ok((_, prev_idx)) => unsafe {
                // println!("read idx: {}", prev_idx);
                let job_ref_ptr = self.map.get_unchecked(prev_idx);
                let job_ref_ptr = job_ref_ptr.load(Ordering::Acquire);
                // println!("read ptr: {}", job_ref_ptr);
                let job = Box::from_raw(job_ref_ptr as *mut T);
                Steal::Success(job)
            },
            Err((_, _)) => Steal::Empty,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::injector::ArrayInjector;
    use crossbeam_deque::Steal;

    #[test]
    fn test() {
        let inject: ArrayInjector = ArrayInjector::new();
        inject.push(1);
        inject.push(2);
        inject.push(3);
        assert_eq!(inject.is_empty(), false);
        assert_eq!(inject.steal(), Steal::Success(Box::new(3)));
    }
}
