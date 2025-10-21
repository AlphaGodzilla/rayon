use crossbeam_deque::Steal;
use crossbeam_utils::Backoff;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BUFFER_SIZE: usize = 65536;

pub(crate) struct ArrayInjector<T> {
    map: Vec<AtomicPtr<MaybeUninit<T>>>,
    index: AtomicUsize,
}
unsafe impl<T> Sync for ArrayInjector<T> {}
unsafe impl<T> Send for ArrayInjector<T> {}

impl<T> ArrayInjector<T> {
    pub(crate) fn new() -> Self {
        let mut map = Vec::with_capacity(BUFFER_SIZE);
        for _ in 0..BUFFER_SIZE {
            map.push(AtomicPtr::default());
        }
        Self {
            map,
            index: AtomicUsize::new(0),
        }
    }

    pub(crate) fn push(&self, task: T) -> bool {
        let backoff = Backoff::new();
        loop {
            let idx = self.index.load(Ordering::Acquire);
            if idx + 1 >= BUFFER_SIZE {
                println!("ArrayInjector full reject {}", BUFFER_SIZE);
                return false;
            }
            match self
                .index
                .compare_exchange(idx, idx + 1, Ordering::SeqCst, Ordering::Acquire)
            {
                Ok(old) => unsafe {
                    let slot = self.map.get_unchecked(old + 1);
                    let slot_ptr = slot.load(Ordering::Acquire);
                    if slot_ptr.is_null() {
                        let task = Box::into_raw(Box::new(MaybeUninit::new(task)));
                        slot.store(task, Ordering::Release);
                    } else {
                        slot_ptr.write(MaybeUninit::new(task));
                    }
                    return true;
                },
                Err(_) => {
                    // retry
                    backoff.snooze();
                }
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.index.load(Ordering::Acquire) <= 0
    }

    pub(crate) fn steal(&self) -> Steal<T> {
        let idx = self.index.load(Ordering::Acquire);
        if idx <= 0 {
            return Steal::Empty;
        }
        match self
            .index
            .compare_exchange(idx, idx - 1, Ordering::SeqCst, Ordering::Acquire)
        {
            Ok(old) => unsafe {
                let slot = self.map.get_unchecked(old);
                let slot = slot.load(Ordering::Acquire);
                let task = slot.read().assume_init();
                Steal::Success(task)
            },
            Err(old) => {
                if old <= 0 {
                    return Steal::Empty;
                }
                Steal::Retry
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::injector::ArrayInjector;
    use crossbeam_deque::Steal;

    #[test]
    fn test() {
        let inject: ArrayInjector<usize> = ArrayInjector::new();
        inject.push(1);
        inject.push(2);
        inject.push(3);
        assert_eq!(inject.is_empty(), false);
        assert_eq!(inject.steal(), Steal::Success(3));
    }
}
