use crossbeam_deque::Steal;
use crossbeam_utils::atomic::AtomicCell;
use crossbeam_utils::Backoff;
use log::info;
use std::cell::UnsafeCell;
use std::env;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

const BUFFER_SIZE: usize = 65536;

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
    version: AtomicUsize,
}

pub(crate) struct LifoArrayInjector<T> {
    max_size: usize,
    ring_buff: Vec<Slot<T>>,
    cursor: AtomicUsize,
    rest_version: AtomicUsize,
}
unsafe impl<T> Sync for LifoArrayInjector<T> {}
unsafe impl<T> Send for LifoArrayInjector<T> {}

impl<T> LifoArrayInjector<T> {
    pub(crate) fn new() -> Self {
        let buffer_size = env::var("RAYON_INJECTOR_MAX_SIZE")
            .map(|x| x.parse::<usize>().ok())
            .ok()
            .flatten()
            .unwrap_or(BUFFER_SIZE);
        info!("ArrayInjector max queue size: {}", buffer_size);
        let mut map = Vec::with_capacity(buffer_size);
        for _ in 0..BUFFER_SIZE {
            let slot = Slot {
                value: UnsafeCell::new(MaybeUninit::uninit()),
                state: AtomicUsize::new(0),
                version: AtomicUsize::new(0),
            };
            map.push(slot);
        }
        Self {
            max_size: buffer_size,
            ring_buff: map,
            cursor: AtomicUsize::new(0),
            rest_version: AtomicUsize::new(0),
        }
    }

    pub(crate) fn push(&self, task: T) {
        let backoff = Backoff::new();
        loop {
            let idx = self.cursor.load(Ordering::Acquire);
            if idx + 1 >= self.max_size {
                // 如果达到队尾部指针归零丢弃所有任务
                if let Ok(_) =
                    self.cursor
                        .compare_exchange(idx, 0, Ordering::SeqCst, Ordering::Acquire)
                {
                    // advance reset version
                    self.rest_version.fetch_add(1, Ordering::AcqRel);
                }
                backoff.snooze();
                continue;
            }
            match self.cursor.compare_exchange_weak(
                idx,
                idx + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(old) => {
                    let slot = unsafe { self.ring_buff.get_unchecked(old + 1) };
                    let mut state;
                    loop {
                        state = slot.state.load(Ordering::Acquire);
                        if state % 2 == 0 {
                            // slot状态可写跳出循环
                            break;
                        }
                        // 不可写
                        let reset_version = self.rest_version.load(Ordering::Acquire);
                        if slot.version.fetch_max(reset_version, Ordering::AcqRel) != reset_version
                        {
                            // 重置当前slot状态为可写
                            let _ = slot.state.fetch_update(
                                Ordering::Release,
                                Ordering::Acquire,
                                |state| {
                                    if state % 2 != 0 {
                                        // unread -> writeable
                                        Some(state + 1)
                                    } else {
                                        // unchange
                                        Some(state)
                                    }
                                },
                            );
                        } else {
                            break;
                        }
                    }
                    // 当前slot可写
                    match slot.state.compare_exchange_weak(
                        state,
                        state + 1,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // writeable
                            unsafe {
                                slot.value.get().write(MaybeUninit::new(task));
                            }
                            // push success
                            break;
                        }
                        Err(_) => {
                            backoff.snooze();
                            continue;
                        }
                    }
                }
                Err(_) => {
                    // retry
                    backoff.snooze();
                }
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cursor.load(Ordering::Acquire) <= 0
    }

    pub(crate) fn steal(&self) -> Steal<T> {
        let idx = self.cursor.load(Ordering::Acquire);
        if idx <= 0 {
            return Steal::Empty;
        }
        match self
            .cursor
            .compare_exchange_weak(idx, idx - 1, Ordering::SeqCst, Ordering::Acquire)
        {
            Ok(idx) => {
                let slot = unsafe {
                    self.ring_buff.get_unchecked(idx)
                };
                let mut state;
                let backoff = Backoff::new();
                loop {
                    state = slot.state.load(Ordering::Acquire);
                    if state % 2 != 0 {
                        // 可读
                        match slot.state.compare_exchange_weak(
                            state,
                            state + 1,
                            Ordering::SeqCst,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                // read
                                let task = unsafe {
                                    slot.value.get().read().assume_init()
                                };
                                return Steal::Success(task);
                            }
                            Err(_) => {
                                backoff.snooze();
                                continue
                            },
                        }
                    } else {
                        break;
                    }
                }
                Steal::Retry
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
    use crate::injector::LifoArrayInjector;
    use crossbeam_deque::Steal;

    #[test]
    fn test() {
        let inject: LifoArrayInjector<usize> = LifoArrayInjector::new();
        for i in 0..100 {
            inject.push(i);
        }
        assert_eq!(inject.is_empty(), false);
        assert_eq!(inject.steal(), Steal::Success(99));
    }
}
