use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use rayon_core::ThreadPoolBuilder;

static ACTIVE_TASKS: AtomicUsize = AtomicUsize::new(0);

#[test]
fn activity_test() {
    let pool = ThreadPoolBuilder::new()
        .thread_name(|size| format!("rayon-worker-{}", size))
        .num_threads(2)
        .busy_handler(|thread| {
            println!("线程繁忙通知: {}", thread);
            ACTIVE_TASKS.fetch_add(1, Ordering::Release);
        })
        .idle_handler(|thread| {
            println!("线程空闲通知: {}", thread);
            let _ = ACTIVE_TASKS.fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                if x <= 0 {
                    None
                }else {
                    Some(x - 1)
                }
            });
        })
        .build()
        .unwrap();
    for i in 0..3 {
        pool.spawn(|| {
            thread::sleep(Duration::from_secs(1));
            println!("{}", ACTIVE_TASKS.load(Ordering::Acquire));
        });
    }
    thread::sleep(Duration::from_secs(5));
    println!("{}", ACTIVE_TASKS.load(Ordering::Acquire));
}