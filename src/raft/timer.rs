use tracing::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

#[derive(Debug)]
pub struct Timer {
    name: String,
    alive: Arc<AtomicBool>,                         // 控制计时器是否在执行
    interval: Arc<Mutex<Duration>>,                 // 计时器触发间隔 (std::sync::Mutex is okay here)
    next_trigger: Arc<Mutex<TokioInstant>>,         // 计时器下次触发时间 (use TokioInstant)
    pub last_reset: Option<std::time::Instant>,     // 上次重置计时器的时间 (std::time::Instant is fine for this field)
    handle: Option<tokio::task::JoinHandle<()>>,    // 计时器内部任务句柄
    stop_tx: Option<tokio::sync::watch::Sender<()>>, // 用于通知任务停止
}

impl Timer {
    pub fn new(name: &str) -> Self {
         Timer {
            name: name.to_string(),
            alive: Arc::new(AtomicBool::new(false)),
            interval: Arc::new(Mutex::new(Duration::from_secs(std::u64::MAX))),
            next_trigger: Arc::new(Mutex::new(TokioInstant::now())),
            last_reset: None,
            handle: None,
            stop_tx: None,
         }
    }
    pub fn schedule<F>(&mut self, trigger_interval: Duration, callback: F)
    where 
        F: 'static + Send + Clone + FnMut() -> () + Sync 
    {
        info!(
            "{} start schedule with trigger interval: {}ms",
            self.name,
            trigger_interval.as_millis()
        );

        // 如果已经有一个任务在运行，先停止它
        if self.handle.is_some() {
            self.stop_internal(false);  // 不需要阻塞，因为新的任务直接替代了
        }

        *self.interval.lock().unwrap() = trigger_interval;
        *self.next_trigger.lock().unwrap() = TokioInstant::now() + trigger_interval;
        self.alive.store(true, Ordering::SeqCst);

        let name_clone = self.name.clone();
        let interval_arc = self.interval.clone();
        let next_trigger_arc = self.next_trigger.clone();
        let alive_arc = self.alive.clone();

        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(());
        self.stop_tx = Some(stop_tx);

        self.handle = Some(tokio::spawn(async move {
            loop {
                let current_next_trigger_time;
                { // Scoped lock
                    current_next_trigger_time = *next_trigger_arc.lock().unwrap();
                }

                tokio::select! {
                    _ = tokio::time::sleep_until(current_next_trigger_time) => {
                        // Check alive status first, in case stop was called during sleep
                        if !alive_arc.load(Ordering::SeqCst) {
                            info!("{} task: alive is false after sleep, exiting.", name_clone);
                            break;
                        }

                        // 异步执行回调函数，不阻塞计时器任务
                        // 如果回调是CPU密集型或阻塞IO，使用 spawn_blocking
                        let mut cb_clone = callback.clone();
                        tokio::task::spawn_blocking(move || {
                            cb_clone();
                        });
                        // 如果回调是 async fn，则：
                        // tokio::spawn(async move { cb_clone().await; });

                        // 重新计算下一次的触发时间
                        let current_interval;
                        { // Scoped lock
                            current_interval = *interval_arc.lock().unwrap();
                        }
                        let new_next_trigger = TokioInstant::now() + current_interval;
                        *next_trigger_arc.lock().unwrap() = new_next_trigger;
                        // info!("{} task: triggered, next at {:?}", name_clone, new_next_trigger);
                    }
                    _ = stop_rx.changed() => {
                        info!("{} task: stop signal received, exiting.", name_clone);
                        alive_arc.store(false, Ordering::SeqCst); // Ensure alive is also false
                        break;
                    }
                }
                // Final check after select, in case alive was set to false by external stop()
                // and stop_rx hasn't been processed or sleep_until returned.
                if !alive_arc.load(Ordering::SeqCst) {
                    info!("{} task: alive is false after select, exiting.", name_clone);
                    break;
                }
            }
            info!("{} task: loop finished.", name_clone);
        }));

    }
    fn stop_internal(&mut self, wait_for_join: bool) {
        info!("{} stopping (internal, wait: {})", self.name, wait_for_join);
        self.alive.store(false, Ordering::SeqCst);

        if let Some(tx) = self.stop_tx.take() {
            info!("{} stop signal sender dropped.", self.name);
        }

        if wait_for_join {
            if let Some(handle) = self.handle.take() {
                info!("{} waiting for task to join...", self.name);
            }
        } else {
            self.handle.take();
        }
    }

     pub fn reset(&mut self, trigger_interval: Duration) {
        info!(
            "{} reset with trigger interval: {}ms",
            self.name,
            trigger_interval.as_millis(),
        );

        self.last_reset = Some(std::time::Instant::now());
        *self.interval.lock().unwrap() = trigger_interval;
        *self.next_trigger.lock().unwrap() = TokioInstant::now() + trigger_interval;

        if self.alive.load(Ordering::SeqCst) {
        } else {
            info!("{} reset called on a stopped or not-yet-scheduled timer. Values set for next schedule.", self.name);
        }
    }

    pub async fn stop(&mut self) { // Made async to allow .await on JoinHandle
        info!("{} stopping", self.name);
        self.alive.store(false, Ordering::SeqCst);

        if let Some(tx) = self.stop_tx.take() {
            // Dropping tx is enough to signal the watch channel.
            let _ = tx.send(()); // Or explicitly send, then drop.
            info!("{} sent stop signal.", self.name);
        }

        if let Some(handle) = self.handle.take() {
            info!("{} awaiting task completion...", self.name);
            // 等待任务自己结束，而不是 abort 它
            if let Err(e) = handle.await {
                // 如果任务 panic 了，这里会捕获到
                super::logging::error!("{} task panicked during join: {:?}", self.name, e);
            } else {
                info!("{} task joined successfully.", self.name);
            }
        }
        info!("{} stopped.", self.name);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        info!("{} dropping.", self.name);
        // 尝试发送停止信号，但不阻塞或 abort
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        // 不要在这里 abort handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering}; // Renamed to avoid conflict
    use tracing_subscriber; // For test logging

    fn setup_tracing() {
        // Ensures tracing is initialized only once.
        static TRACING_SETUP: AtomicBool = AtomicBool::new(false);
        if !TRACING_SETUP.swap(true, AtomicOrdering::SeqCst) {
            tracing_subscriber::fmt::init();
        }
    }

    #[tokio::test]
    async fn test_timer_async() {
        setup_tracing();
        let mut timer = Timer::new("test_timer_async");
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = counter.clone();
        timer.schedule(Duration::from_millis(100), move || {
            let val = counter_clone.fetch_add(1, AtomicOrdering::SeqCst);
            println!("Callback! Count: {}, Time: {:?}", val + 1, std::time::Instant::now());
        });

        tokio::time::sleep(Duration::from_millis(550)).await; // Sleep for 5.5 intervals

        let count_after_schedule = counter.load(AtomicOrdering::SeqCst);
        info!("Count after schedule: {}", count_after_schedule);
        assert!(count_after_schedule >= 4 && count_after_schedule <= 6, "Expected 5 callbacks, got {}", count_after_schedule); // +/- 1 for timing

        println!("\nResetting timer\n");
        timer.reset(Duration::from_millis(200));
        let reset_time_counter_val = counter.load(AtomicOrdering::SeqCst);

        tokio::time::sleep(Duration::from_millis(950)).await; // Sleep for ~4.5 new intervals

        let count_after_reset = counter.load(AtomicOrdering::SeqCst);
        info!("Count after reset: {}", count_after_reset);
        // Expected ~4 more callbacks (total ~9)
        let expected_after_reset = reset_time_counter_val + 4; // or 5
        assert!(
            count_after_reset >= expected_after_reset -1 && count_after_reset <= expected_after_reset + 1 ,
            "Expected around {} callbacks after reset, got {}", expected_after_reset, count_after_reset
        );


        println!("\nStopping timer\n");
        timer.stop().await;
        let count_after_stop = counter.load(AtomicOrdering::SeqCst);
        info!("Count after stop: {}", count_after_stop);

        tokio::time::sleep(Duration::from_millis(500)).await; // Sleep more
        let count_final = counter.load(AtomicOrdering::SeqCst);
        assert_eq!(count_after_stop, count_final, "Callback should not run after stop");
        info!("Final count: {}", count_final);

        // Test scheduling again after stop
        println!("\nScheduling timer again\n");
        let counter_clone2 = counter.clone();
        timer.schedule(Duration::from_millis(50), move || {
            let val = counter_clone2.fetch_add(1, AtomicOrdering::SeqCst);
            println!("Callback again! Count: {}, Time: {:?}", val + 1, std::time::Instant::now());
        });
        tokio::time::sleep(Duration::from_millis(220)).await; // ~4 callbacks
        timer.stop().await;
        let count_after_reschedule = counter.load(AtomicOrdering::SeqCst);
        info!("Count after reschedule: {}", count_after_reschedule);
        assert!(count_after_reschedule >= count_final + 3 && count_after_reschedule <= count_final + 5);


        println!("Test finished");
    }
}