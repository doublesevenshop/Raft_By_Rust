use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn create_noop_callback(counter: Arc<AtomicUsize>) -> impl FnMut() -> () + Send + Clone + 'static {
    move || {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}


fn create_jitter_callback(
    counter: Arc<AtomicUsize>,
    interval_micros: u128,
    jitters_ns: Arc<Mutex<Vec<i64>>>, // Store jitter in nanoseconds
    timer_name: String, // For logging
) -> impl FnMut() -> () + Send + Clone + 'static {
        move || {
        let _now = Instant::now();
        counter.fetch_add(1, Ordering::Relaxed);
        if counter.load(Ordering::Relaxed) % 1000 == 0 {
            println!("[{}] Callback count: {}", timer_name, counter.load(Ordering::Relaxed));
        }
    }
}

mod std_test_runner {
    use super::*;
    use crate::raft::timer_old::Timer;

    pub fn run_std_timers(
        num_timers: usize, 
        interval: Duration, 
        run_duration: Duration) -> Arc<AtomicUsize> {
        println!("\n--- Testing std::thread Timer ---");
        println!("Number of timers: {}", num_timers);
        println!("Interval: {:?}", interval);
        println!("Run duration: {:?}", run_duration);

        let total_callbacks = Arc::new(AtomicUsize::new(0));
        let mut timers = Vec::new();
        

       for i in 0..num_timers {
            let mut timer = Timer::new(&format!("std_timer_{}", i));
            let cb_counter = total_callbacks.clone();
            let callback = create_noop_callback(cb_counter); // Use the simple no-op
            timer.schedule(interval, callback);
            timers.push(timer);
        }
        println!("All std::thread timers scheduled. Running for {:?}...", run_duration);
        println!("MONITOR CPU, MEMORY, AND THREAD COUNT NOW for the next {:?}.", run_duration);
        std::thread::sleep(run_duration);

        println!("Stopping std::thread timers...");
        for mut timer in timers {
            timer.stop();
        }
        println!("All std::thread timers stopped.");
        total_callbacks
    }
}


mod async_test_runner {
    use super::*;
    use crate::raft::timer::Timer;

    pub async fn run_async_timers(
        num_timers: usize, 
        interval: Duration, 
        run_duration: Duration) -> Arc<AtomicUsize> {
        println!("\n--- Testing tokio Timer ---");
        println!("Number of timers: {}", num_timers);
        println!("Interval: {:?}", interval);
        println!("Run duration: {:?}", run_duration);

        let total_callbacks = Arc::new(AtomicUsize::new(0));
        let mut timers = Vec::new();
        for i in 0..num_timers {
            let mut timer = Timer::new(&format!("async_timer_{}", i));
            let cb_counter = total_callbacks.clone();
            let callback = create_noop_callback(cb_counter);
            timer.schedule(interval, callback); // schedule itself is sync
            timers.push(timer);
        }

        println!("All tokio timers scheduled. Running for {:?}...", run_duration);
        println!("MONITOR CPU, MEMORY, AND THREAD COUNT NOW for the next {:?}.", run_duration);
        tokio::time::sleep(run_duration).await;

        println!("Stopping tokio timers...");
        for mut timer in timers {
            timer.stop().await; // stop is async
        }
        println!("All tokio timers stopped.");
        total_callbacks
    }
}

pub async fn run_benchmarks() {
    // Test std::thread version
    // Ensure your std_timer module and Timer struct are correctly pathed
    // let std_callbacks = std_test_runner::run_std_timers(num_timers, interval, run_duration);
    // println!("[Std] Total callbacks for {} timers: {}", num_timers, std_callbacks.load(Ordering::Relaxed));
    // println!("Pausing before next test run...");
    // std::thread::sleep(Duration::from_secs(5)); // Give system time to settle
    let num_timers_to_test = [10, 100, 500, 1000, 5000]; //, 1000, 5000]; // Add more for scalability
    let interval = Duration::from_millis(500);
    let run_duration = Duration::from_secs(20); // Run long enough to observe with system tools

    for &num_timers in &num_timers_to_test {
        // let async_callbacks = async_test_runner::run_async_timers(num_timers, interval, run_duration).await;
        // println!("[Async] Total callbacks for {} timers: {}", num_timers, async_callbacks.load(Ordering::Relaxed));
        // println!("Pausing before next test run...");
        // tokio::time::sleep(Duration::from_secs(5)).await; // Give system time to settle
        
        let std_callbacks = std_test_runner::run_std_timers(num_timers, interval, run_duration);
        println!("[Std] Total callbacks for {} timers: {}", num_timers, std_callbacks.load(Ordering::Relaxed));
        println!("Pausing before next test run...");
        std::thread::sleep(Duration::from_secs(5)); // Give system time to settle
    }
}
