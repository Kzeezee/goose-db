use std::time::{Duration, Instant};

pub struct Timer {
    laps:     Vec<(&'static str, Duration)>,
    counters: Vec<(&'static str, u64)>,
    last:     Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            laps:     Vec::new(),
            counters: Vec::new(),
            last:     Instant::now(),
        }
    }

    pub fn lap(&mut self, label: &'static str) {
        let now = Instant::now();
        self.laps.push((label, now - self.last));
        self.last = now;
    }

    /// Record a pre-measured duration as a lap (e.g. accumulated across a loop).
    /// Resets the internal clock so subsequent `lap()` calls are not double-counted.
    pub fn lap_duration(&mut self, label: &'static str, dur: Duration) {
        self.laps.push((label, dur));
        self.last = Instant::now();
    }

    /// Record a row counter (printed after lap timings in report).
    pub fn count(&mut self, label: &'static str, value: u64) {
        self.counters.push((label, value));
    }

    pub fn report(&self) {
        for (label, dur) in &self.laps {
            eprintln!("  {:<40}: {:>8.2} ms", label, dur.as_secs_f64() * 1000.0);
        }
        if !self.counters.is_empty() {
            eprintln!("  -- row counts --");
            for (label, n) in &self.counters {
                eprintln!("  {:<40}: {:>8}", label, n);
            }
        }
    }
}
