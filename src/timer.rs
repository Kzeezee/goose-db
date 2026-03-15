use std::time::{Duration, Instant};

pub struct Timer {
    laps: Vec<(&'static str, Duration)>,
    last: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            laps: Vec::new(),
            last: Instant::now(),
        }
    }

    pub fn lap(&mut self, label: &'static str) {
        let now = Instant::now();
        self.laps.push((label, now - self.last));
        self.last = now;
    }

    pub fn report(&self) {
        for (label, dur) in &self.laps {
            eprintln!("  {}: {:.2} ms", label, dur.as_secs_f64() * 1000.0);
        }
    }
}
