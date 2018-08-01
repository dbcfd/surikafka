use std;

pub struct Stats {
    alert_count: usize,
    alert_length: usize,
    produce_time: std::time::Duration
}

impl Stats {
    pub fn alert_count(&self) -> usize { self.alert_count }
    pub fn alert_length(&self) -> usize { self.alert_length }
    pub fn produce_time(&self) -> std::time::Duration { self.produce_time }

    pub fn mark(
        &mut self,
        alert_length: usize,
        produce_time: std::time::Duration
    ) -> &mut Self {
        self.alert_count += 1;
        self.alert_length += alert_length;
        self.produce_time += produce_time;
        self
    }
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            alert_count: 0,
            alert_length: 0,
            produce_time: std::time::Duration::from_secs(0)
        }
    }
}