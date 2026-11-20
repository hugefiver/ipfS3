use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use http::{Method, StatusCode};

use crate::store::pinning::resources::{Action, Outcome};

const METHODS: [&str; 8] = [
    "GET", "HEAD", "PUT", "POST", "DELETE", "PATCH", "OPTIONS", "other",
];
const CLASSES: [&str; 6] = ["other", "1xx", "2xx", "3xx", "4xx", "5xx"];
const ACTIONS: [&str; 2] = ["submit", "delete"];
const OUTCOMES: [&str; 6] = [
    "submitted",
    "rejected",
    "deleted",
    "retry",
    "failed",
    "unknown",
];

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    micros: AtomicU64,
}

impl Timing {
    fn observe(&self, elapsed: Duration) {
        self.micros.fetch_add(elapsed.as_micros() as u64, Relaxed);
        self.count.fetch_add(1, Relaxed);
    }

    fn render(&self, text: &mut String, name: &str, labels: &str) {
        let count = self.count.load(Relaxed);
        if count != 0 {
            let seconds = self.micros.load(Relaxed) as f64 / 1_000_000.0;
            writeln!(
                text,
                "{name}_count{{{labels}}} {count}\n{name}_sum{{{labels}}} {seconds}"
            )
            .unwrap();
        }
    }
}

pub struct Metrics {
    requests: [[Timing; CLASSES.len()]; METHODS.len()],
    pins: BTreeMap<String, Arc<PinMetrics>>,
}

#[derive(Default)]
pub(crate) struct PinMetrics {
    label: String,
    dispatches: [AtomicU64; ACTIONS.len()],
    wait: [Timing; ACTIONS.len()],
    provider: [[Timing; OUTCOMES.len()]; ACTIONS.len()],
    settlement: [[Timing; 3]; ACTIONS.len()],
}

impl Metrics {
    pub fn new(config: &crate::pinning::config::ValidatedPinningConfig) -> Self {
        let mut pins = BTreeMap::new();
        for provider in &config.providers {
            // Aliases share a lane. The coordinator uses the first configured route in this order.
            pins.entry(provider.name.clone()).or_insert_with(|| {
                Arc::new(PinMetrics {
                    label: provider
                        .config_name
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\n', "\\n"),
                    ..Default::default()
                })
            });
        }
        Self {
            requests: Default::default(),
            pins,
        }
    }

    pub(crate) fn pin_for(&self, allocation: &str) -> Arc<PinMetrics> {
        Arc::clone(&self.pins[allocation])
    }

    pub fn request(&self, method: &Method, status: StatusCode, elapsed: Duration) {
        let method = METHODS
            .iter()
            .position(|&m| m == method.as_str())
            .unwrap_or(METHODS.len() - 1);
        let class = usize::from(status.as_u16() / 100);
        self.requests[method][if class < CLASSES.len() { class } else { 0 }].observe(elapsed);
    }
}

impl PinMetrics {
    pub(crate) fn dispatch(&self, action: Action, wait: Option<Duration>) {
        self.dispatches[action as usize].fetch_add(1, Relaxed);
        if let Some(wait) = wait {
            self.wait[action as usize].observe(wait);
        }
    }

    pub(crate) fn provider(&self, action: Action, outcome: &Outcome, elapsed: Duration) {
        let outcome = match outcome {
            Outcome::Submitted(_) => 0,
            Outcome::Rejected(_) => 1,
            Outcome::Deleted => 2,
            Outcome::RetryAt(_) => 3,
            Outcome::Failed => 4,
            Outcome::Unknown => 5,
        };
        self.provider[action as usize][outcome].observe(elapsed);
    }

    pub(crate) fn settlement(&self, action: Action, applied: Option<bool>, elapsed: Duration) {
        let outcome = match applied {
            Some(true) => 0,
            Some(false) => 1,
            None => 2,
        };
        self.settlement[action as usize][outcome].observe(elapsed);
    }
}

impl Metrics {
    pub fn render(&self) -> String {
        const REQUEST: &str = "ipfs_s3_request_duration_seconds";
        const WAIT: &str = "ipfs_s3_pin_queue_wait_seconds";
        const PROVIDER: &str = "ipfs_s3_pin_provider_duration_seconds";
        const SETTLE: &str = "ipfs_s3_pin_settlement_duration_seconds";
        const DISPATCH: &str = "ipfs_s3_pin_dispatches_total";
        let mut text = format!("# TYPE {REQUEST} summary\n");
        for (m, method) in METHODS.iter().enumerate() {
            for (s, class) in CLASSES.iter().enumerate() {
                self.requests[m][s].render(
                    &mut text,
                    REQUEST,
                    &format!(r#"method="{method}",status_class="{class}""#),
                );
            }
        }
        writeln!(text, "# TYPE {DISPATCH} counter").unwrap();
        let series = || {
            self.pins.values().flat_map(|pin| {
                ACTIONS.iter().enumerate().map(move |(a, action)| {
                    (
                        pin,
                        a,
                        format!(r#"provider="{}",action="{action}""#, pin.label),
                    )
                })
            })
        };
        for (pin, a, labels) in series() {
            writeln!(
                text,
                "{DISPATCH}{{{labels}}} {}",
                pin.dispatches[a].load(Relaxed)
            )
            .unwrap();
        }
        writeln!(text, "# TYPE {WAIT} summary").unwrap();
        for (pin, a, labels) in series() {
            pin.wait[a].render(&mut text, WAIT, &labels);
        }
        writeln!(text, "# TYPE {PROVIDER} summary").unwrap();
        for (pin, a, labels) in series() {
            for (o, outcome) in OUTCOMES.iter().enumerate() {
                pin.provider[a][o].render(
                    &mut text,
                    PROVIDER,
                    &format!(r#"{labels},outcome="{outcome}""#),
                );
            }
        }
        writeln!(text, "# TYPE {SETTLE} summary").unwrap();
        for (pin, a, labels) in series() {
            for (o, outcome) in ["applied", "stale", "error"].iter().enumerate() {
                pin.settlement[a][o].render(
                    &mut text,
                    SETTLE,
                    &format!(r#"{labels},outcome="{outcome}""#),
                );
            }
        }
        text
    }
}
