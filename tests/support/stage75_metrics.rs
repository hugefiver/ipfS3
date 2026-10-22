//! Operation-local measurements for isolated scale fixtures.
use sea_orm::{DatabaseConnection, Statement};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

pub struct CountingAllocator;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    ALLOCATED.fetch_add(bytes, Ordering::Relaxed);
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: allocation and deallocation are forwarded unchanged to System;
// atomics only observe the sizes and never access the allocated memory.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(size);
        }
        resized
    }
}

pub struct MemorySample {
    initial_live: usize,
    initial_allocated: usize,
    rss: RssSample,
}

pub struct MemoryResult {
    pub rust_peak_delta: usize,
    pub rust_allocated: usize,
    pub rss_initial: Option<usize>,
    pub rss_sampled_peak: Option<usize>,
}

impl MemorySample {
    pub fn begin() -> Self {
        let initial_live = LIVE.load(Ordering::Relaxed);
        PEAK.store(initial_live, Ordering::Relaxed);
        Self {
            initial_live,
            initial_allocated: ALLOCATED.load(Ordering::Relaxed),
            rss: RssSample::begin(),
        }
    }

    /// Rust counters include runtime/instrumentation allocations. Process RSS
    /// additionally includes native drivers and caches; its peak is sampled.
    pub fn finish(self) -> MemoryResult {
        let rust_peak_delta = PEAK
            .load(Ordering::Relaxed)
            .saturating_sub(self.initial_live);
        let rust_allocated = ALLOCATED.load(Ordering::Relaxed) - self.initial_allocated;
        let (rss_initial, rss_sampled_peak) = self.rss.finish();
        MemoryResult {
            rust_peak_delta,
            rust_allocated,
            rss_initial,
            rss_sampled_peak,
        }
    }
}

struct RssSample {
    initial: Option<usize>,
    peak: Arc<AtomicUsize>,
    stopped: Arc<AtomicBool>,
    sampler: Option<std::thread::JoinHandle<()>>,
}

impl RssSample {
    fn begin() -> Self {
        let initial = process_rss();
        let peak = Arc::new(AtomicUsize::new(initial.unwrap_or_default()));
        let stopped = Arc::new(AtomicBool::new(false));
        let sampler = initial.map(|_| {
            let peak = peak.clone();
            let stopped = stopped.clone();
            std::thread::spawn(move || {
                while !stopped.load(Ordering::Relaxed) {
                    if let Some(bytes) = process_rss() {
                        peak.fetch_max(bytes, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        });
        Self {
            initial,
            peak,
            stopped,
            sampler,
        }
    }

    fn finish(mut self) -> (Option<usize>, Option<usize>) {
        if let Some(bytes) = process_rss() {
            self.peak.fetch_max(bytes, Ordering::Relaxed);
        }
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(sampler) = self.sampler.take() {
            sampler.join().unwrap();
        }
        (
            self.initial,
            self.initial.map(|_| self.peak.load(Ordering::Relaxed)),
        )
    }
}

impl Drop for RssSample {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
    }
}

#[cfg(windows)]
fn process_rss() -> Option<usize> {
    #[repr(C)]
    struct Counters {
        size: u32,
        page_faults: u32,
        peak_working_set: usize,
        working_set: usize,
        peak_paged_pool: usize,
        paged_pool: usize,
        peak_nonpaged_pool: usize,
        nonpaged_pool: usize,
        pagefile: usize,
        peak_pagefile: usize,
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut std::ffi::c_void,
            counters: *mut Counters,
            size: u32,
        ) -> i32;
    }
    let size = std::mem::size_of::<Counters>() as u32;
    let mut counters = Counters {
        size,
        page_faults: 0,
        peak_working_set: 0,
        working_set: 0,
        peak_paged_pool: 0,
        paged_pool: 0,
        peak_nonpaged_pool: 0,
        nonpaged_pool: 0,
        pagefile: 0,
        peak_pagefile: 0,
    };
    // SAFETY: -1 is the Windows current-process pseudo handle. The initialized
    // repr(C) buffer and its size match PROCESS_MEMORY_COUNTERS.
    let success = unsafe { GetProcessMemoryInfo(-1isize as *mut _, &mut counters, size) };
    (success != 0).then_some(counters.working_set)
}

#[cfg(not(windows))]
fn process_rss() -> Option<usize> {
    None
}

#[derive(Default)]
pub struct QuerySample {
    pub count: usize,
    pub elapsed: Duration,
    pub statements: BTreeMap<String, (usize, Duration, Statement)>,
}

#[derive(Clone, Default)]
pub struct QueryMeter(Arc<Mutex<Option<QuerySample>>>);

impl QueryMeter {
    pub fn install(db: &mut DatabaseConnection) -> Self {
        let meter = Self::default();
        let state = meter.0.clone();
        db.set_metric_callback(move |info| {
            if let Some(sample) = state.lock().unwrap().as_mut() {
                sample.count += 1;
                sample.elapsed += info.elapsed;
                assert!(sample.statements.len() < 1024, "unbounded query shapes");
                let entry = sample
                    .statements
                    .entry(info.statement.sql.clone())
                    .or_insert_with(|| (0, Duration::ZERO, info.statement.clone()));
                entry.0 += 1;
                entry.1 += info.elapsed;
            }
        });
        meter
    }

    pub fn begin(&self) {
        *self.0.lock().unwrap() = Some(QuerySample::default());
    }

    pub fn finish(&self) -> QuerySample {
        self.0.lock().unwrap().take().unwrap()
    }
}
