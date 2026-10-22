//! Parent-selected native PG scope only. Schema disposal belongs to the parent.
#[path = "stage7_management.rs"]
#[allow(dead_code)]
mod foundation;

include!("stage7_management_apply_postgres_body.rs");
