//! Exact local test faults at the commit-response and receipt-reread boundary.
use super::*;
use std::cell::RefCell;
thread_local! {static NEXT:RefCell<Option<(String,String)>>=const {RefCell::new(None)};}
pub(super) fn install(auth: &records::LocalAuthorization, binding: &records::ConfirmedBinding) {
    NEXT.with(|next| {
        *next.borrow_mut() = Some((
            auth.database_scope().to_owned(),
            binding.request.plan_id.clone(),
        ))
    });
}
pub(super) fn take(
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
) -> bool {
    NEXT.with(|next| {
        let mut next = next.borrow_mut();
        if next.as_ref().is_some_and(|(scope, plan)| {
            scope == auth.database_scope() && plan == &binding.request.plan_id
        }) {
            next.take();
            true
        } else {
            false
        }
    })
}
